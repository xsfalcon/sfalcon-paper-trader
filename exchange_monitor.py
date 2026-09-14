import json
import os
import re
from datetime import datetime, timedelta, timezone

import aiosqlite

BINANCE_NEW_LISTING_CATALOG = int(os.getenv("BINANCE_NEW_LISTING_CATALOG", "48"))
EXCHANGE_SIGNAL_WINDOW_HOURS = int(os.getenv("EXCHANGE_SIGNAL_WINDOW_HOURS", "24"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS exchange_events (
  event_id TEXT PRIMARY KEY,
  exchange_name TEXT NOT NULL,
  event_type TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT,
  published_at TEXT,
  symbols TEXT NOT NULL DEFAULT '[]',
  source TEXT NOT NULL,
  observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_exchange_events_time ON exchange_events(published_at);
"""

SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,12})\b")
STOP_SYMBOLS = {"BINANCE","USD","USDT","USDC","TRY","EUR","BTC","ETH","SOL","NEW","LIST","LISTING","WILL","SPOT","FUTURES"}


def _now(): return datetime.now(timezone.utc)

def _to_iso(x):
    if x is None: return None
    try:
        if isinstance(x, (int,float)) or str(x).isdigit():
            n=float(x)
            if n>1e12: n/=1000
            return datetime.fromtimestamp(n,tz=timezone.utc).isoformat()
        return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def _extract_symbols(title):
    return sorted({x for x in SYMBOL_RE.findall((title or "").upper()) if x not in STOP_SYMBOLS})

class ExchangeMonitor:
    def __init__(self, client, db_path):
        self.client=client; self.db_path=db_path
        self.last_poll_at=None; self.last_success_at=None; self.last_error=None

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA); await db.commit()

    async def _binance(self):
        urls=[
          "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query",
          "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
        ]
        params={"type":1,"catalogId":BINANCE_NEW_LISTING_CATALOG,"pageNo":1,"pageSize":30}
        last=None
        for url in urls:
            try:
                r=await self.client.get(url,params=params,headers={"Accept":"application/json"})
                r.raise_for_status(); data=r.json()
                node=data.get("data") if isinstance(data,dict) else None
                # Binance has changed nesting multiple times; recursively locate article arrays.
                arrays=[]
                def walk(x):
                    if isinstance(x,list):
                        if x and all(isinstance(i,dict) for i in x): arrays.append(x)
                        for i in x: walk(i)
                    elif isinstance(x,dict):
                        for v in x.values(): walk(v)
                walk(node or data)
                rows=max(arrays,key=len) if arrays else []
                out=[]
                for a in rows:
                    title=a.get("title") or a.get("name")
                    if not title: continue
                    code=str(a.get("code") or a.get("id") or a.get("articleId") or title)
                    pub=_to_iso(a.get("releaseDate") or a.get("publishDate") or a.get("createdAt"))
                    link=a.get("url") or (f"https://www.binance.com/en/support/announcement/{code}" if code else None)
                    out.append({"event_id":"binance:"+code,"exchange_name":"Binance","event_type":"new_listing","title":title,"url":link,"published_at":pub,"symbols":_extract_symbols(title),"source":"binance_public_cms"})
                if out: return out
            except Exception as e: last=e
        if last: raise last
        return []

    async def scan_once(self):
        self.last_poll_at=_now().isoformat()
        try:
            rows=await self._binance()
            async with aiosqlite.connect(self.db_path) as db:
                for e in rows:
                    await db.execute("""INSERT OR IGNORE INTO exchange_events(event_id,exchange_name,event_type,title,url,published_at,symbols,source,observed_at)
                                      VALUES(?,?,?,?,?,?,?,?,?)""",(e["event_id"],e["exchange_name"],e["event_type"],e["title"],e["url"],e["published_at"],json.dumps(e["symbols"]),e["source"],_now().isoformat()))
                await db.commit()
            self.last_success_at=_now().isoformat(); self.last_error=None
        except Exception as e:
            self.last_error=f"{type(e).__name__}: {str(e)[:160]}"

    async def signal_for_candidate(self, symbol, name, price_change_m5=0.0):
        sym=(symbol or "").upper().lstrip("$")
        nm=(name or "").lower()
        cutoff=(_now()-timedelta(hours=EXCHANGE_SIGNAL_WINDOW_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory=aiosqlite.Row
            cur=await db.execute("SELECT * FROM exchange_events WHERE COALESCE(published_at,observed_at)>=? ORDER BY COALESCE(published_at,observed_at) DESC",(cutoff,))
            rows=[dict(r) for r in await cur.fetchall()]
        matches=[]
        for r in rows:
            try: syms=set(json.loads(r.get("symbols") or "[]"))
            except Exception: syms=set()
            title=(r.get("title") or "").lower()
            if (sym and sym in syms) or (nm and len(nm)>=4 and nm in title): matches.append(r)
        if not matches: return {"score_delta":0,"events":[],"window_hours":EXCHANGE_SIGNAL_WINDOW_HOURS}
        score=0.0
        for r in matches[:3]:
            t=(r.get("title") or "").lower()
            if "will list" in t or "new cryptocurrency listing" in t or "list " in t: d=8.0
            elif "futures" in t or "perpetual" in t: d=5.0
            else: d=3.0
            if price_change_m5>80: d*=0.1
            elif price_change_m5>60: d*=0.25
            score=max(score,d)
        return {"score_delta":round(min(8.0,score),2),"events":[{"exchange":r["exchange_name"],"title":r["title"],"published_at":r["published_at"]} for r in matches[:5]],"window_hours":EXCHANGE_SIGNAL_WINDOW_HOURS,"chase_adjusted":price_change_m5>60}

    async def status(self):
        async with aiosqlite.connect(self.db_path) as db:
            cur=await db.execute("SELECT COUNT(*) FROM exchange_events"); n=int((await cur.fetchone())[0])
        return {"events_total":n,"last_poll_at":self.last_poll_at,"last_success_at":self.last_success_at,"last_error":self.last_error,"sources":["Binance official public announcement feed (best effort)"]}

    async def recent_events(self, limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory=aiosqlite.Row
            cur=await db.execute("SELECT * FROM exchange_events ORDER BY COALESCE(published_at,observed_at) DESC LIMIT ?",(limit,))
            out=[]
            for r in await cur.fetchall():
                d=dict(r)
                try:d["symbols"]=json.loads(d["symbols"])
                except Exception:pass
                out.append(d)
            return out
