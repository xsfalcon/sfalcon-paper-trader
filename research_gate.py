import json
from datetime import datetime, timezone

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS research_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mint TEXT NOT NULL,
  symbol TEXT,
  verdict TEXT NOT NULL,
  final_score REAL NOT NULL,
  confidence REAL NOT NULL,
  hard_blocks TEXT NOT NULL,
  reasons TEXT NOT NULL,
  snapshot TEXT NOT NULL,
  researched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_runs_mint ON research_runs(mint,researched_at);
CREATE TABLE IF NOT EXISTS radar_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mint TEXT NOT NULL,
  symbol TEXT,
  radar_type TEXT NOT NULL,
  strength REAL NOT NULL,
  details TEXT NOT NULL,
  observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_radar_mint_time ON radar_events(mint,observed_at);
"""


def _now(): return datetime.now(timezone.utc).isoformat()

def volatility_signal(c):
    liq=max(float(c.liquidity_usd or 0),1.0)
    vol_ratio=float(c.volume_m5 or 0)/liq
    triggered=abs(float(c.price_change_m5 or 0))>=8 or abs(float(c.price_change_h1 or 0))>=20 or vol_ratio>=0.15
    total=c.buys_m5+c.sells_m5
    br=c.buys_m5/total if total else 0
    delta=0.0
    if triggered:
        if 3<=c.price_change_m5<=35 and br>=0.58 and vol_ratio>=0.15: delta+=3.0
        if vol_ratio>=0.35 and br>=0.60: delta+=1.0
        if c.price_change_m5<-15: delta-=3.0
        if c.price_change_m5>80: delta-=3.0
        if c.price_change_m5>150: delta-=2.0
    return {"triggered":triggered,"score_delta":max(-4.0,min(4.0,round(delta,2))),"price_change_m5":c.price_change_m5,"price_change_h1":c.price_change_h1,"volume_to_liquidity_5m":round(vol_ratio,4),"buy_ratio":round(br,4)}

def postbond_signal(c,lore_signal):
    bonded=(lore_signal or {}).get("bonded_at")
    if not bonded: return {"available":False,"score_delta":0}
    try:
        dt=datetime.fromisoformat(str(bonded).replace("Z","+00:00"))
        age_h=(datetime.now(timezone.utc)-dt.astimezone(timezone.utc)).total_seconds()/3600
    except Exception:
        age_h=999
    if age_h>24: return {"available":False,"score_delta":0,"age_hours":round(age_h,2)}
    total=c.buys_m5+c.sells_m5; br=c.buys_m5/total if total else 0
    d=0.0
    if c.liquidity_usd>=50000:d+=2
    if c.volume_m5>=10000:d+=2
    if br>=0.58:d+=1
    if 0<=c.price_change_m5<=40:d+=1
    if c.price_change_m5<-15:d-=4
    if c.liquidity_usd<25000:d-=4
    return {"available":True,"score_delta":max(-6.0,min(6.0,d)),"age_hours":round(age_h,2),"buy_ratio":round(br,4),"bonded_at":bonded}

class ResearchGate:
    def __init__(self,db_path): self.db_path=db_path
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA); await db.commit()

    def evaluate(self,c,signals,entry_score_min=85,min_liq=25000,min_vol5=5000):
        blocks=[]; reasons=[]
        if c.suspicious is True: blocks.append("Jupiter audit marks token suspicious")
        if c.mint_authority_disabled is False: blocks.append("mint authority still active")
        if c.freeze_authority_disabled is False: blocks.append("freeze authority still active")
        if c.top_holders_pct is not None and float(c.top_holders_pct)>60: blocks.append("top-holder concentration above 60%")
        if c.dev_balance_pct is not None and float(c.dev_balance_pct)>15: blocks.append("developer balance above 15%")
        if c.liquidity_usd<min_liq: blocks.append("liquidity below minimum")
        if c.volume_m5<min_vol5: blocks.append("5-minute volume below minimum")
        if c.price_change_m5>150: blocks.append("extreme 5-minute chase risk")
        for k,v in signals.items():
            if isinstance(v,dict):
                d=float(v.get("score_delta") or 0)
                if d>=3: reasons.append(f"{k} strongly positive ({d:+.1f})")
                elif d<=-3: reasons.append(f"{k} strongly negative ({d:+.1f})")
        score=float(c.score)
        if blocks: verdict="REJECT"
        elif score>=entry_score_min: verdict="QUALIFIED"
        elif score>=65 or any((isinstance(v,dict) and v.get("triggered")) for v in signals.values()): verdict="WATCH"
        else: verdict="IGNORE"
        known=sum(x is not None for x in [c.suspicious,c.top_holders_pct,c.dev_balance_pct,c.mint_authority_disabled,c.freeze_authority_disabled,c.organic_score,c.holder_count])
        confidence=min(1.0,0.45+0.06*known+0.04*sum(1 for v in signals.values() if isinstance(v,dict) and (v.get("available") or v.get("mentions") or v.get("triggered") or v.get("events"))))
        return {"verdict":verdict,"final_score":round(score,2),"confidence":round(confidence,2),"hard_blocks":blocks,"reasons":reasons}

    async def record(self,c,result,signals):
        snap={"market":{"price":c.price_usd,"liquidity":c.liquidity_usd,"market_cap":c.market_cap,"volume_m5":c.volume_m5,"volume_h1":c.volume_h1,"buys_m5":c.buys_m5,"sells_m5":c.sells_m5,"change_m5":c.price_change_m5,"change_h1":c.price_change_h1},"audit":{"top_holders_pct":c.top_holders_pct,"dev_balance_pct":c.dev_balance_pct,"mint_authority_disabled":c.mint_authority_disabled,"freeze_authority_disabled":c.freeze_authority_disabled,"organic_score":c.organic_score,"holder_count":c.holder_count,"suspicious":c.suspicious},"signals":signals,"score_breakdown":c.score_breakdown}
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO research_runs(mint,symbol,verdict,final_score,confidence,hard_blocks,reasons,snapshot,researched_at) VALUES(?,?,?,?,?,?,?,?,?)",(c.address,c.symbol,result["verdict"],result["final_score"],result["confidence"],json.dumps(result["hard_blocks"]),json.dumps(result["reasons"]),json.dumps(snap),_now()))
            await db.commit()

    async def record_radar(self,c,radar_type,strength,details):
        async with aiosqlite.connect(self.db_path) as db:
            # avoid duplicate spam within same minute by just accepting sparse discovery-cycle inserts
            await db.execute("INSERT INTO radar_events(mint,symbol,radar_type,strength,details,observed_at) VALUES(?,?,?,?,?,?)",(c.address,c.symbol,radar_type,float(strength),json.dumps(details),_now())); await db.commit()

    async def recent(self,limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory=aiosqlite.Row; cur=await db.execute("SELECT * FROM research_runs ORDER BY id DESC LIMIT ?",(limit,)); out=[]
            for r in await cur.fetchall():
                d=dict(r)
                for k in ("hard_blocks","reasons","snapshot"):
                    try:d[k]=json.loads(d[k])
                    except Exception:pass
                out.append(d)
            return out

    async def volatile(self,limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory=aiosqlite.Row; cur=await db.execute("SELECT * FROM radar_events WHERE radar_type='volatility' ORDER BY id DESC LIMIT ?",(limit,)); out=[]
            for r in await cur.fetchall():
                d=dict(r)
                try:d["details"]=json.loads(d["details"])
                except Exception:pass
                out.append(d)
            return out
