import asyncio
import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiosqlite
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

STARTING_EQUITY = float(os.getenv('STARTING_EQUITY_USD', '1000'))
SCAN_INTERVAL = float(os.getenv('SCAN_INTERVAL_SECONDS', '10'))
POSITION_CHECK = float(os.getenv('POSITION_CHECK_SECONDS', '5'))
MAX_SINGLE_PCT = float(os.getenv('MAX_SINGLE_POSITION_PCT', '0.25'))
MAX_TOTAL_PCT = float(os.getenv('MAX_TOTAL_EXPOSURE_PCT', '0.50'))
MAX_OPEN_POSITIONS = int(os.getenv('MAX_OPEN_POSITIONS', '3'))
ENTRY_SCORE_MIN = float(os.getenv('ENTRY_SCORE_MIN', '85'))
MIN_LIQ = float(os.getenv('MIN_LIQUIDITY_USD', '25000'))
MIN_VOL5 = float(os.getenv('MIN_5M_VOLUME_USD', '5000'))
MAX_SLIP = float(os.getenv('MAX_TOP_LEVEL_SLIPPAGE_PCT', '0.05'))
JUPITER_API_KEY = os.getenv('JUPITER_API_KEY') or None
X_BEARER_TOKEN = os.getenv('X_BEARER_TOKEN') or None
DB_PATH = os.getenv('DB_PATH', 'sfalcon.db')
HOST = os.getenv('API_HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', os.getenv('API_PORT', '8787')))
DEX_BASE = 'https://api.dexscreener.com'

SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 address TEXT NOT NULL, symbol TEXT NOT NULL, entry_price REAL NOT NULL,
 size_usd REAL NOT NULL, token_qty REAL NOT NULL, remaining_qty REAL NOT NULL,
 stop_price REAL NOT NULL, high_price REAL NOT NULL, score REAL NOT NULL,
 opened_at TEXT NOT NULL, realized_pnl REAL NOT NULL DEFAULT 0,
 tp1_done INTEGER NOT NULL DEFAULT 0, tp2_done INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE TABLE IF NOT EXISTS fills (
 id INTEGER PRIMARY KEY AUTOINCREMENT, position_id INTEGER NOT NULL,
 side TEXT NOT NULL, price REAL NOT NULL, qty REAL NOT NULL,
 gross_usd REAL NOT NULL, fee_usd REAL NOT NULL, slippage_pct REAL NOT NULL,
 reason TEXT NOT NULL, ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS candidates (
 address TEXT PRIMARY KEY, symbol TEXT, score REAL, price REAL, liquidity REAL,
 market_cap REAL, volume_m5 REAL, buys_m5 INTEGER, sells_m5 INTEGER,
 social_mentions INTEGER, social_engagement INTEGER, breakdown TEXT, observed_at TEXT
);
'''

@dataclass
class Candidate:
    address: str
    symbol: str
    name: str
    price_usd: float
    liquidity_usd: float
    market_cap: float
    volume_m5: float
    volume_h1: float
    buys_m5: int
    sells_m5: int
    buys_h1: int
    sells_h1: int
    price_change_m5: float
    price_change_h1: float
    pair_created_at_ms: int | None
    dex_id: str
    pair_address: str
    top_holders_pct: float | None = None
    dev_balance_pct: float | None = None
    mint_authority_disabled: bool | None = None
    freeze_authority_disabled: bool | None = None
    organic_score: float | None = None
    suspicious: bool | None = None
    social_mentions: int = 0
    social_engagement: int = 0
    score: float = 0.0
    score_breakdown: dict = field(default_factory=dict)
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class Sources:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=12, headers={'User-Agent': 'SFalcon-PaperTrader/1.0'})
        self.x_cache = {}

    async def close(self):
        await self.client.aclose()

    async def discover(self):
        urls = [
            f'{DEX_BASE}/token-profiles/latest/v1',
            f'{DEX_BASE}/token-boosts/latest/v1',
            f'{DEX_BASE}/token-boosts/top/v1',
        ]
        out, seen = [], set()
        for url in urls:
            try:
                r = await self.client.get(url)
                r.raise_for_status()
                rows = r.json() or []
                if isinstance(rows, dict): rows = [rows]
                for row in rows:
                    if str(row.get('chainId', '')).lower() != 'solana':
                        continue
                    addr = row.get('tokenAddress')
                    if addr and addr not in seen:
                        seen.add(addr); out.append(addr)
            except Exception as e:
                print('[DISCOVERY_SOURCE]', type(e).__name__, str(e)[:120])
        return out[:40]

    async def candidate(self, address):
        try:
            r = await self.client.get(f'{DEX_BASE}/token-pairs/v1/solana/{address}')
            r.raise_for_status()
            pairs = r.json() or []
            if not isinstance(pairs, list): return None
            pairs = [p for p in pairs if p.get('priceUsd')]
            if not pairs: return None
            p = max(pairs, key=lambda x: float(((x.get('liquidity') or {}).get('usd') or 0)))
            tx, vol, pc = p.get('txns') or {}, p.get('volume') or {}, p.get('priceChange') or {}
            base, liq = p.get('baseToken') or {}, p.get('liquidity') or {}
            return Candidate(
                address=base.get('address') or address,
                symbol=base.get('symbol') or '?', name=base.get('name') or 'Unknown',
                price_usd=float(p.get('priceUsd') or 0), liquidity_usd=float(liq.get('usd') or 0),
                market_cap=float(p.get('marketCap') or p.get('fdv') or 0),
                volume_m5=float(vol.get('m5') or 0), volume_h1=float(vol.get('h1') or 0),
                buys_m5=int(((tx.get('m5') or {}).get('buys') or 0)), sells_m5=int(((tx.get('m5') or {}).get('sells') or 0)),
                buys_h1=int(((tx.get('h1') or {}).get('buys') or 0)), sells_h1=int(((tx.get('h1') or {}).get('sells') or 0)),
                price_change_m5=float(pc.get('m5') or 0), price_change_h1=float(pc.get('h1') or 0),
                pair_created_at_ms=p.get('pairCreatedAt'), dex_id=p.get('dexId') or 'unknown', pair_address=p.get('pairAddress') or ''
            )
        except Exception:
            return None

    async def enrich_jupiter(self, c):
        if not JUPITER_API_KEY: return c
        try:
            r = await self.client.get('https://api.jup.ag/tokens/v2/search', params={'query': c.address}, headers={'x-api-key': JUPITER_API_KEY})
            r.raise_for_status(); rows = r.json() or []
            row = next((x for x in rows if x.get('id') == c.address), rows[0] if rows else None)
            if not row: return c
            audit = row.get('audit') or {}
            c.top_holders_pct = audit.get('topHoldersPercentage')
            c.dev_balance_pct = audit.get('devBalancePercentage')
            c.mint_authority_disabled = audit.get('mintAuthorityDisabled')
            c.freeze_authority_disabled = audit.get('freezeAuthorityDisabled')
            c.organic_score = row.get('organicScore')
            c.suspicious = audit.get('isSus', False)
        except Exception:
            pass
        return c

    async def x_stats(self, c):
        if not X_BEARER_TOKEN: return 0, 0
        cached = self.x_cache.get(c.address); now = time.monotonic()
        if cached and now - cached[0] < 30: return cached[1], cached[2]
        try:
            q = f'("${c.symbol}" OR "{c.address}") lang:en -is:retweet'[:500]
            r = await self.client.get('https://api.x.com/2/tweets/search/recent', params={'query': q, 'max_results': 100, 'tweet.fields': 'created_at,public_metrics'}, headers={'Authorization': f'Bearer {X_BEARER_TOKEN}'})
            r.raise_for_status(); data = (r.json() or {}).get('data') or []
            eng = 0
            for post in data:
                m = post.get('public_metrics') or {}
                eng += int(m.get('like_count',0)) + 2*int(m.get('retweet_count',0)) + int(m.get('reply_count',0))
            self.x_cache[c.address] = (now, len(data), eng)
            return len(data), eng
        except Exception:
            return (cached[1], cached[2]) if cached else (0,0)

def clamp(x, lo=0, hi=1): return max(lo, min(hi, x))

def score_candidate(c):
    if c.price_usd <= 0 or c.liquidity_usd < MIN_LIQ or c.volume_m5 < MIN_VOL5:
        c.score = 0; c.score_breakdown = {'rejected':'minimum liquidity/volume failed'}; return c
    total = c.buys_m5 + c.sells_m5
    br = c.buys_m5 / total if total else 0
    liq_mc = c.liquidity_usd / c.market_cap if c.market_cap > 0 else 0
    vol_liq = c.volume_m5 / c.liquidity_usd if c.liquidity_usd else 0
    age_min = 999999
    if c.pair_created_at_ms:
        age_min = max(0, (datetime.now(timezone.utc).timestamp()*1000 - c.pair_created_at_ms)/60000)
    parts = {
        'liquidity': 20*clamp((c.liquidity_usd-25000)/225000),
        'buy_flow': 20*clamp((br-0.50)/0.25),
        'volume': 18*clamp(vol_liq/0.60),
        'momentum': 15*clamp((c.price_change_m5+5)/35),
        'structure': 12*clamp(liq_mc/0.20),
        'age': 5 if 3 <= age_min <= 720 else (2 if age_min <= 2880 else 0),
        'social': min(8, c.social_mentions*0.12 + c.social_engagement/1250),
        'safety': 0,
    }
    if c.mint_authority_disabled is True: parts['safety'] += 3
    if c.freeze_authority_disabled is True: parts['safety'] += 3
    if c.organic_score is not None: parts['safety'] += 4*clamp(c.organic_score/100)
    pen = 0
    if c.price_change_m5 > 80: pen += 12
    if br > .92 and total > 40: pen += 8
    if c.market_cap > 0 and liq_mc < .025: pen += 12
    if c.liquidity_usd < 50000: pen += 4
    if c.suspicious is True: pen += 50
    if c.mint_authority_disabled is False: pen += 20
    if c.freeze_authority_disabled is False: pen += 15
    if c.top_holders_pct is not None: pen += 30 if c.top_holders_pct > 60 else 15 if c.top_holders_pct > 40 else 7 if c.top_holders_pct > 25 else 0
    if c.dev_balance_pct is not None: pen += 25 if c.dev_balance_pct > 15 else 12 if c.dev_balance_pct > 8 else 0
    c.score = max(0, min(100, sum(parts.values()) - pen))
    c.score_breakdown = {**{k: round(v,2) for k,v in parts.items()}, 'penalties':pen, 'buy_ratio':round(br,4), 'liq_to_mcap':round(liq_mc,4), 'vol5m_to_liq':round(vol_liq,4), 'age_minutes':round(age_min,1), 'top_holders_pct':c.top_holders_pct, 'dev_balance_pct':c.dev_balance_pct, 'suspicious':c.suspicious}
    return c

def size_pct(score):
    if score >= 98: return .25
    if score >= 96: return .20
    if score >= 93: return .15
    if score >= 90: return .10
    if score >= 85: return .05
    return 0

def slippage(size, liq):
    if liq <= 0: return 1
    return max(.01, min(.12, .01 + (size/liq)*1.5))

def buy_fill(price, size, liq):
    s = slippage(size, liq); return price*(1+s), s, max(.05, size*.003)

def sell_fill(price, notional, liq):
    s = slippage(notional, liq); return price*(1-s), s, max(.05, notional*.003)

async def init_db():
    parent = os.path.dirname(DB_PATH)
    if parent: os.makedirs(parent, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('starting_equity',?)", (str(STARTING_EQUITY),))
        await db.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('cash',?)", (str(STARTING_EQUITY),))
        await db.commit()

async def cash():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='cash'"); row = await cur.fetchone(); return float(row[0])

async def set_cash(v):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE meta SET value=? WHERE key='cash'", (str(v),)); await db.commit()

class Engine:
    def __init__(self, src): self.src = src; self.last_candidates = {}

    async def positions(self, open_only=True):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            q = "SELECT * FROM positions WHERE status='OPEN'" if open_only else "SELECT * FROM positions ORDER BY id DESC"
            cur = await db.execute(q); return [dict(r) for r in await cur.fetchall()]

    async def equity(self):
        eq = await cash()
        for p in await self.positions():
            c = await self.src.candidate(p['address'])
            if c: eq += p['remaining_qty'] * c.price_usd
        return eq

    async def discover_once(self):
        addrs = await self.src.discover()
        sem = asyncio.Semaphore(10)
        async def load(a):
            async with sem: return await self.src.candidate(a)
        cs = [x for x in await asyncio.gather(*(load(a) for a in addrs)) if x]
        cs.sort(key=lambda c:(c.volume_m5,c.liquidity_usd), reverse=True)
        for c in cs[:10]:
            await self.src.enrich_jupiter(c); c.social_mentions, c.social_engagement = await self.src.x_stats(c)
        cs = [score_candidate(c) for c in cs]; cs.sort(key=lambda c:c.score, reverse=True)
        self.last_candidates = {c.address:c for c in cs[:50]}
        async with aiosqlite.connect(DB_PATH) as db:
            for c in cs[:50]:
                await db.execute('''INSERT INTO candidates(address,symbol,score,price,liquidity,market_cap,volume_m5,buys_m5,sells_m5,social_mentions,social_engagement,breakdown,observed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(address) DO UPDATE SET symbol=excluded.symbol,score=excluded.score,price=excluded.price,liquidity=excluded.liquidity,market_cap=excluded.market_cap,volume_m5=excluded.volume_m5,buys_m5=excluded.buys_m5,sells_m5=excluded.sells_m5,social_mentions=excluded.social_mentions,social_engagement=excluded.social_engagement,breakdown=excluded.breakdown,observed_at=excluded.observed_at''',
                (c.address,c.symbol,c.score,c.price_usd,c.liquidity_usd,c.market_cap,c.volume_m5,c.buys_m5,c.sells_m5,c.social_mentions,c.social_engagement,json.dumps(c.score_breakdown),c.observed_at.isoformat()))
            await db.commit()
        for c in cs:
            if c.score >= ENTRY_SCORE_MIN: await self.maybe_enter(c)

    async def maybe_enter(self, c):
        ps = await self.positions()
        if len(ps) >= MAX_OPEN_POSITIONS or any(p['address']==c.address for p in ps): return
        eq, ca = await self.equity(), await cash()
        exposure = sum(p['remaining_qty']*p['entry_price'] for p in ps)
        room = max(0, eq*MAX_TOTAL_PCT-exposure)
        size = min(ca, eq*min(size_pct(c.score),MAX_SINGLE_PCT), room)
        if size < 20: return
        fill, slip, fee = buy_fill(c.price_usd,size,c.liquidity_usd)
        if slip > MAX_SLIP: return
        qty = max(0,size-fee)/fill
        if qty <= 0: return
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('''INSERT INTO positions(address,symbol,entry_price,size_usd,token_qty,remaining_qty,stop_price,high_price,score,opened_at) VALUES(?,?,?,?,?,?,?,?,?,?)''',(c.address,c.symbol,fill,size,qty,qty,fill*.80,fill,c.score,now))
            pid=cur.lastrowid
            await db.execute('INSERT INTO fills(position_id,side,price,qty,gross_usd,fee_usd,slippage_pct,reason,ts) VALUES(?,?,?,?,?,?,?,?,?)',(pid,'BUY',fill,qty,size,fee,slip,'ENTRY',now)); await db.commit()
        await set_cash(ca-size)
        print(f'[ENTRY] {c.symbol} score={c.score:.1f} size=${size:.2f} fill={fill:.10g} slip={slip*100:.2f}%')

    async def manage_once(self):
        for p in await self.positions():
            c=await self.src.candidate(p['address'])
            if not c or c.price_usd<=0: continue
            price, high=c.price_usd,max(p['high_price'],c.price_usd)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute('UPDATE positions SET high_price=? WHERE id=?',(high,p['id'])); await db.commit()
            if price <= p['stop_price']: await self.sell(p,c,p['remaining_qty'],'STOP'); continue
            if not p['tp1_done'] and price >= p['entry_price']*1.25:
                await self.sell(p,c,min(p['remaining_qty'],p['token_qty']*.25),'TP1'); await self.flag(p['id'],'tp1_done'); continue
            if not p['tp2_done'] and price >= p['entry_price']*1.50:
                await self.sell(p,c,min(p['remaining_qty'],p['token_qty']*.25),'TP2'); await self.flag(p['id'],'tp2_done'); continue
            if high >= p['entry_price']*1.25 and price <= high*.80: await self.sell(p,c,p['remaining_qty'],'TRAIL')

    async def flag(self,pid,field):
        if field not in {'tp1_done','tp2_done'}: return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f'UPDATE positions SET {field}=1 WHERE id=?',(pid,)); await db.commit()

    async def sell(self,p,c,qty,reason):
        if qty<=0:return
        market=qty*c.price_usd; fill,slip,fee=sell_fill(c.price_usd,market,c.liquidity_usd)
        proceeds=qty*fill-fee; pnl=proceeds-qty*p['entry_price']; now=datetime.now(timezone.utc).isoformat(); ca=await cash()
        async with aiosqlite.connect(DB_PATH) as db:
            cur=await db.execute('SELECT remaining_qty,realized_pnl FROM positions WHERE id=?',(p['id'],)); row=await cur.fetchone()
            rem=max(0,float(row[0])-qty); real=float(row[1])+pnl; status='CLOSED' if rem<=p['token_qty']*1e-9 else 'OPEN'
            await db.execute('UPDATE positions SET remaining_qty=?,realized_pnl=?,status=? WHERE id=?',(rem,real,status,p['id']))
            await db.execute('INSERT INTO fills(position_id,side,price,qty,gross_usd,fee_usd,slippage_pct,reason,ts) VALUES(?,?,?,?,?,?,?,?,?)',(p['id'],'SELL',fill,qty,qty*fill,fee,slip,reason,now)); await db.commit()
        await set_cash(ca+proceeds); print(f'[EXIT:{reason}] {p["symbol"]} pnl=${pnl:.2f}')

app=FastAPI(title='SFalcon Paper Trader',version='1.0.0')
sources=Sources(); engine=Engine(sources)

@app.get('/')
async def root(): return {'name':'SFalcon','mode':'PAPER_ONLY','health':'/health','status':'/status'}
@app.get('/health')
async def health(): return {'ok':True,'name':'SFalcon','mode':'PAPER_ONLY'}
@app.get('/status')
async def status():
    ca=await cash(); eq=await engine.equity(); ps=await engine.positions()
    async with aiosqlite.connect(DB_PATH) as db:
        cur=await db.execute('SELECT COALESCE(SUM(realized_pnl),0) FROM positions'); real=float((await cur.fetchone())[0])
    return {'name':'SFalcon','mode':'PAPER_ONLY','starting_equity':STARTING_EQUITY,'cash':ca,'equity':eq,'total_return_pct':(eq/STARTING_EQUITY-1)*100,'realized_pnl':real,'open_positions':len(ps),'scan_interval_seconds':SCAN_INTERVAL,'position_check_seconds':POSITION_CHECK,'x_enabled':bool(X_BEARER_TOKEN),'jupiter_enabled':bool(JUPITER_API_KEY)}
@app.get('/positions')
async def positions(): return await engine.positions(open_only=False)
@app.get('/trades')
async def trades():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row; cur=await db.execute('SELECT * FROM fills ORDER BY id DESC LIMIT 500'); return [dict(r) for r in await cur.fetchall()]
@app.get('/candidates')
async def candidates():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row; cur=await db.execute('SELECT * FROM candidates ORDER BY score DESC LIMIT 50'); rows=[]
        for r in await cur.fetchall():
            d=dict(r)
            try:d['breakdown']=json.loads(d['breakdown'])
            except Exception:pass
            rows.append(d)
        return rows

async def repeat(label,seconds,fn):
    while True:
        try: await fn()
        except Exception as e: print(f'[{label}] error: {e}')
        await asyncio.sleep(seconds)

async def main():
    await init_db(); print('SFalcon started — PAPER ONLY'); print(f'Bankroll: ${STARTING_EQUITY:.2f} | discovery {SCAN_INTERVAL:g}s | positions {POSITION_CHECK:g}s')
    config=uvicorn.Config(app,host=HOST,port=PORT,log_level='info'); server=uvicorn.Server(config)
    try: await asyncio.gather(repeat('DISCOVERY',SCAN_INTERVAL,engine.discover_once),repeat('POSITIONS',POSITION_CHECK,engine.manage_once),server.serve())
    finally: await sources.close()

if __name__=='__main__': asyncio.run(main())
