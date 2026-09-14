import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiosqlite
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from wallet_monitor import WalletMonitor
from lore_monitor import LoreMonitor
from social_monitor import SocialMonitor
from exchange_monitor import ExchangeMonitor
from research_gate import ResearchGate, volatility_signal, postbond_signal

load_dotenv()

VERSION = "4.0.0"
STARTING_EQUITY = float(os.getenv("STARTING_EQUITY_USD", "1000"))
SCAN_INTERVAL = float(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
POSITION_CHECK = float(os.getenv("POSITION_CHECK_SECONDS", "5"))
WALLET_CHECK = float(os.getenv("WALLET_CHECK_SECONDS", "5"))
LORE_CHECK = float(os.getenv("LORE_CHECK_SECONDS", "15"))
SOCIAL_CHECK = float(os.getenv("SOCIAL_CHECK_SECONDS", "15"))
EXCHANGE_CHECK = float(os.getenv("EXCHANGE_CHECK_SECONDS", "60"))
MAX_SINGLE_PCT = float(os.getenv("MAX_SINGLE_POSITION_PCT", "0.25"))
MAX_TOTAL_PCT = float(os.getenv("MAX_TOTAL_EXPOSURE_PCT", "0.50"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
ENTRY_SCORE_MIN = float(os.getenv("ENTRY_SCORE_MIN", "85"))
MIN_LIQ = float(os.getenv("MIN_LIQUIDITY_USD", "25000"))
MIN_VOL5 = float(os.getenv("MIN_5M_VOLUME_USD", "5000"))
MAX_SLIP = float(os.getenv("MAX_TOP_LEVEL_SLIPPAGE_PCT", "0.05"))
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY") or None
DB_PATH = os.getenv("DB_PATH", "sfalcon.db")
HOST = os.getenv("API_HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", os.getenv("API_PORT", "8787")))

DEX_BASE = "https://api.dexscreener.com"
JUP_BASE = "https://api.jup.ag"
GECKO_BASE = "https://api.geckoterminal.com/api/v2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    symbol TEXT NOT NULL,
    entry_price REAL NOT NULL,
    size_usd REAL NOT NULL,
    token_qty REAL NOT NULL,
    remaining_qty REAL NOT NULL,
    stop_price REAL NOT NULL,
    high_price REAL NOT NULL,
    score REAL NOT NULL,
    opened_at TEXT NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    tp1_done INTEGER NOT NULL DEFAULT 0,
    tp2_done INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    gross_usd REAL NOT NULL,
    fee_usd REAL NOT NULL,
    slippage_pct REAL NOT NULL,
    reason TEXT NOT NULL,
    ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS candidates (
    address TEXT PRIMARY KEY,
    symbol TEXT,
    score REAL,
    price REAL,
    liquidity REAL,
    market_cap REAL,
    volume_m5 REAL,
    buys_m5 INTEGER,
    sells_m5 INTEGER,
    social_mentions INTEGER,
    social_engagement INTEGER,
    breakdown TEXT,
    observed_at TEXT
);
"""

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
    holder_count: int | None = None
    suspicious: bool | None = None
    source_tags: list[str] = field(default_factory=list)
    score: float = 0.0
    score_breakdown: dict = field(default_factory=dict)
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class Sources:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=14,
            headers={"User-Agent": f"SFalcon-PaperTrader/{VERSION}"},
        )
        self.jupiter_cache = {}
        self.health = {
            "jupiter_ok": False,
            "jupiter_last_error": None,
            "gecko_ok": False,
            "gecko_last_error": None,
            "dex_ok": False,
            "dex_last_error": None,
            "last_discovery_at": None,
            "last_discovery_count": 0,
        }
        self._jup_lock = asyncio.Lock()
        self._last_jup = 0.0
        self._dex_lock = asyncio.Lock()
        self._last_dex = 0.0
        self._gecko_lock = asyncio.Lock()
        self._last_gecko = 0.0

    async def close(self):
        await self.client.aclose()

    async def _limited_get(self, url, kind, params=None, headers=None):
        if kind == "jupiter":
            lock, min_gap = self._jup_lock, 1.05
        elif kind == "dex":
            lock, min_gap = self._dex_lock, 0.38
        else:
            lock, min_gap = self._gecko_lock, 2.2

        async with lock:
            now = time.monotonic()
            last = self._last_jup if kind == "jupiter" else self._last_dex if kind == "dex" else self._last_gecko
            wait = min_gap - (now - last)
            if wait > 0:
                await asyncio.sleep(wait)
            if kind == "jupiter":
                self._last_jup = time.monotonic()
            elif kind == "dex":
                self._last_dex = time.monotonic()
            else:
                self._last_gecko = time.monotonic()

            r = await self.client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                retry = float(r.headers.get("retry-after", "2") or 2)
                await asyncio.sleep(min(max(retry, 1), 15))
                raise RuntimeError(f"{kind} 429")
            r.raise_for_status()
            return r.json()

    async def _jup(self, path, params=None):
        if not JUPITER_API_KEY:
            return []
        try:
            data = await self._limited_get(
                f"{JUP_BASE}{path}",
                "jupiter",
                params=params,
                headers={"x-api-key": JUPITER_API_KEY},
            )
            self.health["jupiter_ok"] = True
            self.health["jupiter_last_error"] = None
            return data or []
        except Exception as e:
            self.health["jupiter_ok"] = False
            self.health["jupiter_last_error"] = str(e)[:180]
            return []

    async def discover_jupiter(self):
        if not JUPITER_API_KEY:
            return []
        specs = [
            ("/tokens/v2/recent", None, "jup_recent"),
            ("/tokens/v2/toporganicscore/5m", {"limit": 40}, "jup_organic"),
            ("/tokens/v2/toptrending/5m", {"limit": 40}, "jup_trending"),
            ("/tokens/v2/toptraded/5m", {"limit": 40}, "jup_traded"),
        ]
        ordered, seen = [], set()
        for path, params, tag in specs:
            rows = await self._jup(path, params)
            if not isinstance(rows, list):
                continue
            for row in rows:
                mint = row.get("id")
                if not mint:
                    continue
                cached = dict(row)
                tags = list(self.jupiter_cache.get(mint, {}).get("_source_tags") or [])
                if tag not in tags:
                    tags.append(tag)
                cached["_source_tags"] = tags
                self.jupiter_cache[mint] = cached
                if mint not in seen:
                    seen.add(mint)
                    ordered.append(mint)
        return ordered

    async def discover_gecko(self):
        ordered, seen = [], set()
        endpoints = [
            (f"{GECKO_BASE}/networks/solana/trending_pools", {"include": "base_token", "duration": "5m"}, "gecko_trending"),
            (f"{GECKO_BASE}/networks/solana/new_pools", {"include": "base_token"}, "gecko_new"),
        ]
        ok = False
        for url, params, tag in endpoints:
            try:
                payload = await self._limited_get(url, "gecko", params=params)
                ok = True
                for row in payload.get("data") or []:
                    rel = (((row.get("relationships") or {}).get("base_token") or {}).get("data") or {})
                    token_id = rel.get("id") or ""
                    mint = token_id.split("solana_", 1)[-1] if token_id.startswith("solana_") else None
                    if mint and mint not in seen:
                        seen.add(mint)
                        ordered.append(mint)
                self.health["gecko_last_error"] = None
            except Exception as e:
                self.health["gecko_last_error"] = str(e)[:180]
        self.health["gecko_ok"] = ok
        return ordered

    async def discover_dex(self):
        ordered, seen = [], set()
        # A single low-frequency DEX Screener discovery feed is retained as a fallback.
        try:
            rows = await self._limited_get(f"{DEX_BASE}/token-profiles/latest/v1", "dex")
            if isinstance(rows, dict):
                rows = [rows]
            for row in rows or []:
                if str(row.get("chainId", "")).lower() != "solana":
                    continue
                mint = row.get("tokenAddress")
                if mint and mint not in seen:
                    seen.add(mint)
                    ordered.append(mint)
            self.health["dex_ok"] = True
            self.health["dex_last_error"] = None
        except Exception as e:
            self.health["dex_ok"] = False
            self.health["dex_last_error"] = str(e)[:180]
        return ordered

    async def discover(self, wallet_mints=None):
        wallet_mints = wallet_mints or []
        jup = await self.discover_jupiter()
        gecko = await self.discover_gecko()
        dex = await self.discover_dex()

        # Wallet-driven candidates are highest priority; then organic/trending feeds.
        ordered = []
        seen = set()
        for source in (wallet_mints, jup, gecko, dex):
            for mint in source:
                if mint and mint not in seen:
                    seen.add(mint)
                    ordered.append(mint)
        self.health["last_discovery_at"] = datetime.now(timezone.utc).isoformat()
        self.health["last_discovery_count"] = len(ordered)
        return ordered[:80]

    async def candidate(self, address):
        try:
            pairs = await self._limited_get(
                f"{DEX_BASE}/token-pairs/v1/solana/{address}",
                "dex",
            )
            if not isinstance(pairs, list):
                return None
            pairs = [p for p in pairs if p.get("priceUsd")]
            if not pairs:
                return None
            p = max(pairs, key=lambda x: float(((x.get("liquidity") or {}).get("usd") or 0)))
            tx = p.get("txns") or {}
            vol = p.get("volume") or {}
            pc = p.get("priceChange") or {}
            base = p.get("baseToken") or {}
            liq = p.get("liquidity") or {}
            c = Candidate(
                address=base.get("address") or address,
                symbol=base.get("symbol") or "?",
                name=base.get("name") or "Unknown",
                price_usd=float(p.get("priceUsd") or 0),
                liquidity_usd=float(liq.get("usd") or 0),
                market_cap=float(p.get("marketCap") or p.get("fdv") or 0),
                volume_m5=float(vol.get("m5") or 0),
                volume_h1=float(vol.get("h1") or 0),
                buys_m5=int(((tx.get("m5") or {}).get("buys") or 0)),
                sells_m5=int(((tx.get("m5") or {}).get("sells") or 0)),
                buys_h1=int(((tx.get("h1") or {}).get("buys") or 0)),
                sells_h1=int(((tx.get("h1") or {}).get("sells") or 0)),
                price_change_m5=float(pc.get("m5") or 0),
                price_change_h1=float(pc.get("h1") or 0),
                pair_created_at_ms=p.get("pairCreatedAt"),
                dex_id=p.get("dexId") or "unknown",
                pair_address=p.get("pairAddress") or "",
            )
            cached = self.jupiter_cache.get(c.address) or self.jupiter_cache.get(address)
            if cached:
                self._apply_jup(c, cached)
            return c
        except Exception as e:
            self.health["dex_last_error"] = str(e)[:180]
            return None

    def _apply_jup(self, c, row):
        audit = row.get("audit") or {}
        c.top_holders_pct = audit.get("topHoldersPercentage")
        c.dev_balance_pct = audit.get("devBalancePercentage")
        c.mint_authority_disabled = audit.get("mintAuthorityDisabled")
        c.freeze_authority_disabled = audit.get("freezeAuthorityDisabled")
        c.organic_score = row.get("organicScore")
        c.holder_count = row.get("holderCount")
        c.suspicious = audit.get("isSus", False)
        c.source_tags = list(row.get("_source_tags") or c.source_tags)
        return c

    async def enrich_jupiter(self, c):
        cached = self.jupiter_cache.get(c.address)
        if cached:
            return self._apply_jup(c, cached)
        rows = await self._jup("/tokens/v2/search", {"query": c.address})
        if isinstance(rows, list) and rows:
            row = next((x for x in rows if x.get("id") == c.address), rows[0])
            self.jupiter_cache[c.address] = row
            return self._apply_jup(c, row)
        return c

def clamp(x, lo=0, hi=1):
    return max(lo, min(hi, x))

def score_candidate(c):
    if c.price_usd <= 0 or c.liquidity_usd < MIN_LIQ or c.volume_m5 < MIN_VOL5:
        c.score = 0
        c.score_breakdown = {"rejected": "minimum liquidity/volume failed"}
        return c

    total = c.buys_m5 + c.sells_m5
    buy_ratio = c.buys_m5 / total if total else 0
    liq_mc = c.liquidity_usd / c.market_cap if c.market_cap > 0 else 0
    vol_liq = c.volume_m5 / c.liquidity_usd if c.liquidity_usd else 0
    age_min = 999999.0
    if c.pair_created_at_ms:
        age_min = max(
            0,
            (datetime.now(timezone.utc).timestamp() * 1000 - c.pair_created_at_ms) / 60000,
        )

    parts = {
        "liquidity": 18 * clamp((c.liquidity_usd - 25000) / 225000),
        "buy_flow": 18 * clamp((buy_ratio - 0.50) / 0.25),
        "volume": 16 * clamp(vol_liq / 0.60),
        "momentum": 13 * clamp((c.price_change_m5 + 5) / 35),
        "structure": 10 * clamp(liq_mc / 0.20),
        "age": 5 if 3 <= age_min <= 720 else (2 if age_min <= 2880 else 0),
        "organic": 0,
        "safety": 0,
        "source_quality": 0,
    }

    if c.organic_score is not None:
        parts["organic"] = 8 * clamp(float(c.organic_score) / 100)
    if "jup_organic" in c.source_tags:
        parts["source_quality"] += 3
    if "jup_trending" in c.source_tags:
        parts["source_quality"] += 2
    if "jup_traded" in c.source_tags:
        parts["source_quality"] += 1

    if c.mint_authority_disabled is True:
        parts["safety"] += 2
    if c.freeze_authority_disabled is True:
        parts["safety"] += 2
    if c.holder_count is not None:
        parts["safety"] += 2 * clamp(float(c.holder_count) / 1500)

    penalty = 0.0
    if c.price_change_m5 > 80:
        penalty += 12
    if buy_ratio > 0.92 and total > 40:
        penalty += 8
    if c.market_cap > 0 and liq_mc < 0.025:
        penalty += 12
    if c.liquidity_usd < 50000:
        penalty += 4
    if c.suspicious is True:
        penalty += 50
    if c.mint_authority_disabled is False:
        penalty += 20
    if c.freeze_authority_disabled is False:
        penalty += 15
    if c.top_holders_pct is not None:
        p = float(c.top_holders_pct)
        penalty += 30 if p > 60 else 15 if p > 40 else 7 if p > 25 else 0
    if c.dev_balance_pct is not None:
        p = float(c.dev_balance_pct)
        penalty += 25 if p > 15 else 12 if p > 8 else 0

    c.score = max(0, min(100, sum(parts.values()) - penalty))
    c.score_breakdown = {
        **{k: round(v, 2) for k, v in parts.items()},
        "penalties": round(penalty, 2),
        "buy_ratio": round(buy_ratio, 4),
        "liq_to_mcap": round(liq_mc, 4),
        "vol5m_to_liq": round(vol_liq, 4),
        "age_minutes": round(age_min, 1),
        "organic_score": c.organic_score,
        "holder_count": c.holder_count,
        "top_holders_pct": c.top_holders_pct,
        "dev_balance_pct": c.dev_balance_pct,
        "suspicious": c.suspicious,
        "source_tags": c.source_tags,
    }
    return c

def size_pct(score):
    if score >= 98:
        return 0.25
    if score >= 96:
        return 0.20
    if score >= 93:
        return 0.15
    if score >= 90:
        return 0.10
    if score >= 85:
        return 0.05
    return 0

def slippage(size, liq):
    if liq <= 0:
        return 1
    return max(0.01, min(0.12, 0.01 + (size / liq) * 1.5))

def buy_fill(price, size, liq):
    s = slippage(size, liq)
    return price * (1 + s), s, max(0.05, size * 0.003)

def sell_fill(price, notional, liq):
    s = slippage(notional, liq)
    return price * (1 - s), s, max(0.05, notional * 0.003)

async def init_db():
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('starting_equity',?)",
            (str(STARTING_EQUITY),),
        )
        await db.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('cash',?)",
            (str(STARTING_EQUITY),),
        )
        await db.commit()

async def cash():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='cash'")
        row = await cur.fetchone()
        return float(row[0])

async def set_cash(v):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE meta SET value=? WHERE key='cash'", (str(v),))
        await db.commit()

class Engine:
    def __init__(self, src, wallets, lore, social, exchange, research):
        self.src = src
        self.wallets = wallets
        self.lore = lore
        self.social = social
        self.exchange = exchange
        self.research = research
        self.last_candidates = {}
        self.social_research_per_scan = int(os.getenv("SOCIAL_RESEARCH_PER_SCAN", "3"))

    async def positions(self, open_only=True):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            q = "SELECT * FROM positions WHERE status='OPEN'" if open_only else "SELECT * FROM positions ORDER BY id DESC"
            cur = await db.execute(q)
            return [dict(r) for r in await cur.fetchall()]

    async def equity(self):
        eq = await cash()
        for p in await self.positions():
            c = await self.src.candidate(p["address"])
            if c:
                eq += p["remaining_qty"] * c.price_usd
        return eq

    async def _initial_signals(self, c, wallet_mints, lore_mints, social_mints):
        score_candidate(c)
        wallet = await self.wallets.signal_for_mint(c.address)
        c.score = max(0, min(100, c.score + float(wallet.get("score_delta") or 0)))

        lore = await self.lore.signal_for_mint(c.address)
        lore_delta = float(lore.get("score_delta") or 0) if lore.get("available") else 0.0
        c.score = max(0, min(100, c.score + lore_delta))

        postbond = postbond_signal(c, lore)
        c.score = max(0, min(100, c.score + float(postbond.get("score_delta") or 0)))

        volatility = volatility_signal(c)
        c.score = max(0, min(100, c.score + float(volatility.get("score_delta") or 0)))

        exchange = await self.exchange.signal_for_candidate(c.symbol, c.name, c.price_change_m5)
        c.score = max(0, min(100, c.score + float(exchange.get("score_delta") or 0)))

        social = await self.social.signal_for_candidate(
            c.address, c.symbol, c.name, wallet_signal=wallet, price_change_m5=c.price_change_m5
        )
        c.score = max(0, min(100, c.score + float(social.get("score_delta") or 0)))

        if c.address in wallet_mints and "tracked_wallet" not in c.source_tags:
            c.source_tags.append("tracked_wallet")
        if c.address in lore_mints and "bond_lore" not in c.source_tags:
            c.source_tags.append("bond_lore")
        if c.address in social_mints and "crypto_twitter" not in c.source_tags:
            c.source_tags.append("crypto_twitter")
        if postbond.get("available") and "postbond" not in c.source_tags:
            c.source_tags.append("postbond")
        if volatility.get("triggered") and "volatility_radar" not in c.source_tags:
            c.source_tags.append("volatility_radar")

        signals = {
            "wallet": wallet,
            "lore": lore,
            "postbond": postbond,
            "volatility": volatility,
            "exchange": exchange,
            "social": social,
        }
        c.score_breakdown.update({
            "wallet_signal": wallet,
            "prebond_lore": lore,
            "postbond_signal": postbond,
            "volatility_signal": volatility,
            "exchange_signal": exchange,
            "social_signal": social,
            "source_tags": c.source_tags,
        })
        return signals

    def _needs_deep_research(self, c, signals):
        return (
            c.score >= 65
            or abs(float(signals["wallet"].get("score_delta") or 0)) >= 5
            or float(signals["lore"].get("score_delta") or 0) >= 4
            or bool(signals["postbond"].get("available"))
            or bool(signals["volatility"].get("triggered"))
            or float(signals["exchange"].get("score_delta") or 0) >= 3
            or int(signals["social"].get("mentions") or 0) >= 2
        )

    async def discover_once(self):
        wallet_mints = await self.wallets.recent_bought_mints(minutes=75, limit=45)
        lore_mints = await self.lore.priority_mints(limit=55)
        social_mints = await self.social.recent_mints(minutes=90, limit=35)
        priority_mints, seen_priority = [], set()
        for mint in list(wallet_mints) + list(lore_mints) + list(social_mints):
            if mint and mint not in seen_priority:
                seen_priority.add(mint); priority_mints.append(mint)
        addrs = await self.src.discover(wallet_mints=priority_mints)

        sem = asyncio.Semaphore(4)
        async def load(a):
            async with sem:
                return await self.src.candidate(a)
        cs = [x for x in await asyncio.gather(*(load(a) for a in addrs)) if x]
        cs.sort(key=lambda c: (c.volume_m5, c.liquidity_usd), reverse=True)

        # Jupiter enrichment is deliberately limited because the free tier is roughly 1 RPS.
        for c in cs[:14]:
            await self.src.enrich_jupiter(c)

        signals_by_mint = {}
        for c in cs:
            signals_by_mint[c.address] = await self._initial_signals(c, wallet_mints, lore_mints, social_mints)

        # Broad free-CT searches are the expensive/noisiest free operation. Research only the best few
        # candidates per discovery cycle, then recompute the social component from the new evidence.
        deep = [c for c in cs if self._needs_deep_research(c, signals_by_mint[c.address])]
        deep.sort(key=lambda c: c.score, reverse=True)
        for c in deep[: self.social_research_per_scan]:
            sigs = signals_by_mint[c.address]
            old_social = float(sigs["social"].get("score_delta") or 0)
            await self.social.research_candidate(c.address, c.symbol, c.name)
            social = await self.social.signal_for_candidate(
                c.address, c.symbol, c.name, wallet_signal=sigs["wallet"], price_change_m5=c.price_change_m5
            )
            c.score = max(0, min(100, c.score - old_social + float(social.get("score_delta") or 0)))
            sigs["social"] = social
            c.score_breakdown["social_signal"] = social
            if social.get("mentions") and "crypto_twitter_researched" not in c.source_tags:
                c.source_tags.append("crypto_twitter_researched")

        # Research gate is mandatory before any entry. Radar events are informational even when rejected.
        research_results = {}
        for c in cs:
            sigs = signals_by_mint[c.address]
            result = self.research.evaluate(c, sigs, ENTRY_SCORE_MIN, MIN_LIQ, MIN_VOL5)
            research_results[c.address] = result
            c.score_breakdown["research_gate"] = result
            c.score_breakdown["source_tags"] = c.source_tags
            await self.research.record(c, result, sigs)
            if sigs["volatility"].get("triggered"):
                await self.research.record_radar(c, "volatility", abs(float(sigs["volatility"].get("score_delta") or 0)), sigs["volatility"])

        cs = sorted(cs, key=lambda c: c.score, reverse=True)
        self.last_candidates = {c.address: c for c in cs[:75]}

        async with aiosqlite.connect(DB_PATH) as db:
            for c in cs[:75]:
                social = signals_by_mint[c.address]["social"]
                await db.execute(
                    """INSERT INTO candidates(address,symbol,score,price,liquidity,market_cap,volume_m5,
                        buys_m5,sells_m5,social_mentions,social_engagement,breakdown,observed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(address) DO UPDATE SET
                        symbol=excluded.symbol,score=excluded.score,price=excluded.price,
                        liquidity=excluded.liquidity,market_cap=excluded.market_cap,
                        volume_m5=excluded.volume_m5,buys_m5=excluded.buys_m5,
                        sells_m5=excluded.sells_m5,social_mentions=excluded.social_mentions,
                        social_engagement=excluded.social_engagement,breakdown=excluded.breakdown,
                        observed_at=excluded.observed_at""",
                    (c.address,c.symbol,c.score,c.price_usd,c.liquidity_usd,c.market_cap,c.volume_m5,
                     c.buys_m5,c.sells_m5,int(social.get("mentions") or 0),
                     int(social.get("independent_positive_accounts") or 0),json.dumps(c.score_breakdown),c.observed_at.isoformat()),
                )
            await db.commit()

        for c in cs:
            if research_results[c.address]["verdict"] == "QUALIFIED" and c.score >= ENTRY_SCORE_MIN:
                await self.maybe_enter(c)

    async def maybe_enter(self, c):
        ps = await self.positions()
        if len(ps) >= MAX_OPEN_POSITIONS or any(p["address"] == c.address for p in ps): return
        eq, ca = await self.equity(), await cash()
        exposure = sum(p["remaining_qty"] * p["entry_price"] for p in ps)
        room = max(0, eq * MAX_TOTAL_PCT - exposure)
        size = min(ca, eq * min(size_pct(c.score), MAX_SINGLE_PCT), room)
        if size < 20: return
        fill, slip, fee = buy_fill(c.price_usd, size, c.liquidity_usd)
        if slip > MAX_SLIP: return
        qty = max(0, size - fee) / fill
        if qty <= 0: return
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""INSERT INTO positions(address,symbol,entry_price,size_usd,token_qty,remaining_qty,
                stop_price,high_price,score,opened_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (c.address,c.symbol,fill,size,qty,qty,fill*.80,fill,c.score,now))
            pid=cur.lastrowid
            await db.execute("""INSERT INTO fills(position_id,side,price,qty,gross_usd,fee_usd,slippage_pct,reason,ts)
                VALUES(?,?,?,?,?,?,?,?,?)""",(pid,"BUY",fill,qty,size,fee,slip,"ENTRY_RESEARCH_QUALIFIED",now))
            await db.commit()
        await set_cash(ca-size)
        print(f"[ENTRY] {c.symbol} score={c.score:.1f} size=${size:.2f} fill={fill:.10g} slip={slip*100:.2f}%")

    async def manage_once(self):
        for p in await self.positions():
            c = await self.src.candidate(p["address"])
            if not c or c.price_usd <= 0: continue
            price, high = c.price_usd, max(p["high_price"], c.price_usd)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE positions SET high_price=? WHERE id=?",(high,p["id"])); await db.commit()
            if price <= p["stop_price"]:
                await self.sell(p,c,p["remaining_qty"],"STOP"); continue
            wallet = await self.wallets.signal_for_mint(p["address"], minutes=20)
            if wallet["score_delta"] <= -8 and wallet["distributing_wallets"] >= 2:
                await self.sell(p,c,p["remaining_qty"],"WALLET_DISTRIBUTION"); continue
            social = await self.social.signal_for_candidate(p["address"],p["symbol"],None,wallet_signal=wallet,price_change_m5=c.price_change_m5)
            if social.get("score_delta",0) <= -8 and wallet.get("distributing_wallets",0) >= 1:
                await self.sell(p,c,p["remaining_qty"],"SOCIAL_AND_WALLET_RISK"); continue
            if not p["tp1_done"] and price >= p["entry_price"]*1.25:
                await self.sell(p,c,min(p["remaining_qty"],p["token_qty"]*.25),"TP1"); await self.flag(p["id"],"tp1_done"); continue
            if not p["tp2_done"] and price >= p["entry_price"]*1.50:
                await self.sell(p,c,min(p["remaining_qty"],p["token_qty"]*.25),"TP2"); await self.flag(p["id"],"tp2_done"); continue
            if high >= p["entry_price"]*1.25 and price <= high*.80:
                await self.sell(p,c,p["remaining_qty"],"TRAIL")

    async def flag(self,pid,field):
        if field not in {"tp1_done","tp2_done"}: return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f"UPDATE positions SET {field}=1 WHERE id=?",(pid,)); await db.commit()

    async def sell(self,p,c,qty,reason):
        if qty<=0:return
        market=qty*c.price_usd; fill,slip,fee=sell_fill(c.price_usd,market,c.liquidity_usd)
        proceeds=qty*fill-fee; pnl=proceeds-qty*p["entry_price"]; now=datetime.now(timezone.utc).isoformat(); ca=await cash()
        async with aiosqlite.connect(DB_PATH) as db:
            cur=await db.execute("SELECT remaining_qty,realized_pnl FROM positions WHERE id=?",(p["id"],)); row=await cur.fetchone()
            rem=max(0,float(row[0])-qty); real=float(row[1])+pnl; status="CLOSED" if rem<=p["token_qty"]*1e-9 else "OPEN"
            await db.execute("UPDATE positions SET remaining_qty=?,realized_pnl=?,status=? WHERE id=?",(rem,real,status,p["id"]))
            await db.execute("INSERT INTO fills(position_id,side,price,qty,gross_usd,fee_usd,slippage_pct,reason,ts) VALUES(?,?,?,?,?,?,?,?,?)",(p["id"],"SELL",fill,qty,qty*fill,fee,slip,reason,now)); await db.commit()
        await set_cash(ca+proceeds); print(f'[EXIT:{reason}] {p["symbol"]} pnl=${pnl:.2f}')

app=FastAPI(title="SFalcon Paper Trader",version=VERSION)
sources=Sources()
wallet_monitor=WalletMonitor(sources.client,DB_PATH)
lore_monitor=LoreMonitor(sources.client,DB_PATH,JUPITER_API_KEY)
social_monitor=SocialMonitor(sources.client,DB_PATH,wallet_monitor)
exchange_monitor=ExchangeMonitor(sources.client,DB_PATH)
research_gate=ResearchGate(DB_PATH)
engine=Engine(sources,wallet_monitor,lore_monitor,social_monitor,exchange_monitor,research_gate)

@app.get("/")
async def root():
    return {"name":"SFalcon","version":VERSION,"mode":"PAPER_ONLY","health":"/health","status":"/status","candidates":"/candidates","research":"/research","volatile":"/volatile","prebond":"/prebond","postbond":"/postbond","wallets":"/wallet-status","wallet_events":"/wallet-events","social_status":"/social-status","social_events":"/social-events","exchange_events":"/exchange-events","lore_status":"/lore-status"}

@app.get("/health")
async def health(): return {"ok":True,"name":"SFalcon","version":VERSION,"mode":"PAPER_ONLY"}

@app.get("/status")
async def status():
    ca=await cash(); eq=await engine.equity(); ps=await engine.positions(); ws=await wallet_monitor.status(); ls=await lore_monitor.status(); ss=await social_monitor.status(); es=await exchange_monitor.status()
    async with aiosqlite.connect(DB_PATH) as db:
        cur=await db.execute("SELECT COALESCE(SUM(realized_pnl),0) FROM positions"); real=float((await cur.fetchone())[0])
        cur=await db.execute("SELECT COUNT(*) FROM positions"); trade_count=int((await cur.fetchone())[0])
    return {"name":"SFalcon","version":VERSION,"mode":"PAPER_ONLY","starting_equity":STARTING_EQUITY,"cash":ca,"equity":eq,"total_return_pct":(eq/STARTING_EQUITY-1)*100,"realized_pnl":real,"positions_ever_opened":trade_count,"open_positions":len(ps),"scan_interval_seconds":SCAN_INTERVAL,"position_check_seconds":POSITION_CHECK,"wallet_check_seconds":WALLET_CHECK,"lore_check_seconds":LORE_CHECK,"social_check_seconds":SOCIAL_CHECK,"exchange_check_seconds":EXCHANGE_CHECK,"jupiter_enabled":bool(JUPITER_API_KEY),"paid_x_api":False,"source_health":sources.health,"wallet_monitor":ws,"bond_lore_monitor":ls,"social_monitor":ss,"exchange_monitor":es}

@app.get("/positions")
async def positions(): return await engine.positions(open_only=False)

@app.get("/trades")
async def trades():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row; cur=await db.execute("SELECT * FROM fills ORDER BY id DESC LIMIT 500"); return [dict(r) for r in await cur.fetchall()]

@app.get("/candidates")
async def candidates():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row; cur=await db.execute("SELECT * FROM candidates ORDER BY score DESC LIMIT 75"); rows=[]
        for r in await cur.fetchall():
            d=dict(r)
            try:d["breakdown"]=json.loads(d["breakdown"])
            except Exception:pass
            rows.append(d)
        return rows

@app.get("/wallet-status")
async def wallet_status(): return await wallet_monitor.status()
@app.get("/wallet-events")
async def wallet_events(): return await wallet_monitor.recent_events(limit=150)
@app.get("/lore-status")
async def lore_status(): return await lore_monitor.status()
@app.get("/prebond")
async def prebond(): return await lore_monitor.top_prebond(limit=75)
@app.get("/postbond")
async def postbond(): return await lore_monitor.top_postbond(limit=75)
@app.get("/social-status")
async def social_status(): return await social_monitor.status()
@app.get("/social-events")
async def social_events(): return await social_monitor.recent_posts(limit=150)
@app.get("/exchange-events")
async def exchange_events(): return await exchange_monitor.recent_events(limit=100)
@app.get("/research")
async def research(): return await research_gate.recent(limit=100)
@app.get("/volatile")
async def volatile(): return await research_gate.volatile(limit=100)

async def repeat(label,seconds,fn):
    while True:
        try: await fn()
        except Exception as e: print(f"[{label}] error: {e}")
        await asyncio.sleep(seconds)

async def main():
    await init_db(); await wallet_monitor.init_db(); await lore_monitor.init_db(); await social_monitor.init_db(); await exchange_monitor.init_db(); await research_gate.init_db()
    print("SFalcon v4 started — PAPER ONLY")
    print(f"Bankroll: ${STARTING_EQUITY:.2f} | discovery {SCAN_INTERVAL:g}s | positions {POSITION_CHECK:g}s | wallet {WALLET_CHECK:g}s | bond lore {LORE_CHECK:g}s | social {SOCIAL_CHECK:g}s | tracked wallets {len(wallet_monitor.wallets)} | CT accounts {len(social_monitor.accounts)}")
    config=uvicorn.Config(app,host=HOST,port=PORT,log_level="info"); server=uvicorn.Server(config)
    try:
        await asyncio.gather(
            repeat("DISCOVERY",SCAN_INTERVAL,engine.discover_once),
            repeat("POSITIONS",POSITION_CHECK,engine.manage_once),
            repeat("WALLETS",WALLET_CHECK,wallet_monitor.scan_once),
            repeat("BOND_LORE",LORE_CHECK,lore_monitor.scan_once),
            repeat("SOCIAL",SOCIAL_CHECK,social_monitor.scan_once),
            repeat("EXCHANGE",EXCHANGE_CHECK,exchange_monitor.scan_once),
            lore_monitor.stream_new_tokens(),
            server.serve(),
        )
    finally: await sources.close()

if __name__=="__main__": asyncio.run(main())
