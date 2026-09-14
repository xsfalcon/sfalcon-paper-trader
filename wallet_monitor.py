import asyncio
import json
import os
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import aiosqlite

SOLANA_RPC = os.getenv("SOLANA_RPC_URL", "https://api.mainnet.solana.com")
WALLET_BATCH_SIZE = int(os.getenv("WALLET_BATCH_SIZE", "10"))
WALLET_SIGNAL_WINDOW_MIN = int(os.getenv("WALLET_SIGNAL_WINDOW_MIN", "45"))
WALLET_TX_LIMIT = int(os.getenv("WALLET_TX_LIMIT", "4"))

# Common quote/native mints are not treated as memecoin signals.
IGNORED_MINTS = {
    "So11111111111111111111111111111111111111112",  # Wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD3gH9H9gwvGv3p6vCwT8hQ",  # USDT (legacy/common)
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_wallet_state (
    address TEXT PRIMARY KEY,
    last_signature TEXT,
    last_checked_at TEXT,
    error_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS wallet_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    wallet_name TEXT NOT NULL,
    mint TEXT NOT NULL,
    direction TEXT NOT NULL,
    token_delta REAL NOT NULL,
    wallet_weight REAL NOT NULL,
    signature TEXT NOT NULL,
    block_time INTEGER,
    observed_at TEXT NOT NULL,
    UNIQUE(wallet_address, mint, signature)
);
CREATE INDEX IF NOT EXISTS idx_wallet_events_mint_time
ON wallet_events(mint, observed_at);
"""

class WalletMonitor:
    def __init__(self, client, db_path, wallets_path="tracked_wallets.json"):
        self.client = client
        self.db_path = db_path
        self.wallets_path = wallets_path
        self.wallets = self._load_wallets()
        self.wallet_by_address = {w["address"]: w for w in self.wallets}
        self._schedule = self._build_schedule()
        self._queue = deque(self._schedule)
        self._rpc_lock = asyncio.Lock()
        self._last_rpc = 0.0
        self.last_cycle_at = None
        self.last_error = None
        self.events_seen = 0

    def _load_wallets(self):
        try:
            with open(self.wallets_path, "r", encoding="utf-8") as f:
                rows = json.load(f)
            out = []
            seen = set()
            for row in rows:
                a = (row.get("address") or "").strip()
                if not a or a in seen:
                    continue
                seen.add(a)
                out.append({
                    "address": a,
                    "name": row.get("name") or "Unnamed Wallet",
                    "groups": row.get("groups") or [],
                    "weight": float(row.get("weight") or 1.0),
                })
            return out
        except Exception as e:
            print("[WALLETS] load error:", e)
            return []

    def _build_schedule(self):
        # Higher-priority user groups are sampled more often while all wallets remain monitored.
        schedule = []
        for w in self.wallets:
            repeats = 1
            groups = set(w.get("groups") or [])
            if "THE ONE" in groups:
                repeats = 4
            elif "Research_SmartMoney" in groups:
                repeats = 3
            elif "Peacock" in groups:
                repeats = 2
            for _ in range(repeats):
                schedule.append(w["address"])
        random.Random(1337).shuffle(schedule)
        return schedule

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def _rpc(self, method, params):
        # Public mainnet RPC is rate-limited. Keep a conservative request cadence.
        async with self._rpc_lock:
            wait = 0.30 - (time.monotonic() - self._last_rpc)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_rpc = time.monotonic()
            r = await self.client.post(
                SOLANA_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            )
            if r.status_code == 429:
                retry = float(r.headers.get("retry-after", "2") or 2)
                await asyncio.sleep(min(max(retry, 1), 10))
                raise RuntimeError("Solana RPC 429")
            r.raise_for_status()
            payload = r.json()
            if payload.get("error"):
                raise RuntimeError(str(payload["error"])[:200])
            return payload.get("result")

    async def _state(self, address):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT last_signature FROM tracked_wallet_state WHERE address=?",
                (address,),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def _save_state(self, address, signature, error=False):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """INSERT INTO tracked_wallet_state(address,last_signature,last_checked_at,error_count)
                   VALUES(?,?,?,?)
                   ON CONFLICT(address) DO UPDATE SET
                     last_signature=COALESCE(excluded.last_signature,tracked_wallet_state.last_signature),
                     last_checked_at=excluded.last_checked_at,
                     error_count=CASE WHEN ? THEN tracked_wallet_state.error_count+1 ELSE tracked_wallet_state.error_count END""",
                (address, signature, now, 1 if error else 0, 1 if error else 0),
            )
            await db.commit()

    async def _record_tx(self, wallet, signature, block_time):
        tx = await self._rpc(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )
        if not tx or not tx.get("meta"):
            return 0
        meta = tx["meta"]
        pre = meta.get("preTokenBalances") or []
        post = meta.get("postTokenBalances") or []

        def balances(rows):
            out = {}
            for b in rows:
                if b.get("owner") != wallet["address"]:
                    continue
                mint = b.get("mint")
                if not mint:
                    continue
                ui = (b.get("uiTokenAmount") or {}).get("uiAmount")
                if ui is None:
                    try:
                        raw = float((b.get("uiTokenAmount") or {}).get("amount") or 0)
                        dec = int((b.get("uiTokenAmount") or {}).get("decimals") or 0)
                        ui = raw / (10 ** dec)
                    except Exception:
                        ui = 0
                out[mint] = float(ui or 0)
            return out

        a, b = balances(pre), balances(post)
        changed = 0
        for mint in set(a) | set(b):
            if mint in IGNORED_MINTS:
                continue
            delta = b.get(mint, 0.0) - a.get(mint, 0.0)
            if abs(delta) <= 1e-12:
                continue
            direction = "ACCUMULATE" if delta > 0 else "DISTRIBUTE"
            async with aiosqlite.connect(self.db_path) as db:
                try:
                    await db.execute(
                        """INSERT OR IGNORE INTO wallet_events
                           (wallet_address,wallet_name,mint,direction,token_delta,wallet_weight,signature,block_time,observed_at)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (
                            wallet["address"],
                            wallet["name"],
                            mint,
                            direction,
                            delta,
                            float(wallet.get("weight") or 1.0),
                            signature,
                            block_time,
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )
                    await db.commit()
                    if db.total_changes:
                        changed += 1
                        self.events_seen += 1
                        print(f'[WALLET:{direction}] {wallet["name"]} mint={mint[:8]}... sig={signature[:8]}...')
                except Exception as e:
                    print("[WALLETS] event insert error:", e)
        return changed

    async def _poll_wallet(self, address):
        wallet = self.wallet_by_address.get(address)
        if not wallet:
            return
        last = await self._state(address)
        try:
            params = [
                address,
                {
                    "limit": WALLET_TX_LIMIT,
                    "commitment": "confirmed",
                },
            ]
            if last:
                params[1]["until"] = last
            rows = await self._rpc("getSignaturesForAddress", params) or []

            # First observation establishes a forward-only baseline. No historical backfill.
            if last is None:
                newest = rows[0]["signature"] if rows else None
                await self._save_state(address, newest, error=False)
                return

            if not rows:
                await self._save_state(address, last, error=False)
                return

            # Process oldest -> newest so event order remains causal.
            valid = [r for r in rows if not r.get("err") and r.get("signature")]
            for row in reversed(valid[:WALLET_TX_LIMIT]):
                await self._record_tx(wallet, row["signature"], row.get("blockTime"))
            await self._save_state(address, rows[0]["signature"], error=False)
        except Exception as e:
            self.last_error = str(e)[:180]
            await self._save_state(address, last, error=True)

    async def scan_once(self):
        if not self._queue:
            self._queue = deque(self._schedule)
        batch = []
        for _ in range(min(WALLET_BATCH_SIZE, len(self._queue))):
            batch.append(self._queue.popleft())
        # Sequential calls are deliberate to respect free public-RPC limits.
        for address in batch:
            await self._poll_wallet(address)
        self.last_cycle_at = datetime.now(timezone.utc).isoformat()

    async def recent_bought_mints(self, minutes=60, limit=40):
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """SELECT mint, SUM(wallet_weight) AS w, COUNT(DISTINCT wallet_address) AS n
                   FROM wallet_events
                   WHERE observed_at>=? AND direction='ACCUMULATE'
                   GROUP BY mint
                   ORDER BY n DESC, w DESC
                   LIMIT ?""",
                (cutoff, limit),
            )
            return [r[0] for r in await cur.fetchall()]

    async def signal_for_mint(self, mint, minutes=None):
        minutes = minutes or WALLET_SIGNAL_WINDOW_MIN
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT wallet_address,wallet_name,direction,wallet_weight,observed_at
                   FROM wallet_events
                   WHERE mint=? AND observed_at>=?
                   ORDER BY observed_at DESC""",
                (mint, cutoff),
            )
            rows = [dict(r) for r in await cur.fetchall()]

        latest_by_wallet = {}
        for r in rows:
            latest_by_wallet.setdefault(r["wallet_address"], r)
        latest = list(latest_by_wallet.values())
        accum = [r for r in latest if r["direction"] == "ACCUMULATE"]
        dist = [r for r in latest if r["direction"] == "DISTRIBUTE"]
        buy_w = sum(float(r["wallet_weight"]) for r in accum)
        sell_w = sum(float(r["wallet_weight"]) for r in dist)

        boost = min(13.0, buy_w * 2.2)
        if len(accum) >= 2:
            boost += 2.5
        if len(accum) >= 3:
            boost += 2.5
        penalty = min(16.0, sell_w * 3.0)
        delta = max(-16.0, min(16.0, boost - penalty))
        return {
            "score_delta": round(delta, 2),
            "accumulating_wallets": len(accum),
            "distributing_wallets": len(dist),
            "buy_weight": round(buy_w, 2),
            "sell_weight": round(sell_w, 2),
            "accumulators": [r["wallet_name"] for r in accum[:8]],
            "distributors": [r["wallet_name"] for r in dist[:8]],
            "accumulator_addresses": [r["wallet_address"] for r in accum[:16]],
            "distributor_addresses": [r["wallet_address"] for r in dist[:16]],
            "window_minutes": minutes,
        }

    async def status(self):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM tracked_wallet_state")
            initialized = int((await cur.fetchone())[0])
            cur = await db.execute("SELECT COUNT(*) FROM wallet_events")
            events = int((await cur.fetchone())[0])
            cur = await db.execute(
                "SELECT COUNT(*) FROM wallet_events WHERE observed_at>=?",
                ((datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),),
            )
            events_1h = int((await cur.fetchone())[0])
        return {
            "configured_wallets": len(self.wallets),
            "initialized_wallets": initialized,
            "events_total": events,
            "events_last_hour": events_1h,
            "last_cycle_at": self.last_cycle_at,
            "last_error": self.last_error,
            "rpc": SOLANA_RPC,
        }

    async def recent_events(self, limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM wallet_events ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in await cur.fetchall()]
