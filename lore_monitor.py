import asyncio
import json
import math
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import aiosqlite
import websockets

PUMP_COIN_BASE = os.getenv("PUMP_COIN_BASE", "https://frontend-api-v3.pump.fun")
PUMP_STREAM_URL = os.getenv("PUMP_STREAM_URL", "wss://pumpportal.fun/api/data")
PUMP_STREAM_FALLBACK_URL = os.getenv("PUMP_STREAM_FALLBACK_URL", "wss://pumpdev.io/ws")
PREBOND_BATCH_SIZE = int(os.getenv("PREBOND_BATCH_SIZE", "10"))
PREBOND_TRACK_HOURS = float(os.getenv("PREBOND_TRACK_HOURS", "8"))
POSTBOND_TRACK_HOURS = float(os.getenv("POSTBOND_TRACK_HOURS", "24"))
PREBOND_LORE_TRIGGER_PCT = float(os.getenv("PREBOND_LORE_TRIGGER_PCT", "70"))
PREBOND_PRIORITY_PCT = float(os.getenv("PREBOND_PRIORITY_PCT", "80"))
PREBOND_MAX_PRIORITY = int(os.getenv("PREBOND_MAX_PRIORITY", "80"))

# Pump standard create_v2 reserves. Pump's current SDK uses these exact values
# for the lightweight graduation-progress helper.
INITIAL_VIRTUAL_TOKEN_RESERVES = 1_073_000_000_000_000
INITIAL_REAL_TOKEN_RESERVES = 793_100_000_000_000

GENERIC_TERMS = {
    "coin", "token", "meme", "memecoin", "official", "new", "pump", "fun",
    "doge", "pepe", "inu", "shib", "bonk", "wif", "elon", "trump", "baby",
    "moon", "ai", "cat", "dog", "frog",
}
STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with",
    "is", "it", "this", "that", "from", "by", "at", "as", "be", "are", "was",
    "we", "our", "you", "your", "its", "their", "has", "have", "will", "not",
    "coin", "token", "meme", "memecoin",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS prebond_tokens (
    mint TEXT PRIMARY KEY,
    name TEXT,
    symbol TEXT,
    description TEXT,
    creator TEXT,
    metadata_uri TEXT,
    image_uri TEXT,
    website TEXT,
    twitter TEXT,
    telegram TEXT,
    created_at INTEGER,
    complete INTEGER NOT NULL DEFAULT 0,
    progress_pct REAL NOT NULL DEFAULT 0,
    market_cap_usd REAL NOT NULL DEFAULT 0,
    reply_count INTEGER NOT NULL DEFAULT 0,
    lore_strength REAL NOT NULL DEFAULT 0,
    uniqueness_score REAL NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    creator_seen_count INTEGER NOT NULL DEFAULT 0,
    score_delta REAL NOT NULL DEFAULT 0,
    analysis_json TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_checked_at TEXT,
    bonded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_prebond_progress ON prebond_tokens(progress_pct DESC);
CREATE INDEX IF NOT EXISTS idx_prebond_created ON prebond_tokens(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_prebond_creator ON prebond_tokens(creator);
"""


def _utcnow():
    return datetime.now(timezone.utc)


def _norm(s):
    s = (s or "").lower().strip()
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _words(s):
    return [w for w in _norm(s).split() if len(w) >= 3 and w not in STOPWORDS]


def _jaccard(a, b):
    a, b = set(_words(a)), set(_words(b))
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


class LoreMonitor:
    """Forward-only pre-bond and lore monitor for Pump.fun launches.

    It does not execute trades. It records close-to-graduation signals and returns
    a bounded score delta that the main SFalcon engine can use after normal risk
    gates. This avoids turning novelty alone into an automatic buy trigger.
    """

    def __init__(self, client, db_path, jupiter_key=None):
        self.client = client
        self.db_path = db_path
        self.jupiter_key = jupiter_key
        self._queue = deque()
        self._queued = set()
        self.last_stream_at = None
        self.last_scan_at = None
        self.last_error = None
        self.stream_source = None
        self.new_tokens_seen = 0
        self.tokens_analyzed = 0
        self._pump_lock = asyncio.Lock()
        self._last_pump = 0.0
        self._jup_lock = asyncio.Lock()
        self._last_jup = 0.0
        self._dex_lock = asyncio.Lock()
        self._last_dex = 0.0

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA)
            await db.commit()
        await self._rebuild_queue()

    async def _rebuild_queue(self):
        cutoff = int((_utcnow() - timedelta(hours=PREBOND_TRACK_HOURS)).timestamp())
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """SELECT mint FROM prebond_tokens
                   WHERE complete=0 AND COALESCE(created_at,0)>=?
                   ORDER BY progress_pct DESC, created_at DESC LIMIT 2000""",
                (cutoff,),
            )
            rows = [r[0] for r in await cur.fetchall()]
        for mint in rows:
            self._enqueue(mint)

    def _enqueue(self, mint, front=False):
        if not mint or mint in self._queued:
            return
        self._queued.add(mint)
        if front:
            self._queue.appendleft(mint)
        else:
            self._queue.append(mint)

    async def _upsert_stream_token(self, row):
        mint = row.get("mint") or row.get("token") or row.get("address")
        if not mint:
            return
        name = row.get("name") or ""
        symbol = row.get("symbol") or ""
        created = int(row.get("created_timestamp") or row.get("timestamp") or time.time())
        if created > 10_000_000_000:
            created //= 1000
        now = _utcnow().isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO prebond_tokens(
                       mint,name,symbol,created_at,first_seen_at,last_seen_at
                   ) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(mint) DO UPDATE SET
                       name=CASE WHEN excluded.name!='' THEN excluded.name ELSE prebond_tokens.name END,
                       symbol=CASE WHEN excluded.symbol!='' THEN excluded.symbol ELSE prebond_tokens.symbol END,
                       last_seen_at=excluded.last_seen_at""",
                (mint, name, symbol, created, now, now),
            )
            await db.commit()
        self.new_tokens_seen += 1
        self.last_stream_at = now
        self._enqueue(mint, front=True)

    async def _mark_migrated(self, mint):
        if not mint:
            return
        now = _utcnow().isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE prebond_tokens SET complete=1,progress_pct=100,bonded_at=COALESCE(bonded_at,?),last_seen_at=? WHERE mint=?",
                (now, now, mint),
            )
            await db.commit()

    async def stream_new_tokens(self):
        """Listen to free create/migration streams; reconnect forever.

        PumpPortal announced that new-token and migration streams remain free.
        A second public stream is used as a fallback if the primary rejects or
        disconnects. No paid token-trade subscription is used.
        """
        urls = [PUMP_STREAM_URL, PUMP_STREAM_FALLBACK_URL]
        idx = 0
        while True:
            url = urls[idx % len(urls)]
            idx += 1
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=2_000_000,
                ) as ws:
                    self.stream_source = url.split("?")[0]
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    try:
                        await ws.send(json.dumps({"method": "subscribeMigration"}))
                    except Exception:
                        pass
                    async for raw in ws:
                        try:
                            row = json.loads(raw)
                        except Exception:
                            continue
                        tx_type = str(row.get("txType") or row.get("type") or row.get("event") or "").lower()
                        mint = row.get("mint") or row.get("token") or row.get("address")
                        if tx_type in {"create", "new", "newtoken", "token_create"} or (
                            mint and (row.get("bondingCurveKey") or row.get("name")) and "trade" not in tx_type
                        ):
                            await self._upsert_stream_token(row)
                        if "migrat" in tx_type or tx_type in {"complete", "graduated", "graduate"}:
                            await self._mark_migrated(mint)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"stream {type(e).__name__}: {str(e)[:140]}"
                await asyncio.sleep(5)

    async def _pump_get(self, mint):
        async with self._pump_lock:
            wait = 0.25 - (time.monotonic() - self._last_pump)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_pump = time.monotonic()
            r = await self.client.get(f"{PUMP_COIN_BASE}/coins-v2/{mint}")
            if r.status_code == 429:
                retry = float(r.headers.get("retry-after", "2") or 2)
                await asyncio.sleep(min(max(retry, 1), 10))
                raise RuntimeError("pump coin API 429")
            r.raise_for_status()
            return r.json() or {}

    async def _jupiter_search(self, query):
        if not self.jupiter_key or not query:
            return []
        async with self._jup_lock:
            wait = 1.05 - (time.monotonic() - self._last_jup)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_jup = time.monotonic()
            try:
                r = await self.client.get(
                    "https://api.jup.ag/tokens/v2/search",
                    params={"query": query[:80]},
                    headers={"x-api-key": self.jupiter_key},
                )
                r.raise_for_status()
                rows = r.json() or []
                return rows if isinstance(rows, list) else []
            except Exception:
                return []

    async def _dex_search(self, query):
        if not query:
            return []
        async with self._dex_lock:
            wait = 0.65 - (time.monotonic() - self._last_dex)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_dex = time.monotonic()
            try:
                r = await self.client.get(
                    "https://api.dexscreener.com/latest/dex/search",
                    params={"q": query[:80]},
                )
                r.raise_for_status()
                return (r.json() or {}).get("pairs") or []
            except Exception:
                return []

    @staticmethod
    def _progress(row):
        if row.get("complete") is True:
            return 100.0
        raw = row.get("virtual_token_reserves") or row.get("virtualTokenReserves")
        try:
            vtokens = int(raw)
        except Exception:
            return 0.0
        sold = max(0, INITIAL_VIRTUAL_TOKEN_RESERVES - vtokens)
        return round(100 * _clamp(sold / INITIAL_REAL_TOKEN_RESERVES), 3)

    async def _duplicate_evidence(self, mint, name, symbol, description, creator):
        nname, nsymbol = _norm(name), _norm(symbol)
        local_exact = 0
        creator_seen = 0
        max_similarity = 0.0
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT mint,name,symbol,description FROM prebond_tokens WHERE mint<>? ORDER BY created_at DESC LIMIT 300",
                (mint,),
            )
            for row in await cur.fetchall():
                other_name, other_symbol = _norm(row["name"]), _norm(row["symbol"])
                if (nname and nname == other_name) or (nsymbol and nsymbol == other_symbol):
                    local_exact += 1
                text_a = f"{name} {description or ''}"
                text_b = f"{row['name'] or ''} {row['description'] or ''}"
                sim = max(_jaccard(text_a, text_b), SequenceMatcher(None, _norm(text_a), _norm(text_b)).ratio() * 0.55)
                max_similarity = max(max_similarity, sim)
            if creator:
                cur = await db.execute(
                    "SELECT COUNT(*) FROM prebond_tokens WHERE creator=? AND mint<>?",
                    (creator, mint),
                )
                creator_seen = int((await cur.fetchone())[0])

        # External matches cover tokens that predate this bot. We only count
        # exact-ish name/symbol matches and exclude the mint being analyzed.
        external = 0
        jup = await self._jupiter_search(name or symbol)
        for row in jup:
            rid = row.get("id")
            if rid == mint:
                continue
            if (nname and _norm(row.get("name")) == nname) or (nsymbol and _norm(row.get("symbol")) == nsymbol):
                external += 1

        dex = await self._dex_search(name or symbol)
        seen_mints = set()
        for pair in dex:
            if str(pair.get("chainId", "")).lower() != "solana":
                continue
            base = pair.get("baseToken") or {}
            rid = base.get("address")
            if not rid or rid == mint or rid in seen_mints:
                continue
            if (nname and _norm(base.get("name")) == nname) or (nsymbol and _norm(base.get("symbol")) == nsymbol):
                seen_mints.add(rid)
        external += len(seen_mints)
        return {
            "local_exact": local_exact,
            "external_exact": external,
            "creator_seen": creator_seen,
            "max_lore_similarity": round(max_similarity, 3),
        }

    async def _analyze(self, mint, row, progress):
        name = row.get("name") or ""
        symbol = row.get("symbol") or ""
        desc = row.get("description") or ""
        creator = row.get("creator") or ""
        website = row.get("website") or ""
        twitter = row.get("twitter") or ""
        telegram = row.get("telegram") or ""
        image = row.get("image_uri") or row.get("imageUri") or ""
        reply_count = int(row.get("reply_count") or row.get("replyCount") or 0)

        dup = await self._duplicate_evidence(mint, name, symbol, desc, creator)
        exact = dup["local_exact"] + dup["external_exact"]
        similarity = dup["max_lore_similarity"]

        uniqueness = 100.0
        uniqueness -= min(65.0, exact * 22.0)
        uniqueness -= 30.0 * _clamp((similarity - 0.35) / 0.55)
        if dup["creator_seen"] >= 8:
            uniqueness -= 10
        elif dup["creator_seen"] >= 4:
            uniqueness -= 5
        uniqueness = max(0.0, min(100.0, uniqueness))

        words = _words(desc)
        unique_words = len(set(words))
        desc_len = len(desc.strip())
        socials = sum(bool(x) for x in (website, twitter, telegram))
        generic_hits = sum(1 for w in _words(f"{name} {symbol}") if w in GENERIC_TERMS)

        depth = 26 * _clamp(desc_len / 220) + 16 * _clamp(unique_words / 35)
        identity = 10.0
        if not name or len(_norm(name)) < 3:
            identity -= 6
        if not symbol or not (2 <= len(symbol) <= 12):
            identity -= 4
        identity -= min(5.0, generic_hits * 1.5)
        metadata = (5 if image else 0) + min(9, socials * 3) + (4 if desc_len >= 50 else 0)
        community = 10 * _clamp(math.log1p(reply_count) / math.log(35)) if reply_count > 0 else 0
        narrative_originality = 18 * (uniqueness / 100)
        lore = max(0.0, min(100.0, depth + identity + metadata + community + narrative_originality))

        # Bond progress controls how much of the lore score may affect the actual
        # trade score. Novelty alone never grants a full-strength signal.
        if progress < PREBOND_LORE_TRIGGER_PCT:
            cap = 0.0
        elif progress < 85:
            cap = 4.0
        elif progress < 95:
            cap = 8.0
        else:
            cap = 12.0

        quality = 0.62 * (lore / 100) + 0.38 * (uniqueness / 100)
        delta = cap * quality
        reasons = []
        if exact == 0:
            reasons.append("no exact prior name/symbol match found in monitored/Jupiter/DEX history")
            delta += 1.0 if progress >= 85 else 0.0
        else:
            reasons.append(f"{exact} prior exact-ish name/symbol match(es) found")
        if similarity >= 0.72:
            reasons.append("lore text strongly resembles an earlier monitored launch")
            delta -= 3.0
        if dup["creator_seen"] >= 8:
            reasons.append("creator has many launches in the monitored history")
            delta -= 2.0
        if lore >= 72:
            reasons.append("strong metadata/narrative depth")
        elif lore < 38:
            reasons.append("thin or generic lore")
            delta -= 2.0
        if progress >= 95:
            reasons.append("very close to graduation")
        elif progress >= 85:
            reasons.append("close to graduation")

        # Duplicate/copy risk can turn the lore component negative, but never
        # more than -8 or +12 in the master trade score.
        if uniqueness < 30:
            delta = min(delta, -2.0)
        delta = round(max(-8.0, min(12.0, delta)), 2)
        analysis = {
            "progress_pct": round(progress, 3),
            "lore_strength": round(lore, 2),
            "uniqueness_score": round(uniqueness, 2),
            "score_delta": delta,
            "prior_exact_matches": exact,
            "local_exact_matches": dup["local_exact"],
            "external_exact_matches": dup["external_exact"],
            "max_lore_similarity": similarity,
            "creator_seen_count": dup["creator_seen"],
            "reply_count": reply_count,
            "metadata": {
                "description_chars": desc_len,
                "unique_lore_words": unique_words,
                "image": bool(image),
                "website": bool(website),
                "twitter": bool(twitter),
                "telegram": bool(telegram),
            },
            "reasons": reasons,
            "uniqueness_scope": "SFalcon observed launches + Jupiter token search + Solana DEX search; not a proof of first-ever existence",
        }
        return analysis

    async def _refresh_one(self, mint):
        try:
            row = await self._pump_get(mint)
            if not isinstance(row, dict) or not row:
                return
            progress = self._progress(row)
            complete = bool(row.get("complete"))
            now = _utcnow().isoformat()
            created = int(row.get("created_timestamp") or row.get("createdAt") or time.time())
            if created > 10_000_000_000:
                created //= 1000
            market_cap = float(row.get("usd_market_cap") or row.get("market_cap_usd") or 0)
            reply_count = int(row.get("reply_count") or row.get("replyCount") or 0)

            analysis = None
            if progress >= PREBOND_LORE_TRIGGER_PCT and not complete:
                analysis = await self._analyze(mint, row, progress)
                self.tokens_analyzed += 1

            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """INSERT INTO prebond_tokens(
                           mint,name,symbol,description,creator,metadata_uri,image_uri,
                           website,twitter,telegram,created_at,complete,progress_pct,
                           market_cap_usd,reply_count,lore_strength,uniqueness_score,
                           duplicate_count,creator_seen_count,score_delta,analysis_json,
                           first_seen_at,last_seen_at,last_checked_at,bonded_at
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(mint) DO UPDATE SET
                           name=excluded.name,symbol=excluded.symbol,description=excluded.description,
                           creator=excluded.creator,metadata_uri=excluded.metadata_uri,image_uri=excluded.image_uri,
                           website=excluded.website,twitter=excluded.twitter,telegram=excluded.telegram,
                           created_at=excluded.created_at,complete=excluded.complete,
                           progress_pct=excluded.progress_pct,market_cap_usd=excluded.market_cap_usd,
                           reply_count=excluded.reply_count,
                           lore_strength=CASE WHEN excluded.analysis_json IS NOT NULL THEN excluded.lore_strength ELSE prebond_tokens.lore_strength END,
                           uniqueness_score=CASE WHEN excluded.analysis_json IS NOT NULL THEN excluded.uniqueness_score ELSE prebond_tokens.uniqueness_score END,
                           duplicate_count=CASE WHEN excluded.analysis_json IS NOT NULL THEN excluded.duplicate_count ELSE prebond_tokens.duplicate_count END,
                           creator_seen_count=CASE WHEN excluded.analysis_json IS NOT NULL THEN excluded.creator_seen_count ELSE prebond_tokens.creator_seen_count END,
                           score_delta=CASE WHEN excluded.analysis_json IS NOT NULL THEN excluded.score_delta ELSE prebond_tokens.score_delta END,
                           analysis_json=COALESCE(excluded.analysis_json,prebond_tokens.analysis_json),
                           last_seen_at=excluded.last_seen_at,last_checked_at=excluded.last_checked_at,
                           bonded_at=CASE WHEN excluded.complete=1 THEN COALESCE(prebond_tokens.bonded_at,excluded.bonded_at) ELSE prebond_tokens.bonded_at END""",
                    (
                        mint,
                        row.get("name") or "",
                        row.get("symbol") or "",
                        row.get("description") or "",
                        row.get("creator") or "",
                        row.get("metadata_uri") or "",
                        row.get("image_uri") or "",
                        row.get("website") or "",
                        row.get("twitter") or "",
                        row.get("telegram") or "",
                        created,
                        1 if complete else 0,
                        progress,
                        market_cap,
                        reply_count,
                        float((analysis or {}).get("lore_strength") or 0),
                        float((analysis or {}).get("uniqueness_score") or 0),
                        int((analysis or {}).get("prior_exact_matches") or 0),
                        int((analysis or {}).get("creator_seen_count") or 0),
                        float((analysis or {}).get("score_delta") or 0),
                        json.dumps(analysis) if analysis else None,
                        now,
                        now,
                        now,
                        now if complete else None,
                    ),
                )
                await db.commit()

            if not complete:
                # Priority launches are revisited sooner. Others go to the back.
                self._enqueue(mint, front=progress >= PREBOND_PRIORITY_PCT)
        except Exception as e:
            self.last_error = f"scan {type(e).__name__}: {str(e)[:150]}"

    async def scan_once(self):
        if not self._queue:
            await self._rebuild_queue()
        batch = []
        while self._queue and len(batch) < PREBOND_BATCH_SIZE:
            mint = self._queue.popleft()
            self._queued.discard(mint)
            batch.append(mint)
        for mint in batch:
            await self._refresh_one(mint)
        self.last_scan_at = _utcnow().isoformat()

    async def signal_for_mint(self, mint, max_age_hours=None):
        max_age_hours = max_age_hours or max(PREBOND_TRACK_HOURS, POSTBOND_TRACK_HOURS)
        cutoff = (_utcnow() - timedelta(hours=max_age_hours)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT progress_pct,lore_strength,uniqueness_score,score_delta,
                          duplicate_count,analysis_json,last_checked_at,bonded_at,complete,last_seen_at
                   FROM prebond_tokens WHERE mint=? AND last_seen_at>=?""",
                (mint, cutoff),
            )
            row = await cur.fetchone()
        if not row:
            return {"score_delta": 0.0, "available": False, "reason": "no pre/post-bond lore record"}
        data = dict(row)
        try:
            analysis = json.loads(data.pop("analysis_json") or "{}")
        except Exception:
            analysis = {}
        data["available"] = True
        data["analysis"] = analysis
        return data

    async def priority_mints(self, limit=60):
        pre_cutoff = (_utcnow() - timedelta(hours=PREBOND_TRACK_HOURS)).isoformat()
        post_cutoff = (_utcnow() - timedelta(hours=POSTBOND_TRACK_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """SELECT mint FROM prebond_tokens
                   WHERE (complete=0 AND last_seen_at>=? AND progress_pct>=?)
                      OR (bonded_at IS NOT NULL AND bonded_at>=?)
                   ORDER BY CASE WHEN bonded_at IS NOT NULL THEN 0 ELSE 1 END,
                            COALESCE(bonded_at,last_seen_at) DESC, progress_pct DESC LIMIT ?""",
                (pre_cutoff, PREBOND_PRIORITY_PCT, post_cutoff, min(limit, PREBOND_MAX_PRIORITY)),
            )
            return [r[0] for r in await cur.fetchall()]

    async def postbond_mints(self, limit=50):
        cutoff = (_utcnow() - timedelta(hours=POSTBOND_TRACK_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT mint FROM prebond_tokens WHERE bonded_at IS NOT NULL AND bonded_at>=? ORDER BY bonded_at DESC LIMIT ?",
                (cutoff, limit),
            )
            return [r[0] for r in await cur.fetchall()]

    async def top_prebond(self, limit=50):
        cutoff = (_utcnow() - timedelta(hours=PREBOND_TRACK_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT mint,name,symbol,progress_pct,market_cap_usd,reply_count,
                          lore_strength,uniqueness_score,duplicate_count,score_delta,
                          creator_seen_count,complete,analysis_json,last_checked_at,bonded_at
                   FROM prebond_tokens
                   WHERE last_seen_at>=? AND complete=0
                   ORDER BY progress_pct DESC,lore_strength DESC LIMIT ?""",
                (cutoff, limit),
            )
            rows = []
            for r in await cur.fetchall():
                d = dict(r)
                try: d["analysis"] = json.loads(d.pop("analysis_json") or "{}")
                except Exception: d["analysis"] = {}
                rows.append(d)
            return rows

    async def top_postbond(self, limit=50):
        cutoff = (_utcnow() - timedelta(hours=POSTBOND_TRACK_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT mint,name,symbol,progress_pct,market_cap_usd,reply_count,
                          lore_strength,uniqueness_score,duplicate_count,score_delta,
                          creator_seen_count,complete,analysis_json,last_checked_at,bonded_at
                   FROM prebond_tokens WHERE bonded_at IS NOT NULL AND bonded_at>=?
                   ORDER BY bonded_at DESC,lore_strength DESC LIMIT ?""",
                (cutoff, limit),
            )
            rows=[]
            for r in await cur.fetchall():
                d=dict(r)
                try: d["analysis"] = json.loads(d.pop("analysis_json") or "{}")
                except Exception: d["analysis"]={}
                rows.append(d)
            return rows

    async def status(self):
        cutoff = (_utcnow() - timedelta(hours=PREBOND_TRACK_HOURS)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM prebond_tokens")
            total = int((await cur.fetchone())[0])
            cur = await db.execute(
                "SELECT COUNT(*) FROM prebond_tokens WHERE last_seen_at>=? AND complete=0",
                (cutoff,),
            )
            active = int((await cur.fetchone())[0])
            cur = await db.execute(
                "SELECT COUNT(*) FROM prebond_tokens WHERE last_seen_at>=? AND progress_pct>=? AND complete=0",
                (cutoff, PREBOND_LORE_TRIGGER_PCT),
            )
            near = int((await cur.fetchone())[0])
            cur = await db.execute(
                "SELECT COUNT(*) FROM prebond_tokens WHERE analysis_json IS NOT NULL",
            )
            analyzed = int((await cur.fetchone())[0])
            post_cutoff = (_utcnow() - timedelta(hours=POSTBOND_TRACK_HOURS)).isoformat()
            cur = await db.execute(
                "SELECT COUNT(*) FROM prebond_tokens WHERE bonded_at IS NOT NULL AND bonded_at>=?",
                (post_cutoff,),
            )
            postbond = int((await cur.fetchone())[0])
        return {
            "enabled": True,
            "stream_source": self.stream_source,
            "new_tokens_seen_runtime": self.new_tokens_seen,
            "tracked_total": total,
            "active_recent": active,
            "near_bond_recent": near,
            "analyzed_total": analyzed,
            "postbond_recent": postbond,
            "postbond_track_hours": POSTBOND_TRACK_HOURS,
            "lore_trigger_pct": PREBOND_LORE_TRIGGER_PCT,
            "priority_pct": PREBOND_PRIORITY_PCT,
            "last_stream_at": self.last_stream_at,
            "last_scan_at": self.last_scan_at,
            "last_error": self.last_error,
            "queue_depth": len(self._queue),
            "progress_method": "Pump standard create_v2 reserve estimate; complete flag overrides to 100%",
        }
