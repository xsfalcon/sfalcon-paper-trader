import asyncio
import html
import json
import os
import random
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import aiosqlite

FX_BASE = os.getenv("FXTWITTER_BASE", "https://api.fxtwitter.com")
SYND_BASE = os.getenv("X_SYNDICATION_BASE", "https://syndication.twitter.com/srv/timeline-profile/screen-name")
SOCIAL_PROFILE_COUNT = int(os.getenv("SOCIAL_PROFILE_COUNT", "20"))
SOCIAL_SIGNAL_WINDOW_MIN = int(os.getenv("SOCIAL_SIGNAL_WINDOW_MIN", "120"))
SOCIAL_SEARCH_COOLDOWN_MIN = int(os.getenv("SOCIAL_SEARCH_COOLDOWN_MIN", "20"))
SOCIAL_GLOBAL_429_COOLDOWN = int(os.getenv("SOCIAL_GLOBAL_429_COOLDOWN_SECONDS", "300"))

BASE58_RE = re.compile(r"(?<![1-9A-HJ-NP-Za-km-z])([1-9A-HJ-NP-Za-km-z]{32,44})(?![1-9A-HJ-NP-Za-km-z])")
TICKER_RE = re.compile(r"\$([A-Za-z][A-Za-z0-9_]{1,14})\b")
PUMP_RE = re.compile(r"pump\.fun/(?:coin/)?([1-9A-HJ-NP-Za-km-z]{32,44})", re.I)

POSITIVE = {
    "ape", "aping", "aped", "bid", "bidding", "bullish", "send", "sending",
    "runner", "running", "cook", "cooking", "gem", "moon", "breakout", "accumulate",
    "accumulating", "buy", "bought", "long", "conviction", "undervalued", "early",
}
NEGATIVE = {
    "rug", "rugged", "scam", "avoid", "dump", "dumping", "sell", "sold", "exit",
    "exploit", "honeypot", "bundled", "bundle", "insider", "warning", "fake", "drain",
    "malicious", "sniper", "cabals", "cabal",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS social_posts (
  post_id TEXT PRIMARY KEY,
  handle TEXT NOT NULL,
  account_name TEXT,
  category TEXT,
  account_weight REAL NOT NULL DEFAULT 0.35,
  wallet_address TEXT,
  text TEXT NOT NULL,
  created_at TEXT,
  likes INTEGER NOT NULL DEFAULT 0,
  reposts INTEGER NOT NULL DEFAULT 0,
  replies INTEGER NOT NULL DEFAULT 0,
  views INTEGER NOT NULL DEFAULT 0,
  extracted_mints TEXT NOT NULL DEFAULT '[]',
  tickers TEXT NOT NULL DEFAULT '[]',
  stance REAL NOT NULL DEFAULT 0,
  source TEXT NOT NULL,
  observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_social_posts_created ON social_posts(created_at);
CREATE INDEX IF NOT EXISTS idx_social_posts_handle ON social_posts(handle);
CREATE TABLE IF NOT EXISTS social_account_state (
  handle TEXT PRIMARY KEY,
  last_checked_at TEXT,
  last_post_id TEXT,
  error_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS social_candidate_search (
  mint TEXT PRIMARY KEY,
  symbol TEXT,
  last_searched_at TEXT,
  query_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
"""


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat()
    s = str(dt)
    if s.isdigit():
        try:
            n = int(s)
            if n > 10**12:
                n /= 1000
            return datetime.fromtimestamp(n, tz=timezone.utc).isoformat()
        except Exception:
            return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        # Twitter legacy date format
        try:
            return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).isoformat()
        except Exception:
            return None


def _metric(d, *keys):
    for key in keys:
        v = d.get(key) if isinstance(d, dict) else None
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return 0


def _walk_tweets(obj):
    """Yield tweet-like dicts from FxTwitter/syndication response shapes."""
    seen_obj = set()
    stack = [obj]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            oid = id(x)
            if oid in seen_obj:
                continue
            seen_obj.add(oid)
            text = x.get("text") or x.get("full_text") or x.get("raw_text")
            post_id = x.get("id") or x.get("id_str") or x.get("tweet_id") or x.get("rest_id")
            if text and post_id:
                yield x
            for v in x.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(x, list):
            stack.extend(x)


def _author_handle(row, fallback=""):
    for key in ("author", "user", "creator"):
        a = row.get(key)
        if isinstance(a, dict):
            h = a.get("screen_name") or a.get("username") or a.get("handle") or a.get("name")
            if h:
                return str(h).lstrip("@")
    return str(row.get("screen_name") or row.get("username") or fallback).lstrip("@")


def _post_from_row(row, fallback_handle=""):
    text = str(row.get("text") or row.get("full_text") or row.get("raw_text") or "").strip()
    if not text:
        return None
    post_id = str(row.get("id") or row.get("id_str") or row.get("tweet_id") or row.get("rest_id") or "")
    if not post_id:
        return None
    metrics = row.get("public_metrics") or row.get("metrics") or row
    created = row.get("created_at") or row.get("createdAt") or row.get("date") or row.get("timestamp")
    return {
        "post_id": post_id,
        "handle": _author_handle(row, fallback_handle),
        "text": html.unescape(text),
        "created_at": _iso(created),
        "likes": _metric(metrics, "like_count", "likes", "favorite_count", "favoriteCount"),
        "reposts": _metric(metrics, "retweet_count", "retweets", "reposts", "retweetCount"),
        "replies": _metric(metrics, "reply_count", "replies", "replyCount"),
        "views": _metric(metrics, "view_count", "views", "viewCount"),
    }


def _extract(text):
    mints = set(PUMP_RE.findall(text))
    mints.update(BASE58_RE.findall(text))
    tickers = {x.upper() for x in TICKER_RE.findall(text)}
    return sorted(mints), sorted(tickers)


def _stance(text):
    words = re.findall(r"[A-Za-z]+", text.lower())
    pos = sum(1 for w in words if w in POSITIVE)
    neg = sum(1 for w in words if w in NEGATIVE)
    if not pos and not neg:
        return 0.0
    return max(-1.0, min(1.0, (pos - neg) / max(1, pos + neg)))


class SocialMonitor:
    def __init__(self, client, db_path, wallets, watchlist_path="social_watchlist.json"):
        self.client = client
        self.db_path = db_path
        self.wallets = wallets
        self.watchlist_path = watchlist_path
        self.accounts = self._load_watchlist()
        self.by_handle = {a["handle"].lower(): a for a in self.accounts}
        self._queue = deque(self._schedule())
        self.last_cycle_at = None
        self.last_error = None
        self.last_success_at = None
        self.global_cooldown_until = 0.0
        self.source_ok = None
        self.posts_seen = 0

    def _load_watchlist(self):
        try:
            rows = json.load(open(self.watchlist_path, "r", encoding="utf-8"))
        except Exception as e:
            print("[SOCIAL] watchlist load error:", e)
            return []
        out, seen = [], set()
        for r in rows:
            h = str(r.get("handle") or "").lstrip("@").strip()
            if not h or h.lower() in seen:
                continue
            seen.add(h.lower())
            out.append({
                "handle": h,
                "name": r.get("name") or h,
                "category": r.get("category") or "attention",
                "weight": float(r.get("weight") or 1.0),
                "wallet": r.get("wallet"),
                "tier": r.get("tier") or "B",
                "note": r.get("note") or "",
            })
        return out

    def _schedule(self):
        arr = []
        for a in self.accounts:
            repeats = 3 if a["tier"] == "A" else 2 if a["tier"] == "B" else 1
            arr.extend([a["handle"]] * repeats)
        random.Random(404).shuffle(arr)
        return arr

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def _state(self, handle):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT last_post_id FROM social_account_state WHERE handle=?", (handle.lower(),))
            row = await cur.fetchone()
            return row[0] if row else None

    async def _save_state(self, handle, last_post_id=None, error=None):
        now = _now().isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO social_account_state(handle,last_checked_at,last_post_id,error_count,last_error)
                   VALUES(?,?,?,?,?) ON CONFLICT(handle) DO UPDATE SET
                   last_checked_at=excluded.last_checked_at,
                   last_post_id=COALESCE(excluded.last_post_id,social_account_state.last_post_id),
                   error_count=CASE WHEN excluded.last_error IS NULL THEN social_account_state.error_count ELSE social_account_state.error_count+1 END,
                   last_error=excluded.last_error""",
                (handle.lower(), now, last_post_id, 1 if error else 0, error),
            )
            await db.commit()

    async def _fetch_fx(self, handle):
        url = f"{FX_BASE}/2/profile/{handle}/statuses"
        r = await self.client.get(url, params={"count": SOCIAL_PROFILE_COUNT})
        if r.status_code == 429:
            self.global_cooldown_until = time.monotonic() + SOCIAL_GLOBAL_429_COOLDOWN
            raise RuntimeError("FxTwitter 429")
        r.raise_for_status()
        data = r.json()
        posts = []
        seen = set()
        for row in _walk_tweets(data):
            p = _post_from_row(row, handle)
            if p and p["post_id"] not in seen:
                seen.add(p["post_id"]); posts.append(p)
        return posts

    async def _fetch_syndication(self, handle):
        r = await self.client.get(f"{SYND_BASE}/{handle}")
        if r.status_code == 429:
            self.global_cooldown_until = time.monotonic() + SOCIAL_GLOBAL_429_COOLDOWN
            raise RuntimeError("X syndication 429")
        r.raise_for_status()
        text = r.text
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, re.S | re.I)
        if not m:
            return []
        try:
            data = json.loads(html.unescape(m.group(1)))
        except Exception:
            return []
        posts, seen = [], set()
        for row in _walk_tweets(data):
            p = _post_from_row(row, handle)
            if p and p["post_id"] not in seen:
                seen.add(p["post_id"]); posts.append(p)
        return posts

    async def _store_post(self, p, source, account=None):
        handle = (p.get("handle") or (account or {}).get("handle") or "unknown").lstrip("@")
        account = account or self.by_handle.get(handle.lower()) or {}
        mints, tickers = _extract(p["text"])
        stance = _stance(p["text"])
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR IGNORE INTO social_posts(
                  post_id,handle,account_name,category,account_weight,wallet_address,text,created_at,
                  likes,reposts,replies,views,extracted_mints,tickers,stance,source,observed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    p["post_id"], handle, account.get("name") or handle, account.get("category") or "search",
                    float(account.get("weight") or 0.35), account.get("wallet"), p["text"], p.get("created_at"),
                    p.get("likes",0), p.get("reposts",0), p.get("replies",0), p.get("views",0),
                    json.dumps(mints), json.dumps(tickers), stance, source, _now().isoformat(),
                ),
            )
            changed = db.total_changes
            await db.commit()
        if changed:
            self.posts_seen += 1
        return bool(changed)

    async def poll_account(self, handle):
        if time.monotonic() < self.global_cooldown_until:
            return
        account = self.by_handle.get(handle.lower())
        if not account:
            return
        last_id = await self._state(handle)
        posts = []
        source = "fxtwitter"
        try:
            posts = await self._fetch_fx(handle)
            self.source_ok = True
        except Exception as e1:
            try:
                source = "x_syndication"
                posts = await self._fetch_syndication(handle)
                self.source_ok = True
            except Exception as e2:
                self.source_ok = False
                self.last_error = f"{type(e2).__name__}: {str(e2)[:120]}"
                await self._save_state(handle, error=self.last_error)
                return
        newest = posts[0]["post_id"] if posts else last_id
        # First observation creates a baseline; store recent posts because they are useful for candidate research,
        # but time-windowing prevents stale posts from becoming current signals.
        for p in reversed(posts[:SOCIAL_PROFILE_COUNT]):
            # Ignore nested quoted/replied statuses belonging to another author when walking the payload.
            if p.get("handle") and p["handle"].lower() != handle.lower():
                continue
            await self._store_post(p, source, account)
        await self._save_state(handle, newest, error=None)
        self.last_success_at = _now().isoformat()

    async def scan_once(self):
        if time.monotonic() < self.global_cooldown_until:
            return
        if not self._queue:
            self._queue = deque(self._schedule())
        if not self._queue:
            return
        handle = self._queue.popleft()
        await self.poll_account(handle)
        self.last_cycle_at = _now().isoformat()

    async def _search_due(self, mint):
        cutoff = (_now() - timedelta(minutes=SOCIAL_SEARCH_COOLDOWN_MIN)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT last_searched_at FROM social_candidate_search WHERE mint=?", (mint,))
            row = await cur.fetchone()
            return not row or not row[0] or row[0] < cutoff

    async def research_candidate(self, mint, symbol=None, name=None):
        if not mint or time.monotonic() < self.global_cooldown_until or not await self._search_due(mint):
            return 0
        queries = [mint]
        if symbol and symbol not in {"?", "UNKNOWN"}:
            queries.append(f"${symbol}")
        stored = 0
        err = None
        try:
            for query in queries[:2]:
                r = await self.client.get(f"{FX_BASE}/2/search", params={"q": query, "feed": "latest", "count": 20})
                if r.status_code == 429:
                    self.global_cooldown_until = time.monotonic() + SOCIAL_GLOBAL_429_COOLDOWN
                    raise RuntimeError("FxTwitter search 429")
                r.raise_for_status()
                data = r.json()
                seen = set()
                for row in _walk_tweets(data):
                    p = _post_from_row(row)
                    if not p or p["post_id"] in seen:
                        continue
                    seen.add(p["post_id"])
                    account = self.by_handle.get((p.get("handle") or "").lower())
                    if await self._store_post(p, "fxtwitter_search", account):
                        stored += 1
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:140]}"
            self.last_error = err
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO social_candidate_search(mint,symbol,last_searched_at,query_count,last_error)
                   VALUES(?,?,?,?,?) ON CONFLICT(mint) DO UPDATE SET
                   symbol=excluded.symbol,last_searched_at=excluded.last_searched_at,
                   query_count=social_candidate_search.query_count+excluded.query_count,last_error=excluded.last_error""",
                (mint, symbol, _now().isoformat(), len(queries[:2]), err),
            )
            await db.commit()
        return stored

    async def signal_for_candidate(self, mint, symbol=None, name=None, wallet_signal=None, price_change_m5=0.0):
        cutoff = (_now() - timedelta(minutes=SOCIAL_SIGNAL_WINDOW_MIN)).isoformat()
        sym = (symbol or "").upper().lstrip("$")
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT * FROM social_posts WHERE COALESCE(created_at,observed_at)>=? ORDER BY COALESCE(created_at,observed_at) DESC LIMIT 1000""",
                (cutoff,),
            )
            rows = [dict(r) for r in await cur.fetchall()]

        matches = []
        for r in rows:
            try: mints = set(json.loads(r.get("extracted_mints") or "[]"))
            except Exception: mints = set()
            try: tickers = set(json.loads(r.get("tickers") or "[]"))
            except Exception: tickers = set()
            exact = mint in mints
            ticker = bool(sym and sym in tickers)
            if exact or ticker:
                r["exact_mint"] = exact
                matches.append(r)

        # One latest post per handle avoids spam from a single account dominating.
        latest = {}
        for r in matches:
            latest.setdefault((r.get("handle") or "").lower(), r)
        matches = list(latest.values())
        score = 0.0
        positive_accounts, negative_accounts = [], []
        categories = {}
        accum_addresses = set((wallet_signal or {}).get("accumulator_addresses") or [])
        dist_addresses = set((wallet_signal or {}).get("distributor_addresses") or [])

        for r in matches:
            cat = r.get("category") or "search"
            categories[cat] = categories.get(cat, 0) + 1
            weight = float(r.get("account_weight") or 0.35)
            st = float(r.get("stance") or 0)
            base = weight * (2.5 if r.get("exact_mint") else 1.25)
            # Neutral posts still count as attention, but much less than directional posts.
            direction = st if abs(st) >= 0.15 else 0.18
            contrib = base * direction
            wallet = r.get("wallet_address")
            if wallet and st > 0 and wallet in accum_addresses:
                contrib += 2.0 * weight
            if wallet and st > 0 and wallet in dist_addresses:
                contrib -= 2.5 * weight
            if cat == "exchange":
                # A listing/promotion event is useful, but chasing an already vertical candle is not.
                contrib = min(contrib + (1.5 if st >= 0 else 0.5), 3.0)
                if price_change_m5 > 60:
                    contrib *= 0.2
            elif cat in {"ecosystem", "developer", "ecosystem_dev"}:
                # Ecosystem/dev mentions are attention catalysts, not endorsements.
                contrib = max(-2.5, min(2.5, contrib))
            elif cat in {"attention", "onchain", "analysis"}:
                contrib = max(-2.0, min(2.0, contrib))
            elif cat == "risk":
                # Risk investigators matter primarily when they are warning, not when merely mentioning.
                contrib = min(0.0, contrib * 1.5) if st <= 0 else min(0.5, contrib * 0.25)
            elif cat == "search":
                contrib = max(-0.75, min(0.75, contrib))
            score += contrib
            if contrib > 0.4:
                positive_accounts.append(r.get("handle"))
            elif contrib < -0.4:
                negative_accounts.append(r.get("handle"))

        independent = len(set(x for x in positive_accounts if x))
        if independent >= 2: score += 1.5
        if independent >= 3: score += 2.0
        if independent >= 5: score += 2.0
        score = max(-15.0, min(15.0, score))
        return {
            "score_delta": round(score, 2),
            "mentions": len(matches),
            "independent_positive_accounts": independent,
            "positive_accounts": positive_accounts[:12],
            "negative_accounts": negative_accounts[:12],
            "categories": categories,
            "window_minutes": SOCIAL_SIGNAL_WINDOW_MIN,
            "free_source": "FxTwitter public API + syndication fallback",
        }

    async def recent_mints(self, minutes=90, limit=40):
        cutoff = (_now() - timedelta(minutes=minutes)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT extracted_mints FROM social_posts WHERE COALESCE(created_at,observed_at)>=? ORDER BY COALESCE(created_at,observed_at) DESC LIMIT 300",
                (cutoff,),
            )
            rows = await cur.fetchall()
        out, seen = [], set()
        for (raw,) in rows:
            try: vals = json.loads(raw or "[]")
            except Exception: vals = []
            for mint in vals:
                if mint not in seen:
                    seen.add(mint); out.append(mint)
                    if len(out) >= limit: return out
        return out

    async def status(self):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM social_posts")
            posts = int((await cur.fetchone())[0])
            cur = await db.execute("SELECT COUNT(*) FROM social_account_state")
            initialized = int((await cur.fetchone())[0])
            cutoff = (_now() - timedelta(hours=1)).isoformat()
            cur = await db.execute("SELECT COUNT(*) FROM social_posts WHERE observed_at>=?", (cutoff,))
            posts_1h = int((await cur.fetchone())[0])
        return {
            "configured_accounts": len(self.accounts),
            "initialized_accounts": initialized,
            "posts_total": posts,
            "posts_last_hour": posts_1h,
            "last_cycle_at": self.last_cycle_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "source_ok": self.source_ok,
            "cooldown_seconds": max(0, int(self.global_cooldown_until - time.monotonic())),
            "paid_x_api": False,
        }

    async def recent_posts(self, limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM social_posts ORDER BY COALESCE(created_at,observed_at) DESC LIMIT ?", (limit,))
            rows = []
            for r in await cur.fetchall():
                d = dict(r)
                for k in ("extracted_mints", "tickers"):
                    try: d[k] = json.loads(d[k])
                    except Exception: pass
                rows.append(d)
            return rows
