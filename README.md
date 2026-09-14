# SFalcon v4 — paper-only memecoin research trader

SFalcon v4 is a forward paper-trading research system for Solana memecoins. It keeps the existing $1,000 virtual ledger and persistent SQLite database, and **does not contain real-wallet signing or real-money execution code**.

## v4 intelligence stack

- **Pre-bond radar:** watches Pump.fun launches approaching graduation and evaluates lore/narrative strength, originality signals, creator reuse, metadata quality, and prior observed duplicates.
- **Post-bond radar:** keeps recently graduated launches in the candidate pool for 24 hours and scores early post-migration liquidity, volume, buy flow, and price behavior.
- **Volatility radar:** flags candidates with unusual 5-minute/1-hour movement or volume/liquidity expansion. Volatility is a research trigger, not automatically a bullish signal.
- **414 tracked wallets:** public Solana wallet activity is monitored forward-only. Wallet convergence can strengthen a setup; distribution can weaken it or contribute to an exit signal.
- **Crypto Twitter / X watchlist:** 55 curated trader/KOL, ecosystem/developer, exchange, on-chain, and high-mindshare public accounts are monitored on a best-effort basis through free public read infrastructure. No paid X API token is required.
- **Candidate-specific CT research:** stronger candidates get a free public X search for exact mint/contract and ticker mentions, cached to avoid unnecessary requests.
- **Exchange event monitor:** official/public exchange announcement surfaces are checked for listing-related catalysts. Exchange attention is reduced when price is already vertically extended.
- **Mandatory research gate:** every possible entry receives a research snapshot covering market structure, liquidity/volume, Jupiter audit fields, holder/developer concentration, wallet flow, lore, post-bond state, volatility, exchange events, and social evidence. Only `QUALIFIED` candidates can enter the paper portfolio.

## Important signal philosophy

SFalcon does **not** blindly copy a famous trader or assume a public mention is a paid promotion or endorsement. High-frequency KOL wallets are primarily early-radar sources. Stronger evidence is independent convergence: multiple wallets or accounts pointing at the same token, especially when a mapped trader's public post agrees with that trader's on-chain accumulation and the token still passes safety/liquidity checks.

Likewise, a developer/ecosystem account mentioning a token is treated as an **attention catalyst**, not proof of quality. An exchange listing is a catalyst, but the score is sharply reduced if the token has already pumped enough that the bot would be chasing.

## Free X monitoring caveat

The primary free source is the public FxTwitter/FxEmbed API, with X's public syndication timeline as a fallback for curated profiles. These are read-only and require no paid X API credits, but availability and upstream behavior can change. SFalcon therefore records source health and gracefully degrades instead of pretending it has a guaranteed full X firehose.

## Main endpoints

- `/status` — account and subsystem health
- `/candidates` — current ranked candidates and full score breakdown
- `/research` — research-gate runs and verdicts
- `/volatile` — volatility radar events
- `/prebond` — near-graduation Pump.fun candidates
- `/postbond` — recently graduated candidates
- `/wallet-status`, `/wallet-events` — wallet intelligence
- `/social-status`, `/social-events` — public CT watchlist/search intelligence
- `/exchange-events` — exchange catalyst feed
- `/lore-status` — bond/lore subsystem
- `/trades`, `/positions` — paper fills/positions

## Default paper-entry rule

The existing 85/100 threshold remains. A score is **not sufficient by itself**: the research gate must also return `QUALIFIED`, and hard safety blocks (e.g. suspicious audit, active mint/freeze authority when known, extreme holder/dev concentration, minimum liquidity/volume failure, or extreme chase conditions) prevent entry.

## New environment variables (all optional)

- `SOCIAL_CHECK_SECONDS=15`
- `EXCHANGE_CHECK_SECONDS=60`
- `SOCIAL_RESEARCH_PER_SCAN=3`
- `SOCIAL_SIGNAL_WINDOW_MIN=120`
- `SOCIAL_SEARCH_COOLDOWN_MIN=20`
- `POSTBOND_TRACK_HOURS=24`
- `FXTWITTER_BASE=https://api.fxtwitter.com`

Existing Railway variables such as `DB_PATH`, `JUPITER_API_KEY`, and the paper-risk settings continue to work unchanged.
