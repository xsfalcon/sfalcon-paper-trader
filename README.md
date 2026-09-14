# SFalcon — Continuous Memecoin Paper Trader

SFalcon is an always-on, paper-only Solana memecoin research/trading engine.

## What it does
- Scans new/recent Solana token profiles and boosted tokens through DEX Screener.
- Pulls live pair liquidity, price, 5m/1h volume, buys/sells, market cap and age.
- Optionally enriches candidates with Jupiter Tokens V2 holder concentration, developer balance, mint/freeze authority and organic-score data.
- Optionally polls X recent-search for candidate ticker/address hype using your X API bearer token.
- Scores candidates, rejects thin/unsafe-looking setups, and dynamically sizes entries from a $1,000 virtual bankroll.
- Watches open positions every few seconds and simulates stops, partial take-profits, trailing exits, slippage and fees.
- Stores every decision/trade in SQLite.
- Exposes a local API at `/status`, `/positions`, `/trades`, and `/candidates`.

## Important
This project does **not** connect a wallet and cannot place real trades. It is deliberately paper-only so performance can be measured prospectively before risking money.

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
python -m sfalcon.main
```

Open `http://localhost:8787/status` while it is running.

## Continuous operation
Run it on an always-on machine/VPS/container. The defaults are:
- market discovery: every 10 seconds
- open-position monitoring: every 5 seconds
- X polling: every 30 seconds when configured

DEX Screener has rate limits, so increasing frequency too far can reduce reliability rather than improve it.

## Jupiter safety enrichment
Create a Jupiter developer API key and put it in `JUPITER_API_KEY`. This adds holder concentration, developer balance, mint/freeze-authority and organic-score checks.

## X
Create an X developer app and put its bearer token in `X_BEARER_TOKEN`. Without it, SFalcon still runs using market/on-chain-style DEX signals, but its social score is neutral rather than fabricated.

## Telegram / Discord
Private or gated chats cannot be scraped generically. Those sources should only be added using accounts/bots you are authorized to connect to the relevant channels/servers. SFalcon's source layer is modular so authenticated adapters can be added later.


## Railway deployment
The API automatically binds to Railway's injected `PORT`. A lightweight `/health` endpoint is included for service health checks.

For a durable multi-day experiment, attach persistent storage and set `DB_PATH` to a file on that mount. Without persistent storage, a redeploy can reset the local SQLite ledger.
