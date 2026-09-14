# SFalcon v3 — Free-Intelligence Memecoin Paper Trader

SFalcon v3 is an always-on, paper-only Solana memecoin research and simulated-trading engine.

## Core configuration
- Starting virtual bankroll: $1,000
- No leverage
- No real-wallet signing or real trade execution
- Persistent SQLite ledger via Railway DB_PATH=/data/sfalcon.db
- Discovery: 60 seconds by default
- Open-position checks: 5 seconds
- Wallet monitoring batches: 5 seconds
- Entry threshold: 85/100
- Dynamic position sizing up to 25% per coin, max 50% total exposure

## Free intelligence sources
- Jupiter Tokens V2: recent, top organic score, trending, traded, token audits
- GeckoTerminal keyless Solana trending/new pools
- DEX Screener pair pricing/liquidity/volume
- Solana public RPC for tracked-wallet activity

## Wallet intelligence
The supplied terminal exports are deduplicated into `tracked_wallets.json`.
User groups are used only as initial sampling weights:
- THE ONE: highest priority
- Peacock: high priority
- Diamond: above-standard priority
- Main: standard priority

Wallet token-balance increases are recorded as ACCUMULATE and decreases as DISTRIBUTE. These are treated as signals, not proof of a buy/sell, and are combined with liquidity, organic trading, holder, audit and momentum checks.

No historical wallet trades are backfilled when v3 first starts. Each wallet establishes a current transaction baseline, then only future activity is recorded, preserving the forward-test integrity.

## API endpoints
- `/health`
- `/status`
- `/positions`
- `/trades`
- `/candidates`
- `/wallet-status`
- `/wallet-events`
