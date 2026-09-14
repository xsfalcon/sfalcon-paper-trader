# SFalcon v4 — Signal-source research brief

Research snapshot: 2026-09-14. These are public signal sources, not claims that any account is paid to promote tokens. Pseudonymous wallet-to-person mappings are used only where public analytics or the existing user watchlist supplied a mapping; otherwise the social account is monitored without asserting a wallet identity.

## Core research conclusion

The strongest implementation is **not** “famous KOL says buy → copy trade.” The most active Solana memecoin wallets often make thousands or tens of thousands of trades per week. That makes single-wallet activity very noisy and sensitive to execution delay. SFalcon therefore treats famous traders as an early-radar layer and rewards **independent convergence**, especially when a mapped trader’s public post agrees with that same wallet’s on-chain accumulation and the token independently passes liquidity, audit, holder, creator, lore and market-structure checks.

Current public KOL analytics illustrate why: Cented has recently shown roughly +$66.5K 7D / +$787K 30D PnL over 8,405 weekly trades; Cupsey roughly +$154.7K 7D / +$470.7K 30D over 37,365 weekly trades; Kadenox roughly +$30.7K 7D / +$243.9K 30D over 10,541 trades; Theo roughly +$20.2K 7D / +$321K 30D over 14,033 trades; Cottage roughly +$96.1K 7D / +$193.1K 30D over 5,659 trades. Cooker is the useful counterexample: a high recent win rate can coexist with negative 7D and 30D PnL, so win rate or mindshare alone must not be a bullish score. These figures change continuously and are used as research context rather than hard-coded performance promises.

## Curated public X/CT watchlist

### Memecoin traders / wallet-mapped traders (16)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @Cented7 | A | yes | high-PnL, high-noise nano-cap radar |
| @notdecu | A | yes | strong recent PnL/win-rate; high activity |
| @theonomix | A | yes | high-volume early radar |
| @radiancebrr | A | yes | trader |
| @Ga__ke | A | yes | trader |
| @CookerFlips | B | yes | high mindshare but current PnL can diverge; confirmation only |
| @kadenox | A | yes | trader |
| @clukzSOL | B | yes | trader |
| @trunoest | A | yes | very strong current 7D/30D PnL; still high-noise |
| @cap100x | A | yes | trader |
| @nyhrox | A | yes | trader |
| @XScharo | B | yes | trader |
| @cottagepvp | A | yes | trader |
| @goyimpnl | B | no | useful for launch/bundling behavior; not automatic bullish signal |
| @boogiepnl | B | yes | trader |
| @Cupseyy | A | yes | famous high-volume Solana memecoin trader; wallet signal is high-noise and requires confirmation |

### KOL / memecoin mindshare (7)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @blknoiz06 | A | yes | high mindshare; wallet confirmation required |
| @s1mple_s1mple | B | no | kol |
| @nftkeano | B | no | kol |
| @SolanaDali | B | no | kol |
| @TeddyxROO | B | no | kol |
| @MustStopMurad | A | no | major memecoin thesis/mindshare account; attention catalyst, not auto-buy |
| @frankdegods | B | no | Solana consumer-crypto/meme-culture commentator; no wallet mapping assumed |

### Ecosystem, protocol and developer accounts (9)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @Pumpfun | A | no | ecosystem_dev |
| @a1lon9 | A | no | ecosystem_dev |
| @solana | A | no | ecosystem_dev |
| @aeyakovenko | A | no | attention catalyst; not treated as fundamental endorsement |
| @JupiterExchange | A | no | ecosystem_dev |
| @weremeow | A | no | ecosystem_dev |
| @phantom | A | no | ecosystem_dev |
| @RaydiumProtocol | B | no | ecosystem_dev |
| @MeteoraAG | B | no | ecosystem_dev |

### Exchange accounts (11)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @binance | A | no | exchange |
| @CoinbaseAssets | A | no | exchange |
| @CoinbaseExch | A | no | exchange |
| @Coinbase | A | no | exchange |
| @OKX | A | no | exchange |
| @Bybit_Official | B | no | exchange |
| @MEXC_Official | B | no | exchange |
| @gate_io | B | no | exchange |
| @kucoincom | B | no | exchange |
| @krakenfx | B | no | exchange |
| @bitgetglobal | B | no | exchange |

### High-mindshare/news/on-chain attention (9)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @SolanaFloor | A | no | attention |
| @WatcherGuru | A | no | attention |
| @arkham | A | no | attention |
| @lookonchain | A | no | attention |
| @OnchainLens | A | no | attention |
| @WuBlockchain | A | no | attention |
| @tier10k | B | no | attention |
| @DegenerateNews | B | no | attention |
| @Stalkchain | B | no | attention |

### Risk / investigation accounts (2)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @bubblemaps | A | no | risk |
| @Deebs_DeFi | A | no | risk |

### Analyst accounts (1)

| Handle | Tier | Wallet-linked | Role in SFalcon |
|---|---:|---:|---|
| @ieatjeets | A | no | on-chain/narrative analysis; mentions are contextual rather than direct calls |

## Exchange research

Binance has an official **New Cryptocurrency Listing** announcement category. SFalcon polls its public announcement surface as a structured catalyst feed, while also watching official exchange X accounts such as Binance and Coinbase Assets/Exchange. Coinbase’s help center explicitly identifies @Coinbase, @CoinbaseAssets and @CoinbaseExch as official accounts. A listing signal is not blindly bullish: if the token has already moved vertically, the boost is cut sharply to avoid buying the announcement candle.

## Free X monitoring research

SFalcon v4 does not use the paid X API. Its primary best-effort read layer is the open-source FxTwitter/FxEmbed public API, which exposes public profile timelines and post search without an X bearer token. It has a documented `/2/profile/{handle}/statuses` route and `/2/search` route. X syndication is retained only as a fallback for curated profile timelines. Because these are unofficial/third-party public-reading paths rather than a guaranteed X firehose, v4 records source health, backs off on 429s, and never claims complete coverage of all X.

## How the signals are interpreted

- **Trader/KOL post only:** small-to-moderate attention boost; never enough by itself.
- **Mapped trader post + same wallet accumulation:** stronger confirmation.
- **Multiple independent trader wallets accumulating:** convergence boost.
- **Post says bullish while mapped wallet distributes:** contradiction penalty.
- **Exchange listing/promotion:** event catalyst, chase-adjusted.
- **Ecosystem/dev mention:** attention catalyst, not an endorsement.
- **Risk/investigator warning:** negative evidence, weighted more strongly than a neutral mention.
- **Unknown account from broad search:** very low weight unless corroborated.

## Sources used for the research design

- KOL Explorer public Solana trader analytics and wallet pages (Cented, Cupsey, Kadenox, Theo, Cottage, Decu, Cooker, Trunoest and others).
- FxEmbed/FxTwitter open-source API documentation and OpenAPI routes.
- Binance Support → New Cryptocurrency Listing announcements.
- Coinbase Help → official social media account list.
- Existing user-exported wallet watchlists, deduplicated and preserved as the primary personal smart-money layer.
