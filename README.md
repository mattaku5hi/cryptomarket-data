# market-data

Reusable Rust public market-data library extracted from polyazimuth-btc.
Rust 1.85, no project-specific pm-* dependency, no credentials or trading.

Implemented core: Binance Spot/USD-M symbol-bound parsing and causal trade
candles with configurable fixed UTC intervals. `polyazimuth-btc` uses the core
through local compatibility adapters. Enable `network` for bounded public depth
reads and aggregate-trade/depth streams with proactive shared process budgets.
[Transport and finite example](docs/PUBLIC_TRANSPORT.md).

The additive `feeds` API provides decimal Binance/Bybit minute candles, public
trades, open interest, funding, liquidation snapshots and instrument metadata.
Its optional transport returns bounded raw responses before parsing and shares
request budgets across clients. [Feed contracts](docs/SCANNER_FEEDS.md).

Feature `vision` adds `market_data::vision`: a cached, sha256-verified reader for
the public data.binance.vision archive (USD-M klines, metrics, funding, premium
index; spot klines). The cache mirrors archive paths, keeps `.CHECKSUM` files and
`.404` negative entries, and works fully offline from an existing cache.

The workspace also includes extracted `pm-schema` and `pm-record` crates for
checksummed event recording. Existing Binance/trade-candle APIs remain available.
Scanner orchestration, strategies and notification logic belong to consumers.

Build through `tools/cargo_budget.sh`; target cap 8 GB with cargo-sweep.
Licensed under [MIT](LICENSE), selected by the owner.

Repository: https://github.com/mattaku5hi/cryptomarket-data

The GitHub repository is named `cryptomarket-data`; the Rust crate is
`market-data`. The owner workspace is `~/Projects/work_selfstudy/cryptomarket-data`.
Consumers pin a reviewed Git revision. The original sibling `market-data` draft
is retained for rollback and is no longer the development authority.
