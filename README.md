# market-data

Reusable Rust public market-data library extracted from polyazimuth-btc.
Rust 1.85, no project-specific pm-* dependency, no credentials or trading.

Implemented core: Binance Spot/USD-M symbol-bound parsing and causal trade
candles with configurable fixed UTC intervals. `polyazimuth-btc` uses the core
through local compatibility adapters. Enable `network` for bounded public depth
reads and aggregate-trade/depth streams with proactive shared process budgets.
[Transport and finite example](docs/PUBLIC_TRANSPORT.md).

No additional exchange or provider REST kline implementation is claimed.
Behavioral acceptance and native raw-first transport migration remain pending. Compiler/Clippy/MSRV checks are not behavioral acceptance.

Build through `tools/cargo_budget.sh`; target cap 8 GB with cargo-sweep.
Licensed under [MIT](LICENSE), selected by the owner.

Repository: https://github.com/mattaku5hi/cryptomarket-data

The GitHub repository is named `cryptomarket-data`; the Rust crate is
`market-data`. The owner workspace is `~/Projects/work_selfstudy/cryptomarket-data`.
Consumers pin a reviewed Git revision. The original sibling `market-data` draft
is retained for rollback and is no longer the development authority.
