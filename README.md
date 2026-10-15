# market-data

Reusable Rust public market-data library extracted from polyazimuth-btc.
Rust 1.85, no project-specific pm-* dependency, no credentials or trading.

Implemented core: Binance Spot/USD-M symbol-bound parsing and causal trade
candles with configurable fixed UTC intervals. `polyazimuth-btc` uses the core
through local compatibility adapters. Enable `network` for bounded public depth
reads and aggregate-trade/depth streams with proactive shared process budgets.
[Transport and finite example](docs/PUBLIC_TRANSPORT.md).

No additional exchange or provider REST kline implementation is claimed.
Behavioral acceptance, native raw-first transport migration and public remote
remain pending. Compiler/Clippy/MSRV checks are not behavioral acceptance.

Build through `tools/cargo_budget.sh`; target cap 8 GB with cargo-sweep.
Licensed under [MIT](LICENSE), selected by the owner. Public remote is pending.
