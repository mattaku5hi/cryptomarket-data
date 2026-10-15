# market-data

Reusable Rust public market-data library extracted from polyazimuth-btc.
Rust 1.85, no project-specific pm-* dependency, no credentials or trading.

In progress: Binance Spot/USD-M symbol-bound parsing, causal trade candles with
configurable fixed UTC intervals, then transport and proactive request budgets.
REST kline ingestion is new work, not an already extracted implementation.
No exchange-agnostic transport abstraction or additional exchange implementation
is claimed. Consumer migration, behavioral acceptance and public remote are pending.

Build through `tools/cargo_budget.sh`; target cap 8 GB with cargo-sweep.
No license or public publication decision is inferred by local repository setup.
