# Extraction plan

Owner2026-10-15: market-data, standalone repository at ~/Projects/work_selfstudy/market-data.

1. Move bounded Binance parsing/decimal/duplicate-key JSON, explicit symbol/product.
2. Move causal candle arithmetic; arbitrary positive fixed UTC cadence; explicit
   instrument/volume/availability/clock identity. Keep skipped buckets/open tail.
3. Thin compatibility conversion in pm-binance/pm-features; preserve BTC wire/state.
4. Generic bounded public REST/WS acquisition and IP-shared proactive weight budget;
   separate Spot/USD-M rules and provider kline finality. No automatic retry storm.
5. Freeze source provenance; compiler/Clippy/MSRV; behavioral tests only on request.
6. License/publication review, immutable dependency pin, CI and reusable examples.

No pm-record paths, strategy, Polymarket features, credentials or raw private
archives move to this repository. No active collector binary change is implied.
