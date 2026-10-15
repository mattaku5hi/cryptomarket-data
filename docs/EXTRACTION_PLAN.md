# Extraction plan

Owner2026-10-15: crate market-data, standalone repository at
~/Projects/work_selfstudy/cryptomarket-data (owner-created final path).

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

Publication: public GitHub repository and compiler CI passed at b530706.
MIT selected; public transport, bounded example and CI implemented.
Compiler-only checks passed; behavioral acceptance remains pending.
EXTRACTION_PROVENANCE.json describes the earlier core extraction snapshot;
NETWORK_COMPILE.json records the subsequent transport/compiler scope.
