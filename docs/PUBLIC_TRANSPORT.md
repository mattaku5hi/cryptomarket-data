# Optional public Binance transport

Enable `features = ["network"]`. Core parsing and candle consumers do not need
network dependencies enabled. `PublicClient` owns bounded public HTTPS/WS reads;
it has no signing, secrets, trading, raw archive or automatic retry/reconnect.

## Before requests

Supply two `Arc<RequestBudget>` instances shared by every relevant client in
the process: REST request weight and WebSocket connection attempts. REST windows
must be at least60seconds and connection windows at least300seconds. Choose
conservative capacities below current provider limits; no provider capacity is
guessed. Configured limits apply to a rolling window and can delay a request
or refuse before its deadline. A request/connection reservation is charged
before I/O and never refunded on cancellation or failure.

These are local budgets. Other processes sharing the public IP require external
coordination/headroom; this is not a guarantee against429. REST success preserves
headers including any source-reported used weight. A429stops the applicable
budget;418stops both budgets of that client. Clones sharing a stopped budget
refuse further reservations. There is no automatic reset/retry; resolve the
cause and provider wait before explicitly replacing the budget. Retry-After
seconds are retained in HTTP errors when provided.

## REST depth

`depth(instrument, limit, timeout)` performs one official public depth request,
at most30seconds including budget wait, receive and parsing. It rejects redirects,
caps the raw body at1MiB and preserves successful raw body, headers and local
request/receive/availability times. Timestamp units remain source metadata.
HTTP/partial-body failure archival is the recorder's responsibility.

Current reviewed Spot weights:5/25/50/250for1–100/101–500/501–1000/1001–5000levels.
USD-M valid limits5/10/20/50cost2,100cost5,500cost10,1000cost20. These weights
are implementation-versioned source assumptions and must be reviewed on changes.
Snapshots alone do not synchronize a book or prove complete/healthy depth.

## WebSocket

`connect` selects Spot or the explicit USD-M market/public lane for aggregate
trades or100msdepth updates. Each session has a positive lifetime at most23hours,
at most100,000inbound frames and1MiBmessage/frame limits. Caller-supplied idle
timeouts are at most30seconds. Ping receives a bounded Pong; binary/unexpected
messages refuse. Symbol/product identity is checked by the shared parser.

Every error or cancellation latches the session. FrameLimit is a finite bound,
not a healthy-history claim. Drop/close the session and explicitly resynchronize
before continuing dependent state. No missing trades, closed candles or zero
volume are synthesized. Received/available timestamps remain locally observed
wall-clock annotations; clock status/source health are unqualified.

## Finite candle example

```sh
tools/cargo_budget.sh run --features network --example candles --locked -- spot ETHUSDT 30 1000
```

This example was compiler-checked, not run. It constructs30second trade candles
from the public aggregate-trade stream, with Unknown clock, at most5minutes and
the requested inbound-frame cap. Other symbols/fixed cadences are explicit CLI
inputs. Calendar months and provider historical/streamed `klines` are different
interfaces and are not implemented by this trade-candle example.

## Sources reviewed October15,2026

- [Spot depth weights](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints).
- [USD-M depth weights](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book).
- [Spot IP limits and Retry-After](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits).
- [Spot WebSocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

No public Binance request, behavioral control or test was run for this module.
Compiler/Clippy/MSRV evidence does not establish transport fault handling or
production latency. The native recorder retains its existing raw-first transport.
