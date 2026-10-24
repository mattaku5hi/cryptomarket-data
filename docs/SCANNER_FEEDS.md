# Public scanner feeds

`market_data::feeds` is additive to the existing Binance/trade-candle API.
Identity is `{ venue, market, symbol }`; prices and volumes are decimals and
timestamps are UTC Unix milliseconds. Funding rates are fractions per interval.
Open interest is a provider's base-contract quantity, not USDT notional; compare
changes within a provider and contract rather than adding incompatible series.

`feeds::binance` parses raw and combined 1m klines, all-market mark-price arrays,
liquidation snapshots, REST klines, OI and instrument metadata. Current USD-M
market streams use `/market/stream`; mixed CM rows are outside the USD-M scope.
The provider's `T=0` funding timestamp denotes unavailable next-funding time.
Liquidation rows describe sampled cumulative fills and cannot establish a full
market liquidation total. [Binance streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect).

`feeds::bybit` parses v5 metadata, REST candles/OI/turnover, WS candles/trades,
and snapshot/delta tickers. Keep a bounded `TickerCache` for each connection,
reset it on reconnect, and preserve omitted delta fields. Funding intervals
are normalized to minutes. Historical REST candles have no taker-buy volume;
unknown buy volume stays `None`. `bybit::ret_code` extracts a non-zero REST
`retCode`/`retMsg` from a raw body; `is_transient_code` marks 10006, 10016 and
10018 as throttling or busy rather than symbol errors. [Bybit tickers](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker),
[REST candles](https://bybit-exchange.github.io/docs/v5/market/kline).

With the `network` feature, `feeds::network::PublicClient` exposes bounded raw
HTTP/WS reads. Share request and connection budgets with every client targeting
the same provider in a process. REST usage headers and explicit provider limits
apply an 80% threshold; `set_rest_limit` updates it without resetting usage or
cooldowns. HTTP failures remain raw responses for archival. Check the status
before parsing; honor `BudgetWait` when reconnecting or retrying.

REST and connection cooldowns propagate in both directions. HTTP 403 pauses for
at least ten minutes; 418 without Retry-After pauses for three days. Bybit
`retCode=10006`, including HTTP 200, respects reset/retry headers with a sixty-second
minimum. `defer_for` restores a caller's durable cooldown; `cooldown_remaining`
exposes it for persistence. Unrepresentable delays halt instead of shortening.

The transport handles ping/pong and bounded session lifetimes. Consumers own
subscription acknowledgement checks, reconnects, gap backfill, event archives,
and data-readiness gates. Shard Bybit subscriptions to fit its 21,000-character
topic bound. Budgets do not coordinate separate processes on the same IP.
[Bybit connection limits](https://bybit-exchange.github.io/docs/v5/ws/connect).

`pm-schema` and `pm-record` retain their original production source and package
names; their READMEs record extraction provenance. They include legacy schema
types for compatibility. No scanner strategy depends in the reverse direction.

Verification commands:

```sh
tools/cargo_budget.sh test --workspace --all-targets --features network
tools/cargo_budget.sh clippy --workspace --all-targets --features network -- -D warnings
```
