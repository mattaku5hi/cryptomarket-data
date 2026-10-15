//! Bybit v5 public spot and USDT-linear perpetual feeds.
//!
//! REST kline callers supply the requested interval; it is not echoed by Bybit.
//! Candle volumes are cumulative base quantity and quote turnover, with no taker split.
//! See https://bybit-exchange.github.io/docs/v5/websocket/public/ticker for patch semantics.
use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde_json::Value;

use super::{
    CandleInterval, Funding, HistoricalCandle, Instrument, InstrumentMetadata, Market, MarketEvent,
    MinuteCandle, OpenInterest, ParseError, Side, Trade, Venue,
};
use crate::decimal::ExactDecimal;

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ITEMS: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentPage {
    pub instruments: Vec<InstrumentMetadata>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenInterestPage {
    pub observations: Vec<OpenInterest>,
    pub next_cursor: Option<String>,
}

/// A partially populated ticker. Readiness requires a snapshot in this connection.
/// OI is base quantity (Bybit's both-sides measure); value is quote notional.
/// Funding rates and caps are fractions, not percentage points.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickerState {
    pub instrument: Instrument,
    pub updated_at_ms: i64,
    pub sequence: u64,
    pub seen_snapshot: bool,
    pub turnover_24h: Option<Decimal>,
    pub open_interest: Option<Decimal>,
    pub open_interest_value: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_time_ms: Option<i64>,
    pub funding_interval_minutes: Option<u32>,
    pub funding_cap: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub index_price: Option<Decimal>,
}

impl TickerState {
    pub fn open_interest_ready(&self) -> bool {
        self.seen_snapshot && self.open_interest.is_some()
    }

    pub fn funding_ready(&self) -> bool {
        self.seen_snapshot && self.funding_rate.is_some()
    }

    fn empty(instrument: Instrument, updated_at_ms: i64, sequence: u64) -> Self {
        Self {
            instrument,
            updated_at_ms,
            sequence,
            seen_snapshot: false,
            turnover_24h: None,
            open_interest: None,
            open_interest_value: None,
            funding_rate: None,
            next_funding_time_ms: None,
            funding_interval_minutes: None,
            funding_cap: None,
            mark_price: None,
            index_price: None,
        }
    }
}

/// Bounded per-instrument cache. Call `clear` on reconnect before accepting deltas.
pub struct TickerCache {
    max_symbols: usize,
    states: BTreeMap<Instrument, TickerState>,
}

impl TickerCache {
    pub fn new(max_symbols: usize) -> Result<Self, ParseError> {
        if max_symbols == 0 || max_symbols > MAX_ITEMS {
            return Err(ParseError::InvalidField("max_symbols"));
        }
        Ok(Self {
            max_symbols,
            states: BTreeMap::new(),
        })
    }

    pub fn get(&self, instrument: &Instrument) -> Option<&TickerState> {
        self.states.get(instrument)
    }

    pub fn clear(&mut self) {
        self.states.clear();
    }

    /// Invalid patches leave the entire cache unchanged. A snapshot replaces prior state.
    pub fn apply(&mut self, raw: &[u8], market: Market) -> Result<TickerState, ParseError> {
        self.apply_value(&json(raw)?, market)
    }

    fn apply_value(&mut self, value: &Value, market: Market) -> Result<TickerState, ParseError> {
        let instrument = topic_instrument(value, "tickers.", market)?;
        let data = field(value, "data")?;
        if string(data, "symbol")? != instrument.symbol {
            return Err(ParseError::InstrumentMismatch);
        }
        let snapshot = match string(value, "type")? {
            "snapshot" => true,
            "delta" if market == Market::UsdtPerpetual => false,
            _ => return Err(ParseError::UnsupportedMessage),
        };
        let time = timestamp(field(value, "ts")?, "ts")?;
        let sequence = field(value, "cs")?
            .as_u64()
            .ok_or(ParseError::InvalidField("cs"))?;
        let previous = self.states.get(&instrument);
        if previous.is_none() && self.states.len() >= self.max_symbols {
            return Err(ParseError::Limit("ticker symbols"));
        }
        let mut next = if snapshot {
            TickerState::empty(instrument, time, sequence)
        } else if let Some(previous) = previous {
            if time < previous.updated_at_ms || sequence < previous.sequence {
                return Err(ParseError::InvalidField("stale ticker delta"));
            }
            previous.clone()
        } else {
            TickerState::empty(instrument, time, sequence)
        };
        next.updated_at_ms = time;
        next.sequence = sequence;
        next.seen_snapshot |= snapshot;
        patch_decimal(data, "turnover24h", &mut next.turnover_24h, false)?;
        if market == Market::UsdtPerpetual {
            patch_decimal(data, "openInterest", &mut next.open_interest, false)?;
            patch_decimal(
                data,
                "openInterestValue",
                &mut next.open_interest_value,
                false,
            )?;
            patch_decimal(data, "fundingRate", &mut next.funding_rate, true)?;
            patch_decimal(data, "fundingCap", &mut next.funding_cap, false)?;
            patch_decimal(data, "markPrice", &mut next.mark_price, false)?;
            patch_decimal(data, "indexPrice", &mut next.index_price, false)?;
            if let Some(value) = data.get("nextFundingTime") {
                next.next_funding_time_ms = optional_time(value, "nextFundingTime")?;
            }
            if let Some(value) = data.get("fundingIntervalHour") {
                next.funding_interval_minutes = if value.as_str() == Some("") {
                    None
                } else {
                    let hours = timestamp(value, "fundingIntervalHour")?;
                    let minutes = u32::try_from(hours)
                        .ok()
                        .and_then(|v| v.checked_mul(60))
                        .filter(|v| *v > 0)
                        .ok_or(ParseError::InvalidField("fundingIntervalHour"))?;
                    Some(minutes)
                };
            }
        }
        self.states.insert(next.instrument.clone(), next.clone());
        Ok(next)
    }
}

/// Parses one supported WS message. Unknown/control messages return UnsupportedMessage.
/// Only OI/funding fields present in this frame generate samples; cached values retain context.
pub fn parse_websocket(
    raw: &[u8],
    market: Market,
    cache: &mut TickerCache,
) -> Result<Vec<MarketEvent>, ParseError> {
    let value = json(raw)?;
    let topic = string(&value, "topic")?;
    if topic.starts_with("kline.") {
        return ws_klines(&value, market).map(|v| v.into_iter().map(MarketEvent::Candle).collect());
    }
    if topic.starts_with("publicTrade.") {
        return ws_trades(&value, market).map(|v| v.into_iter().map(MarketEvent::Trade).collect());
    }
    if !topic.starts_with("tickers.") {
        return Err(ParseError::UnsupportedMessage);
    }
    let state = cache.apply_value(&value, market)?;
    let data = field(&value, "data")?;
    let mut events = Vec::new();
    if state.seen_snapshot && data.get("openInterest").is_some() {
        if let Some(amount) = state.open_interest {
            events.push(MarketEvent::OpenInterest(OpenInterest {
                instrument: state.instrument.clone(),
                time_ms: state.updated_at_ms,
                value: amount,
            }));
        }
    }
    if state.seen_snapshot && data.get("fundingRate").is_some() {
        if let Some(rate) = state.funding_rate {
            events.push(MarketEvent::Funding(Funding {
                instrument: state.instrument,
                time_ms: state.updated_at_ms,
                rate,
                next_funding_time_ms: state.next_funding_time_ms,
                interval_minutes: state.funding_interval_minutes,
                mark_price: state.mark_price,
                index_price: state.index_price,
            }));
        }
    }
    Ok(events)
}

/// Includes inactive instruments for caller-side eligibility checks; filters other contract families.
/// Linear pages must be followed until next_cursor=None. Spot has no pagination.
pub fn parse_instruments(raw: &[u8], market: Market) -> Result<InstrumentPage, ParseError> {
    let value = json(raw)?;
    let result = rest_result(&value, market)?;
    let mut instruments = Vec::new();
    let mut symbols = BTreeSet::new();
    for item in list(result, "list")? {
        let symbol = string(item, "symbol")?;
        let base_asset = asset(item, "baseCoin")?;
        let quote_asset = asset(item, "quoteCoin")?;
        if market == Market::UsdtPerpetual
            && (quote_asset != "USDT"
                || string(item, "contractType")? != "LinearPerpetual"
                || asset(item, "settleCoin")? != "USDT")
        {
            continue;
        }
        validate_symbol(symbol)?;
        if !symbols.insert(symbol) {
            return Err(ParseError::InvalidField("duplicate symbol"));
        }
        let status = string(item, "status")?;
        bounded_text(status, "status", 64)?;
        let (listing_time_ms, delivery_time_ms, max_leverage) = if market == Market::UsdtPerpetual {
            let listing = timestamp(field(item, "launchTime")?, "launchTime")?;
            let delivery = timestamp(field(item, "deliveryTime")?, "deliveryTime")?;
            let leverage = decimal(
                field(field(item, "leverageFilter")?, "maxLeverage")?,
                "maxLeverage",
                false,
            )?;
            if leverage <= Decimal::ZERO {
                return Err(ParseError::InvalidField("maxLeverage"));
            }
            (
                Some(listing),
                if delivery == 0 { None } else { Some(delivery) },
                Some(leverage),
            )
        } else {
            (None, None, None)
        };
        instruments.push(InstrumentMetadata {
            instrument: Instrument {
                venue: Venue::Bybit,
                market,
                symbol: symbol.to_owned(),
            },
            base_asset,
            quote_asset,
            status: status.to_owned(),
            is_perpetual: market == Market::UsdtPerpetual,
            listing_time_ms,
            delivery_time_ms,
            max_leverage,
        });
    }
    let next_cursor = cursor(result)?;
    if market == Market::Spot && next_cursor.is_some() {
        return Err(ParseError::InvalidField("spot cursor"));
    }
    Ok(InstrumentPage {
        instruments,
        next_cursor,
    })
}

/// Parses quote-asset 24h turnover from /v5/market/tickers for universe selection.
/// The linear endpoint also returns other contract families, which are excluded.
pub fn parse_turnovers(
    raw: &[u8],
    market: Market,
) -> Result<BTreeMap<Instrument, Decimal>, ParseError> {
    let value = json(raw)?;
    let result = rest_result(&value, market)?;
    let mut turnovers = BTreeMap::new();
    for row in list(result, "list")? {
        let symbol = string(row, "symbol")?;
        if market == Market::UsdtPerpetual && !symbol.ends_with("USDT") {
            continue;
        }
        let instrument = Instrument {
            venue: Venue::Bybit,
            market,
            symbol: symbol.to_owned(),
        };
        validate_instrument(&instrument)?;
        let turnover = decimal(field(row, "turnover24h")?, "turnover24h", false)?;
        if turnovers.insert(instrument, turnover).is_some() {
            return Err(ParseError::InvalidField("duplicate symbol"));
        }
    }
    Ok(turnovers)
}

/// Parses interval=1 REST responses into ascending order. The caller supplies observation time.
/// Rows whose minute has not ended remain forming. No buy volume is inferred from turnover.
pub fn parse_rest_klines(
    raw: &[u8],
    instrument: &Instrument,
    observed_at_ms: i64,
) -> Result<Vec<MinuteCandle>, ParseError> {
    parse_rest_values(raw, instrument, CandleInterval::M1, observed_at_ms)
}

/// Parse REST klines requested with `interval.bybit_name()`. Bybit does not echo
/// the interval or a close time, so the caller must supply the requested interval.
/// Results are ascending, retain forming rows, and never infer taker buy volume.
pub fn parse_rest_candles(
    raw: &[u8],
    instrument: &Instrument,
    interval: CandleInterval,
    observed_at_ms: i64,
) -> Result<Vec<HistoricalCandle>, ParseError> {
    Ok(
        parse_rest_values(raw, instrument, interval, observed_at_ms)?
            .into_iter()
            .map(|candle| HistoricalCandle::from_values(candle, interval))
            .collect(),
    )
}

fn parse_rest_values(
    raw: &[u8],
    instrument: &Instrument,
    interval: CandleInterval,
    observed_at_ms: i64,
) -> Result<Vec<MinuteCandle>, ParseError> {
    validate_instrument(instrument)?;
    if observed_at_ms < 0 {
        return Err(ParseError::InvalidField("observed_at_ms"));
    }
    let value = json(raw)?;
    let result = rest_result(&value, instrument.market)?;
    match_symbol(result, instrument)?;
    let mut candles = Vec::new();
    let mut starts = BTreeSet::new();
    for row in list(result, "list")? {
        let values = row
            .as_array()
            .filter(|v| v.len() == 7)
            .ok_or(ParseError::InvalidField("kline row"))?;
        let at = |n| values.get(n).ok_or(ParseError::InvalidField("kline row"));
        let start = timestamp(at(0)?, "startTime")?;
        let end = interval.end_ms(start)?;
        if start > observed_at_ms || !starts.insert(start) {
            return Err(ParseError::InvalidField("startTime"));
        }
        let candle = MinuteCandle {
            instrument: instrument.clone(),
            open_time_ms: start,
            updated_at_ms: observed_at_ms,
            open: decimal(at(1)?, "open", false)?,
            high: decimal(at(2)?, "high", false)?,
            low: decimal(at(3)?, "low", false)?,
            close: decimal(at(4)?, "close", false)?,
            base_volume: decimal(at(5)?, "volume", false)?,
            quote_volume: decimal(at(6)?, "turnover", false)?,
            taker_buy_quote_volume: None,
            is_final: observed_at_ms >= end,
        };
        validate_candle(&candle)?;
        candles.push(candle);
    }
    candles.sort_by_key(|c| c.open_time_ms);
    Ok(candles)
}

/// Linear OI is the exchange's both-sides base-asset quantity, never quote notional.
pub fn parse_open_interest(
    raw: &[u8],
    instrument: &Instrument,
) -> Result<OpenInterestPage, ParseError> {
    validate_instrument(instrument)?;
    if instrument.market != Market::UsdtPerpetual {
        return Err(ParseError::UnsupportedMessage);
    }
    let value = json(raw)?;
    let result = rest_result(&value, instrument.market)?;
    match_symbol(result, instrument)?;
    let mut observations = Vec::new();
    let mut times = BTreeSet::new();
    for row in list(result, "list")? {
        let time_ms = timestamp(field(row, "timestamp")?, "timestamp")?;
        if !times.insert(time_ms) {
            return Err(ParseError::InvalidField("duplicate timestamp"));
        }
        observations.push(OpenInterest {
            instrument: instrument.clone(),
            time_ms,
            value: decimal(field(row, "openInterest")?, "openInterest", false)?,
        });
    }
    observations.sort_by_key(|oi| oi.time_ms);
    Ok(OpenInterestPage {
        observations,
        next_cursor: cursor(result)?,
    })
}

fn ws_klines(value: &Value, market: Market) -> Result<Vec<MinuteCandle>, ParseError> {
    let instrument = topic_instrument(value, "kline.1.", market)?;
    if string(value, "type")? != "snapshot" {
        return Err(ParseError::UnsupportedMessage);
    }
    let time = timestamp(field(value, "ts")?, "ts")?;
    let mut candles = Vec::new();
    for row in list(value, "data")? {
        if string(row, "interval")? != "1" {
            return Err(ParseError::UnsupportedMessage);
        }
        let start = timestamp(field(row, "start")?, "start")?;
        let end = minute_end(start)?;
        // Live rollover batches may stamp the closed row at publication after its end.
        let last_trade = timestamp(field(row, "timestamp")?, "timestamp")?;
        let is_final = field(row, "confirm")?
            .as_bool()
            .ok_or(ParseError::InvalidField("confirm"))?;
        if timestamp(field(row, "end")?, "end")? != end - 1
            || time < start
            || last_trade < start
            || last_trade > time
            || (is_final && time < end)
        {
            return Err(ParseError::InvalidField("kline time"));
        }
        let candle = MinuteCandle {
            instrument: instrument.clone(),
            open_time_ms: start,
            updated_at_ms: time,
            open: decimal(field(row, "open")?, "open", false)?,
            high: decimal(field(row, "high")?, "high", false)?,
            low: decimal(field(row, "low")?, "low", false)?,
            close: decimal(field(row, "close")?, "close", false)?,
            base_volume: decimal(field(row, "volume")?, "volume", false)?,
            quote_volume: decimal(field(row, "turnover")?, "turnover", false)?,
            taker_buy_quote_volume: None,
            is_final,
        };
        validate_candle(&candle)?;
        candles.push(candle);
    }
    Ok(candles)
}

fn ws_trades(value: &Value, market: Market) -> Result<Vec<Trade>, ParseError> {
    let instrument = topic_instrument(value, "publicTrade.", market)?;
    if string(value, "type")? != "snapshot" {
        return Err(ParseError::UnsupportedMessage);
    }
    let emitted = timestamp(field(value, "ts")?, "ts")?;
    let mut trades = Vec::new();
    for row in list(value, "data")? {
        if string(row, "s")? != instrument.symbol {
            return Err(ParseError::InstrumentMismatch);
        }
        let time_ms = timestamp(field(row, "T")?, "T")?;
        if time_ms > emitted {
            return Err(ParseError::InvalidField("T"));
        }
        let id = string(row, "i")?;
        bounded_text(id, "i", 128)?;
        let side = match string(row, "S")? {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            _ => return Err(ParseError::InvalidField("S")),
        };
        let price = decimal(field(row, "p")?, "p", false)?;
        let quantity = decimal(field(row, "v")?, "v", false)?;
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(ParseError::InvalidField("trade amount"));
        }
        trades.push(Trade {
            instrument: instrument.clone(),
            time_ms,
            id: id.to_owned(),
            side,
            price,
            quantity,
        });
    }
    Ok(trades)
}

fn json(raw: &[u8]) -> Result<Value, ParseError> {
    crate::json::parse(raw, MAX_BYTES).map_err(|error| match error {
        crate::json::Error::Limit => ParseError::Limit("frame bytes"),
        _ => ParseError::MalformedJson,
    })
}
fn field<'a>(value: &'a Value, name: &'static str) -> Result<&'a Value, ParseError> {
    value.get(name).ok_or(ParseError::InvalidField(name))
}
fn string<'a>(value: &'a Value, name: &'static str) -> Result<&'a str, ParseError> {
    field(value, name)?
        .as_str()
        .ok_or(ParseError::InvalidField(name))
}
fn list<'a>(value: &'a Value, name: &'static str) -> Result<&'a Vec<Value>, ParseError> {
    let list = field(value, name)?
        .as_array()
        .ok_or(ParseError::InvalidField(name))?;
    if list.len() > MAX_ITEMS {
        return Err(ParseError::Limit("items"));
    }
    Ok(list)
}
fn timestamp(value: &Value, name: &'static str) -> Result<i64, ParseError> {
    let result = if let Some(s) = value.as_str() {
        if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) || s.len() > 19 {
            return Err(ParseError::InvalidField(name));
        }
        s.parse::<i64>()
            .map_err(|_| ParseError::InvalidField(name))?
    } else {
        value.as_i64().ok_or(ParseError::InvalidField(name))?
    };
    if result < 0 {
        return Err(ParseError::InvalidField(name));
    }
    Ok(result)
}
fn optional_time(value: &Value, name: &'static str) -> Result<Option<i64>, ParseError> {
    if value.as_str() == Some("") {
        Ok(None)
    } else {
        timestamp(value, name).map(Some)
    }
}
fn decimal(value: &Value, name: &'static str, signed: bool) -> Result<Decimal, ParseError> {
    let text = value.as_str().ok_or(ParseError::InvalidField(name))?;
    if text.len() > 64 {
        return Err(ParseError::InvalidField(name));
    }
    let value = ExactDecimal::parse(text)
        .map_err(|_| ParseError::InvalidField(name))?
        .value();
    if !signed && value < Decimal::ZERO {
        return Err(ParseError::InvalidField(name));
    }
    Ok(value)
}
fn patch_decimal(
    data: &Value,
    name: &'static str,
    target: &mut Option<Decimal>,
    signed: bool,
) -> Result<(), ParseError> {
    if let Some(value) = data.get(name) {
        *target = if value.as_str() == Some("") {
            None
        } else {
            Some(decimal(value, name, signed)?)
        };
    }
    Ok(())
}
fn bounded_text(text: &str, name: &'static str, max: usize) -> Result<(), ParseError> {
    if text.is_empty()
        || text.len() > max
        || text.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(ParseError::InvalidField(name));
    }
    Ok(())
}
fn validate_symbol(symbol: &str) -> Result<(), ParseError> {
    if symbol.is_empty()
        || symbol.len() > 64
        || !symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err(ParseError::InvalidField("symbol"));
    }
    Ok(())
}
fn asset(value: &Value, name: &'static str) -> Result<String, ParseError> {
    let text = string(value, name)?;
    bounded_text(text, name, 64)?;
    Ok(text.to_owned())
}
fn validate_instrument(instrument: &Instrument) -> Result<(), ParseError> {
    if instrument.venue != Venue::Bybit {
        return Err(ParseError::InstrumentMismatch);
    }
    validate_symbol(&instrument.symbol)?;
    if instrument.market == Market::UsdtPerpetual && !instrument.symbol.ends_with("USDT") {
        return Err(ParseError::InstrumentMismatch);
    }
    Ok(())
}
fn topic_instrument(value: &Value, prefix: &str, market: Market) -> Result<Instrument, ParseError> {
    let symbol = string(value, "topic")?
        .strip_prefix(prefix)
        .ok_or(ParseError::UnsupportedMessage)?;
    let instrument = Instrument {
        venue: Venue::Bybit,
        market,
        symbol: symbol.to_owned(),
    };
    validate_instrument(&instrument)?;
    Ok(instrument)
}
fn rest_result(value: &Value, market: Market) -> Result<&Value, ParseError> {
    if field(value, "retCode")?.as_i64() != Some(0) {
        return Err(ParseError::InvalidField("retCode"));
    }
    let result = field(value, "result")?;
    let expected = match market {
        Market::Spot => "spot",
        Market::UsdtPerpetual => "linear",
    };
    if string(result, "category")? != expected {
        return Err(ParseError::InstrumentMismatch);
    }
    Ok(result)
}
fn match_symbol(value: &Value, instrument: &Instrument) -> Result<(), ParseError> {
    if string(value, "symbol")? != instrument.symbol {
        return Err(ParseError::InstrumentMismatch);
    }
    Ok(())
}
fn cursor(value: &Value) -> Result<Option<String>, ParseError> {
    match value.get("nextPageCursor") {
        None => Ok(None),
        Some(value) => {
            let text = value
                .as_str()
                .ok_or(ParseError::InvalidField("nextPageCursor"))?;
            if text.is_empty() {
                return Ok(None);
            }
            bounded_text(text, "nextPageCursor", 4096)?;
            Ok(Some(text.to_owned()))
        }
    }
}
fn minute_end(start: i64) -> Result<i64, ParseError> {
    if start % 60_000 != 0 {
        return Err(ParseError::InvalidField("minute alignment"));
    }
    start
        .checked_add(60_000)
        .ok_or(ParseError::InvalidField("minute end"))
}
fn validate_candle(candle: &MinuteCandle) -> Result<(), ParseError> {
    if candle.low <= Decimal::ZERO
        || candle.high < candle.low
        || candle.open < candle.low
        || candle.close < candle.low
        || candle.open > candle.high
        || candle.close > candle.high
    {
        return Err(ParseError::InvalidField("OHLC"));
    }
    Ok(())
}
