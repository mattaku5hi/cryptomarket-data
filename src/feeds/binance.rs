//! Binance Spot and USDT perpetual feeds, with millisecond timestamps only.
//! All-market feeds intentionally omit COIN-M, dated and non-USDT contracts.
//! Liquidations are exchange snapshots, not a complete execution ledger.
use super::{
    Funding, Instrument, InstrumentMetadata, Liquidation, Market, MarketEvent, MinuteCandle,
    OpenInterest, ParseError, Side, Venue,
};
use crate::decimal::ExactDecimal;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashSet;

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ITEMS: usize = 10_000;
// Upper bound distinguishes modern microsecond timestamps; permits metadata dates to year 2100.
const MAX_TIME_MS: i64 = 4_102_444_800_000;

/// Parse raw or combined streams. An array is valid only for all-market mark prices.
/// Errors are atomic: malformed in-scope rows never produce a partial result.
pub fn parse_websocket(raw: &[u8], market: Market) -> Result<Vec<MarketEvent>, ParseError> {
    let root = json(raw)?;
    let (data, stream) = if root.get("stream").is_some() || root.get("data").is_some() {
        (field(&root, "data")?, Some(string(&root, "stream")?))
    } else {
        (&root, None)
    };
    if let Some(rows) = data.as_array() {
        if market != Market::UsdtPerpetual || stream.is_some_and(|s| !is_mark_array(s)) {
            return Err(ParseError::UnsupportedMessage);
        }
        bounded(rows)?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if string(row, "e")? != "markPriceUpdate" {
                return Err(ParseError::UnsupportedMessage);
            }
            if in_scope(row, string(row, "s")?)? {
                events.push(MarketEvent::Funding(funding(row, market)?));
            }
        }
        return Ok(events);
    }
    let event_type = string(data, "e")?;
    let symbol = if event_type == "forceOrder" {
        string(field(data, "o")?, "s")?
    } else {
        string(data, "s")?
    };
    validate_symbol(symbol)?;
    if let Some(stream) = stream {
        check_stream(stream, event_type, symbol)?;
    }
    match event_type {
        "kline" => {
            check_product(data, market)?;
            Ok(vec![MarketEvent::Candle(kline(data, market)?)])
        }
        "markPriceUpdate" | "forceOrder" => {
            if market != Market::UsdtPerpetual {
                return Err(ParseError::InstrumentMismatch);
            }
            if !in_scope(data, symbol)? {
                return Ok(Vec::new());
            }
            if event_type == "markPriceUpdate" {
                Ok(vec![MarketEvent::Funding(funding(data, market)?)])
            } else {
                Ok(vec![MarketEvent::Liquidation(liquidation(data, market)?)])
            }
        }
        _ => Err(ParseError::UnsupportedMessage),
    }
}

/// Parse 1m REST klines using the request's instrument and observation timestamp.
/// The final, still-open row remains provisional. Rows must be strictly ascending.
pub fn parse_rest_klines(
    raw: &[u8],
    instrument: &Instrument,
    observed_at_ms: i64,
) -> Result<Vec<MinuteCandle>, ParseError> {
    validate_instrument(instrument)?;
    valid_time(observed_at_ms, "observed_at_ms")?;
    let root = json(raw)?;
    let rows = array(&root, "klines")?;
    if rows.len() > 1500 {
        return Err(ParseError::Limit("klines"));
    }
    let mut result = Vec::with_capacity(rows.len());
    let mut previous = None;
    for row in rows {
        let r = array(row, "kline row")?;
        if r.len() != 12 {
            return Err(ParseError::InvalidField("kline row length"));
        }
        let get = |i| r.get(i).ok_or(ParseError::InvalidField("kline row"));
        let open = time_value(get(0)?, "open time")?;
        let close = time_value(get(6)?, "close time")?;
        validate_minute(open, close)?;
        if previous.is_some_and(|p| open <= p) || open > observed_at_ms {
            return Err(ParseError::InvalidField("kline time order"));
        }
        previous = Some(open);
        if get(8)?.as_u64().is_none() {
            return Err(ParseError::InvalidField("trade count"));
        }
        let candle = MinuteCandle {
            instrument: instrument.clone(),
            open_time_ms: open,
            updated_at_ms: observed_at_ms,
            open: decimal(get(1)?, "open")?,
            high: decimal(get(2)?, "high")?,
            low: decimal(get(3)?, "low")?,
            close: decimal(get(4)?, "close")?,
            base_volume: decimal(get(5)?, "base volume")?,
            quote_volume: decimal(get(7)?, "quote volume")?,
            taker_buy_quote_volume: Some(decimal(get(10)?, "taker quote volume")?),
            is_final: observed_at_ms > close,
        };
        validate_candle(&candle, decimal(get(9)?, "taker base volume")?)?;
        result.push(candle);
    }
    Ok(result)
}

pub fn parse_open_interest(
    raw: &[u8],
    instrument: &Instrument,
) -> Result<OpenInterest, ParseError> {
    validate_instrument(instrument)?;
    if instrument.market != Market::UsdtPerpetual {
        return Err(ParseError::InstrumentMismatch);
    }
    let root = json(raw)?;
    if string(&root, "symbol")? != instrument.symbol {
        return Err(ParseError::InstrumentMismatch);
    }
    let value = nonnegative(
        decimal(field(&root, "openInterest")?, "openInterest")?,
        "openInterest",
    )?;
    Ok(OpenInterest {
        instrument: instrument.clone(),
        time_ms: time(&root, "time")?,
        value,
    })
}

/// Return supported instruments of every status; universe admission belongs to the consumer.
/// Futures rows outside USDT perpetuals are explicitly filtered, not mislabeled.
pub fn parse_instruments(
    raw: &[u8],
    market: Market,
) -> Result<Vec<InstrumentMetadata>, ParseError> {
    let root = json(raw)?;
    let rows = array(field(&root, "symbols")?, "symbols")?;
    bounded(rows)?;
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let symbol = string(row, "symbol")?;
        validate_symbol(symbol)?;
        let base = asset(row, "baseAsset")?;
        let quote = asset(row, "quoteAsset")?;
        let status = string(row, "status")?;
        if status.is_empty() || status.len() > 64 {
            return Err(ParseError::InvalidField("status"));
        }
        let perpetual =
            market == Market::UsdtPerpetual && string(row, "contractType")? == "PERPETUAL";
        let listing_time_ms = optional_time(row, "onboardDate")?;
        // Binance uses 4133404800000 (2100-12-25) for perpetual delivery sentinel.
        let delivery_time_ms = row
            .get("deliveryDate")
            .map(|v| {
                v.as_i64()
                    .filter(|t| *t >= 0 && *t <= 4_133_404_800_000)
                    .ok_or(ParseError::InvalidField("deliveryDate"))
            })
            .transpose()?;
        if market == Market::UsdtPerpetual
            && (!perpetual || quote != "USDT" || asset(row, "marginAsset")? != "USDT")
        {
            continue;
        }
        let instrument = make_instrument(market, symbol)?;
        if !seen.insert(symbol) {
            return Err(ParseError::InvalidField("duplicate symbol"));
        }
        result.push(InstrumentMetadata {
            instrument,
            base_asset: base.into(),
            quote_asset: quote.into(),
            status: status.into(),
            is_perpetual: perpetual,
            listing_time_ms,
            delivery_time_ms,
            max_leverage: None,
        });
    }
    Ok(result)
}

fn kline(v: &Value, market: Market) -> Result<MinuteCandle, ParseError> {
    let instrument = make_instrument(market, string(v, "s")?)?;
    let k = field(v, "k")?;
    if string(k, "s")? != instrument.symbol {
        return Err(ParseError::InstrumentMismatch);
    }
    if string(k, "i")? != "1m" {
        return Err(ParseError::InvalidField("kline interval"));
    }
    let open_time_ms = time(k, "t")?;
    let close_time_ms = time(k, "T")?;
    validate_minute(open_time_ms, close_time_ms)?;
    let candle = MinuteCandle {
        instrument,
        open_time_ms,
        updated_at_ms: time(v, "E")?,
        open: decimal(field(k, "o")?, "open")?,
        high: decimal(field(k, "h")?, "high")?,
        low: decimal(field(k, "l")?, "low")?,
        close: decimal(field(k, "c")?, "close")?,
        base_volume: decimal(field(k, "v")?, "base volume")?,
        quote_volume: decimal(field(k, "q")?, "quote volume")?,
        taker_buy_quote_volume: Some(decimal(field(k, "Q")?, "taker quote volume")?),
        is_final: field(k, "x")?
            .as_bool()
            .ok_or(ParseError::InvalidField("x"))?,
    };
    if candle.updated_at_ms < open_time_ms
        || (candle.is_final && candle.updated_at_ms < close_time_ms)
    {
        return Err(ParseError::InvalidField("event time"));
    }
    validate_candle(&candle, decimal(field(k, "V")?, "taker base volume")?)?;
    Ok(candle)
}
fn funding(v: &Value, market: Market) -> Result<Funding, ParseError> {
    let time_ms = time(v, "E")?;
    let next = time(v, "T")?;
    if next != 0 && next < time_ms {
        return Err(ParseError::InvalidField("next funding time"));
    }
    Ok(Funding {
        instrument: make_instrument(market, string(v, "s")?)?,
        time_ms,
        rate: decimal(field(v, "r")?, "funding rate")?,
        // T=0 is the exchange sentinel for no scheduled funding.
        next_funding_time_ms: (next != 0).then_some(next),
        interval_minutes: None,
        mark_price: Some(positive(
            decimal(field(v, "p")?, "mark price")?,
            "mark price",
        )?),
        index_price: Some(positive(
            decimal(field(v, "i")?, "index price")?,
            "index price",
        )?),
    })
}
fn liquidation(v: &Value, market: Market) -> Result<Liquidation, ParseError> {
    let o = field(v, "o")?;
    let time_ms = time(o, "T")?;
    if time_ms > time(v, "E")? {
        return Err(ParseError::InvalidField("liquidation time"));
    }
    let quantity = nonnegative(
        decimal(field(o, "z")?, "filled quantity")?,
        "filled quantity",
    )?;
    let original = nonnegative(
        decimal(field(o, "q")?, "original quantity")?,
        "original quantity",
    )?;
    if quantity > original {
        return Err(ParseError::InvalidField("filled quantity"));
    }
    let price = nonnegative(decimal(field(o, "ap")?, "average price")?, "average price")?;
    if quantity > Decimal::ZERO && price == Decimal::ZERO {
        return Err(ParseError::InvalidField("average price"));
    }
    Ok(Liquidation {
        instrument: make_instrument(market, string(o, "s")?)?,
        time_ms,
        side: match string(o, "S")? {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return Err(ParseError::InvalidField("side")),
        },
        price,
        quantity,
        id: None,
    })
}
fn in_scope(v: &Value, symbol: &str) -> Result<bool, ParseError> {
    validate_symbol(symbol)?;
    let st = v
        .get("st")
        .map(|x| x.as_u64().ok_or(ParseError::InvalidField("st")))
        .transpose()?;
    if st.is_some_and(|x| x != 1 && x != 2) {
        return Err(ParseError::InvalidField("st"));
    }
    if let Some(pair) = v.get("ps") {
        let pair = pair.as_str().ok_or(ParseError::InvalidField("ps"))?;
        validate_symbol(pair)?;
        if st != Some(2) && symbol.ends_with("USDT") && pair != symbol {
            return Err(ParseError::InstrumentMismatch);
        }
    }
    Ok(st != Some(2) && symbol.ends_with("USDT"))
}
fn check_product(v: &Value, market: Market) -> Result<(), ParseError> {
    if let Some(st) = v.get("st") {
        if market != Market::UsdtPerpetual || st.as_u64() != Some(1) {
            return Err(ParseError::InstrumentMismatch);
        }
    }
    Ok(())
}
fn is_mark_array(s: &str) -> bool {
    matches!(
        s,
        "!markPrice@arr" | "!markPrice@arr@1s" | "!markPrice@arr@3s"
    )
}
fn check_stream(stream: &str, event: &str, symbol: &str) -> Result<(), ParseError> {
    let lower = symbol.to_lowercase();
    let valid = match event {
        "kline" => stream == format!("{lower}@kline_1m"),
        "markPriceUpdate" => {
            stream == format!("{lower}@markPrice")
                || stream == format!("{lower}@markPrice@1s")
                || stream == format!("{lower}@markPrice@3s")
        }
        "forceOrder" => stream == "!forceOrder@arr" || stream == format!("{lower}@forceOrder"),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ParseError::InstrumentMismatch)
    }
}
fn validate_candle(c: &MinuteCandle, taker_base: Decimal) -> Result<(), ParseError> {
    if c.low <= Decimal::ZERO
        || c.low > c.open
        || c.low > c.close
        || c.high < c.open
        || c.high < c.close
    {
        return Err(ParseError::InvalidField("OHLC"));
    }
    nonnegative(c.base_volume, "base volume")?;
    nonnegative(c.quote_volume, "quote volume")?;
    nonnegative(taker_base, "taker base volume")?;
    if taker_base > c.base_volume
        || c.taker_buy_quote_volume
            .is_some_and(|q| q < Decimal::ZERO || q > c.quote_volume)
    {
        return Err(ParseError::InvalidField("taker volume"));
    }
    Ok(())
}
fn validate_minute(open: i64, close: i64) -> Result<(), ParseError> {
    if open % 60_000 != 0 || open.checked_add(59_999) != Some(close) {
        return Err(ParseError::InvalidField("1m time bounds"));
    }
    Ok(())
}
fn make_instrument(market: Market, symbol: &str) -> Result<Instrument, ParseError> {
    let instrument = Instrument {
        venue: Venue::Binance,
        market,
        symbol: symbol.into(),
    };
    validate_instrument(&instrument)?;
    Ok(instrument)
}
fn validate_instrument(i: &Instrument) -> Result<(), ParseError> {
    validate_symbol(&i.symbol)?;
    if i.venue != Venue::Binance
        || (i.market == Market::UsdtPerpetual && !i.symbol.ends_with("USDT"))
    {
        return Err(ParseError::InstrumentMismatch);
    }
    Ok(())
}
fn validate_symbol(s: &str) -> Result<(), ParseError> {
    if s.is_empty()
        || s.len() > 128
        || !s.chars().all(|c| c.is_alphanumeric() || c == '_')
        || s.chars().any(char::is_lowercase)
    {
        return Err(ParseError::InvalidField("symbol"));
    }
    Ok(())
}
fn json(raw: &[u8]) -> Result<Value, ParseError> {
    crate::json::parse(raw, MAX_BYTES).map_err(|e| match e {
        crate::json::Error::Limit => ParseError::Limit("payload bytes"),
        _ => ParseError::MalformedJson,
    })
}
fn bounded(rows: &[Value]) -> Result<(), ParseError> {
    if rows.len() > MAX_ITEMS {
        Err(ParseError::Limit("rows"))
    } else {
        Ok(())
    }
}
fn field<'a>(v: &'a Value, key: &'static str) -> Result<&'a Value, ParseError> {
    v.get(key).ok_or(ParseError::InvalidField(key))
}
fn string<'a>(v: &'a Value, key: &'static str) -> Result<&'a str, ParseError> {
    field(v, key)?.as_str().ok_or(ParseError::InvalidField(key))
}
fn asset<'a>(v: &'a Value, key: &'static str) -> Result<&'a str, ParseError> {
    let s = string(v, key)?;
    validate_symbol(s)?;
    Ok(s)
}
fn array<'a>(v: &'a Value, key: &'static str) -> Result<&'a Vec<Value>, ParseError> {
    v.as_array().ok_or(ParseError::InvalidField(key))
}
fn decimal(v: &Value, key: &'static str) -> Result<Decimal, ParseError> {
    ExactDecimal::parse(v.as_str().ok_or(ParseError::InvalidField(key))?)
        .map(|d| d.value())
        .map_err(|_| ParseError::InvalidField(key))
}
fn nonnegative(d: Decimal, key: &'static str) -> Result<Decimal, ParseError> {
    if d < Decimal::ZERO {
        Err(ParseError::InvalidField(key))
    } else {
        Ok(d)
    }
}
fn positive(d: Decimal, key: &'static str) -> Result<Decimal, ParseError> {
    if d <= Decimal::ZERO {
        Err(ParseError::InvalidField(key))
    } else {
        Ok(d)
    }
}
fn valid_time(t: i64, key: &'static str) -> Result<i64, ParseError> {
    if !(0..=MAX_TIME_MS).contains(&t) {
        Err(ParseError::InvalidField(key))
    } else {
        Ok(t)
    }
}
fn time_value(v: &Value, key: &'static str) -> Result<i64, ParseError> {
    valid_time(v.as_i64().ok_or(ParseError::InvalidField(key))?, key)
}
fn time(v: &Value, key: &'static str) -> Result<i64, ParseError> {
    time_value(field(v, key)?, key)
}
fn optional_time(v: &Value, key: &'static str) -> Result<Option<i64>, ParseError> {
    v.get(key).map(|v| time_value(v, key)).transpose()
}
