//! Bounded, pure parsers for the symbol-bound Binance public market-data messages.
//!
//! Parsing functions never connect or establish source continuity. [`collector`] provides
//! optional finite public collection; [`book`] validates bounded snapshot/update sequences.

use crate::decimal::ExactDecimal;
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use std::{collections::HashSet, fmt};

const USD_M_MARKET_TYPE: u64 = 1;
const MAX_INPUT_BYTES: usize = 1_048_576;
const MAX_LEVELS_PER_SIDE: usize = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketKind {
    Spot,
    UsdMPerpetual,
}

/// Explicit Binance product and symbol; symbol existence is checked by the caller/exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub market: MarketKind,
    pub symbol: Symbol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symbol(String);
impl Symbol {
    /// Preserve case and Unicode. Callers must use the exchange's canonical symbol.
    pub fn new(value: impl Into<String>) -> Result<Self, ParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            return Err(ParseError::InstrumentMismatch);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampUnit {
    Milliseconds,
    Microseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderTimestamp {
    pub value: u64,
    pub unit: TimestampUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    pub max_bytes: usize,
    pub max_levels_per_side: usize,
}

impl Default for ParseLimits {
    fn default() -> Self {
        Self {
            max_bytes: MAX_INPUT_BYTES,
            max_levels_per_side: MAX_LEVELS_PER_SIDE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggTrade {
    pub instrument: Instrument,
    pub aggregate_trade_id: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub price: ExactDecimal,
    /// Binance `q`: aggregate quantity (includes RPI quantity on USD-M).
    pub quantity: ExactDecimal,
    /// Binance USD-M `nq`, which excludes RPI quantity.
    pub non_rpi_quantity: Option<ExactDecimal>,
    /// True means the buyer was the maker; the seller was the aggressor.
    pub buyer_is_maker: bool,
    pub event_time: ProviderTimestamp,
    pub trade_time: ProviderTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceLevel {
    pub price: ExactDecimal,
    /// Absolute level quantity. Zero is a delete instruction in depth updates.
    pub quantity: ExactDecimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthUpdate {
    pub instrument: Instrument,
    pub event_time: ProviderTimestamp,
    pub transaction_time: Option<ProviderTimestamp>,
    pub first_update_id: u64,
    pub final_update_id: u64,
    /// USD-M only; previous stream event final update ID.
    pub previous_final_update_id: Option<u64>,
    /// Absolute bid level sizes in wire order; zero quantity removes that price.
    pub bids: Vec<PriceLevel>,
    /// Absolute ask level sizes in wire order; zero quantity removes that price.
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshot {
    pub instrument: Instrument,
    pub last_update_id: u64,
    pub event_time: Option<ProviderTimestamp>,
    pub transaction_time: Option<ProviderTimestamp>,
    /// Snapshot levels preserve Binance's bid-descending wire order.
    pub bids: Vec<PriceLevel>,
    /// Snapshot levels preserve Binance's ask-ascending wire order.
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsEvent {
    AggTrade(AggTrade),
    DepthUpdate(DepthUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Json(String),
    WrongShape(&'static str),
    MissingField(&'static str),
    InvalidField(&'static str),
    UnsupportedMessage,
    InstrumentMismatch,
    UnsupportedTimestampUnit,
    InvalidBounds,
    Limit(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(message) => write!(f, "invalid Binance JSON: {message}"),
            Self::WrongShape(message) => write!(f, "invalid Binance message shape: {message}"),
            Self::MissingField(field) => write!(f, "missing Binance field {field}"),
            Self::InvalidField(field) => write!(f, "invalid Binance field {field}"),
            Self::UnsupportedMessage => f.write_str("unsupported Binance message type"),
            Self::InstrumentMismatch => f.write_str("Binance message instrument mismatch"),
            Self::UnsupportedTimestampUnit => {
                f.write_str("timestamp unit is unsupported for this market message")
            }
            Self::InvalidBounds => f.write_str("invalid Binance parser bounds"),
            Self::Limit(message) => write!(f, "Binance parser limit exceeded: {message}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses a single Binance public WebSocket JSON event.
///
/// `timestamp_unit` must match the explicit stream configuration. USD-M
/// currently uses millisecond timestamps and rejects another unit.
pub fn parse_websocket(
    raw: &[u8],
    instrument: Instrument,
    timestamp_unit: TimestampUnit,
    limits: ParseLimits,
) -> Result<WsEvent, ParseError> {
    let value = parse_json(raw, limits)?;
    let object = as_object(&value, "event must be an object")?;
    match required_str(object, "e")? {
        "aggTrade" => parse_agg_trade(object, instrument, timestamp_unit).map(WsEvent::AggTrade),
        "depthUpdate" => {
            parse_depth_update(object, instrument, timestamp_unit, limits).map(WsEvent::DepthUpdate)
        }
        _ => Err(ParseError::UnsupportedMessage),
    }
}

/// Parses one REST depth snapshot. Identity comes from the explicit caller
/// context because Binance snapshot payloads do not echo the symbol.
pub fn parse_rest_depth_snapshot(
    raw: &[u8],
    instrument: Instrument,
    limits: ParseLimits,
) -> Result<DepthSnapshot, ParseError> {
    let value = parse_json(raw, limits)?;
    let object = as_object(&value, "snapshot must be an object")?;
    if let Some(symbol) = optional_str(object, "symbol")? {
        check_symbol(symbol, &instrument)?;
    }
    let (event_time, transaction_time) = match instrument.market {
        MarketKind::Spot => (None, None),
        MarketKind::UsdMPerpetual => (
            Some(timestamp(object, "E", TimestampUnit::Milliseconds)?),
            Some(timestamp(object, "T", TimestampUnit::Milliseconds)?),
        ),
    };
    let bids = parse_levels(required(object, "bids")?, limits, "bids", false)?;
    let asks = parse_levels(required(object, "asks")?, limits, "asks", false)?;
    validate_snapshot_order(&bids, &asks)?;
    Ok(DepthSnapshot {
        instrument,
        last_update_id: uint(object, "lastUpdateId")?,
        event_time,
        transaction_time,
        bids,
        asks,
    })
}

fn parse_agg_trade(
    object: &Map<String, Value>,
    instrument: Instrument,
    unit: TimestampUnit,
) -> Result<AggTrade, ParseError> {
    check_timestamp_unit(&instrument, unit)?;
    check_symbol(required_str(object, "s")?, &instrument)?;
    check_market_type(object, &instrument)?;
    if instrument.market == MarketKind::Spot && object.contains_key("nq") {
        return Err(ParseError::InstrumentMismatch);
    }
    let price = positive_decimal(object, "p")?;
    let quantity = positive_decimal(object, "q")?;
    let non_rpi_quantity = match instrument.market {
        MarketKind::Spot => None,
        MarketKind::UsdMPerpetual => Some(nonnegative_decimal_value(required(object, "nq")?)?),
    };
    if non_rpi_quantity
        .as_ref()
        .is_some_and(|non_rpi| non_rpi.value() > quantity.value())
    {
        return Err(ParseError::InvalidField("nq"));
    }
    let first_trade_id = uint(object, "f")?;
    let last_trade_id = uint(object, "l")?;
    if first_trade_id > last_trade_id {
        return Err(ParseError::InvalidField("f/l"));
    }
    Ok(AggTrade {
        instrument,
        aggregate_trade_id: uint(object, "a")?,
        first_trade_id,
        last_trade_id,
        price,
        quantity,
        non_rpi_quantity,
        buyer_is_maker: bool_field(object, "m")?,
        event_time: timestamp(object, "E", unit)?,
        trade_time: timestamp(object, "T", unit)?,
    })
}

fn parse_depth_update(
    object: &Map<String, Value>,
    instrument: Instrument,
    unit: TimestampUnit,
    limits: ParseLimits,
) -> Result<DepthUpdate, ParseError> {
    check_timestamp_unit(&instrument, unit)?;
    check_symbol(required_str(object, "s")?, &instrument)?;
    check_market_type(object, &instrument)?;
    match instrument.market {
        MarketKind::Spot => {
            if object.contains_key("ps") || object.contains_key("pu") {
                return Err(ParseError::InstrumentMismatch);
            }
        }
        MarketKind::UsdMPerpetual => check_symbol(required_str(object, "ps")?, &instrument)?,
    }
    let first_update_id = uint(object, "U")?;
    let final_update_id = uint(object, "u")?;
    if first_update_id > final_update_id {
        return Err(ParseError::InvalidField("U/u"));
    }
    let (transaction_time, previous_final_update_id) = match instrument.market {
        MarketKind::Spot => (None, None),
        MarketKind::UsdMPerpetual => (
            Some(timestamp(object, "T", unit)?),
            Some(uint(object, "pu")?),
        ),
    };
    let bids = parse_levels(required(object, "b")?, limits, "b", true)?;
    let asks = parse_levels(required(object, "a")?, limits, "a", true)?;
    Ok(DepthUpdate {
        instrument,
        event_time: timestamp(object, "E", unit)?,
        transaction_time,
        first_update_id,
        final_update_id,
        previous_final_update_id,
        bids,
        asks,
    })
}

fn parse_json(raw: &[u8], limits: ParseLimits) -> Result<Value, ParseError> {
    if limits.max_bytes == 0
        || limits.max_bytes > MAX_INPUT_BYTES
        || limits.max_levels_per_side > MAX_LEVELS_PER_SIDE
    {
        return Err(ParseError::InvalidBounds);
    }
    crate::json::parse(raw, limits.max_bytes).map_err(|error| match error {
        crate::json::Error::Limit => ParseError::Limit("message bytes"),
        crate::json::Error::InvalidBounds => ParseError::InvalidBounds,
        crate::json::Error::Malformed => ParseError::Json("malformed or duplicate-key JSON".into()),
    })
}

fn check_timestamp_unit(instrument: &Instrument, unit: TimestampUnit) -> Result<(), ParseError> {
    if instrument.market == MarketKind::UsdMPerpetual && unit != TimestampUnit::Milliseconds {
        return Err(ParseError::UnsupportedTimestampUnit);
    }
    Ok(())
}

fn check_market_type(
    object: &Map<String, Value>,
    instrument: &Instrument,
) -> Result<(), ParseError> {
    match instrument.market {
        MarketKind::Spot => {
            if object.contains_key("st") {
                Err(ParseError::InstrumentMismatch)
            } else {
                Ok(())
            }
        }
        MarketKind::UsdMPerpetual => {
            if uint(object, "st")? == USD_M_MARKET_TYPE {
                Ok(())
            } else {
                Err(ParseError::InstrumentMismatch)
            }
        }
    }
}

fn check_symbol(symbol: &str, instrument: &Instrument) -> Result<(), ParseError> {
    if symbol == instrument.symbol.as_str() {
        Ok(())
    } else {
        Err(ParseError::InstrumentMismatch)
    }
}

fn parse_levels(
    value: &Value,
    limits: ParseLimits,
    field: &'static str,
    allow_zero_quantity: bool,
) -> Result<Vec<PriceLevel>, ParseError> {
    let levels = value.as_array().ok_or(ParseError::InvalidField(field))?;
    if levels.len() > limits.max_levels_per_side {
        return Err(ParseError::Limit("levels per side"));
    }
    let mut parsed = Vec::with_capacity(levels.len());
    for item in levels {
        let pair = item
            .as_array()
            .ok_or(ParseError::WrongShape("level must be a pair"))?;
        if pair.len() != 2 {
            return Err(ParseError::WrongShape(
                "level must contain price and quantity",
            ));
        }
        let price = positive_decimal_value(
            pair.first()
                .ok_or(ParseError::WrongShape("missing level price"))?,
        )?;
        let quantity = nonnegative_decimal_value(
            pair.get(1)
                .ok_or(ParseError::WrongShape("missing level quantity"))?,
        )?;
        if !allow_zero_quantity && quantity.value() == Decimal::ZERO {
            return Err(ParseError::InvalidField("snapshot quantity"));
        }
        parsed.push(PriceLevel { price, quantity });
    }
    reject_duplicate_prices(&parsed)?;
    Ok(parsed)
}

fn validate_snapshot_order(bids: &[PriceLevel], asks: &[PriceLevel]) -> Result<(), ParseError> {
    if bids.windows(2).any(|pair| {
        pair.first()
            .zip(pair.get(1))
            .is_some_and(|(left, right)| left.price.value() < right.price.value())
    }) {
        return Err(ParseError::InvalidField("bids order"));
    }
    if asks.windows(2).any(|pair| {
        pair.first()
            .zip(pair.get(1))
            .is_some_and(|(left, right)| left.price.value() > right.price.value())
    }) {
        return Err(ParseError::InvalidField("asks order"));
    }
    Ok(())
}

fn reject_duplicate_prices(levels: &[PriceLevel]) -> Result<(), ParseError> {
    let mut seen = HashSet::with_capacity(levels.len());
    for level in levels {
        if !seen.insert(level.price.value()) {
            return Err(ParseError::InvalidField("duplicate level price"));
        }
    }
    Ok(())
}

fn positive_decimal(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<ExactDecimal, ParseError> {
    positive_decimal_value(required(object, field)?)
}

fn positive_decimal_value(value: &Value) -> Result<ExactDecimal, ParseError> {
    let decimal = decimal_value(value)?;
    if decimal.value() <= Decimal::ZERO {
        return Err(ParseError::InvalidField("positive decimal"));
    }
    Ok(decimal)
}

fn nonnegative_decimal_value(value: &Value) -> Result<ExactDecimal, ParseError> {
    let decimal = decimal_value(value)?;
    if decimal.value() < Decimal::ZERO {
        return Err(ParseError::InvalidField("nonnegative decimal"));
    }
    Ok(decimal)
}

fn decimal_value(value: &Value) -> Result<ExactDecimal, ParseError> {
    let value = value
        .as_str()
        .ok_or(ParseError::InvalidField("decimal string"))?;
    ExactDecimal::parse(value).map_err(|_| ParseError::InvalidField("exact decimal"))
}

fn timestamp(
    object: &Map<String, Value>,
    field: &'static str,
    unit: TimestampUnit,
) -> Result<ProviderTimestamp, ParseError> {
    Ok(ProviderTimestamp {
        value: uint(object, field)?,
        unit,
    })
}

fn uint(object: &Map<String, Value>, field: &'static str) -> Result<u64, ParseError> {
    required(object, field)?
        .as_u64()
        .ok_or(ParseError::InvalidField(field))
}

fn bool_field(object: &Map<String, Value>, field: &'static str) -> Result<bool, ParseError> {
    required(object, field)?
        .as_bool()
        .ok_or(ParseError::InvalidField(field))
}

fn required<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a Value, ParseError> {
    object.get(field).ok_or(ParseError::MissingField(field))
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, ParseError> {
    required(object, field)?
        .as_str()
        .ok_or(ParseError::InvalidField(field))
}

fn optional_str<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<Option<&'a str>, ParseError> {
    object
        .get(field)
        .map(|value| value.as_str().ok_or(ParseError::InvalidField(field)))
        .transpose()
}

fn as_object<'a>(
    value: &'a Value,
    message: &'static str,
) -> Result<&'a Map<String, Value>, ParseError> {
    value.as_object().ok_or(ParseError::WrongShape(message))
}
