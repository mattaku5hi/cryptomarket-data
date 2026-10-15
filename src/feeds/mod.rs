//! Exchange-neutral public feeds. Prices and quantities retain exact decimal values.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod binance;
pub mod bybit;
#[cfg(feature = "network")]
pub mod network;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Venue {
    Binance,
    Bybit,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Market {
    Spot,
    UsdtPerpetual,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub venue: Venue,
    pub market: Market,
    pub symbol: String,
}
/// Fixed UTC intervals, ordered by duration. Weeks begin Monday at 00:00 UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CandleInterval {
    M1,
    M5,
    M15,
    M30,
    H1,
    H4,
    D1,
    W1,
}
impl CandleInterval {
    pub const fn duration_ms(self) -> i64 {
        match self {
            Self::M1 => 60_000,
            Self::M5 => 300_000,
            Self::M15 => 900_000,
            Self::M30 => 1_800_000,
            Self::H1 => 3_600_000,
            Self::H4 => 14_400_000,
            Self::D1 => 86_400_000,
            Self::W1 => 604_800_000,
        }
    }

    /// Inclusive UTC start of the containing bucket. Supports times before Unix epoch;
    /// saturates at i64::MIN only when the bucket start is outside the timestamp range.
    pub fn bucket_start_ms(self, timestamp_ms: i64) -> i64 {
        let duration = self.duration_ms();
        // Unix epoch was Thursday, three days after the preceding Monday.
        let offset = if self == Self::W1 { 259_200_000 } else { 0 };
        let elapsed = (timestamp_ms.rem_euclid(duration) + offset).rem_euclid(duration);
        timestamp_ms.saturating_sub(elapsed)
    }

    pub const fn binance_name(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
            Self::W1 => "1w",
        }
    }

    pub const fn bybit_name(self) -> &'static str {
        match self {
            Self::M1 => "1",
            Self::M5 => "5",
            Self::M15 => "15",
            Self::M30 => "30",
            Self::H1 => "60",
            Self::H4 => "240",
            Self::D1 => "D",
            Self::W1 => "W",
        }
    }

    pub(crate) fn end_ms(self, start: i64) -> Result<i64, ParseError> {
        if start < 0 || self.bucket_start_ms(start) != start {
            return Err(ParseError::InvalidField("candle alignment"));
        }
        start
            .checked_add(self.duration_ms())
            .ok_or(ParseError::InvalidField("candle end"))
    }
}

/// An interval-aware REST candle. `updated_at_ms` is the supplied observation time;
/// `is_final` becomes true at the next interval boundary, without estimating buy volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalCandle {
    pub instrument: Instrument,
    pub interval: CandleInterval,
    pub open_time_ms: i64,
    pub updated_at_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub base_volume: Decimal,
    pub quote_volume: Decimal,
    pub taker_buy_quote_volume: Option<Decimal>,
    pub is_final: bool,
}
impl HistoricalCandle {
    pub(crate) fn from_values(candle: MinuteCandle, interval: CandleInterval) -> Self {
        Self {
            instrument: candle.instrument,
            interval,
            open_time_ms: candle.open_time_ms,
            updated_at_ms: candle.updated_at_ms,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            base_volume: candle.base_volume,
            quote_volume: candle.quote_volume,
            taker_buy_quote_volume: candle.taker_buy_quote_volume,
            is_final: candle.is_final,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinuteCandle {
    pub instrument: Instrument,
    pub open_time_ms: i64,
    pub updated_at_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub base_volume: Decimal,
    pub quote_volume: Decimal,
    pub taker_buy_quote_volume: Option<Decimal>,
    pub is_final: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenInterest {
    pub instrument: Instrument,
    pub time_ms: i64,
    /// Base asset quantity in the exchange contract's denomination, never quote notional.
    pub value: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Funding {
    pub instrument: Instrument,
    pub time_ms: i64,
    /// Fraction per funding interval (0.0001 means 0.01%).
    pub rate: Decimal,
    pub next_funding_time_ms: Option<i64>,
    pub interval_minutes: Option<u32>,
    pub mark_price: Option<Decimal>,
    pub index_price: Option<Decimal>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Liquidation {
    pub instrument: Instrument,
    pub time_ms: i64,
    /// Executed order side, not the side of the liquidated position.
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub instrument: Instrument,
    pub time_ms: i64,
    pub id: String,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketEvent {
    Candle(MinuteCandle),
    OpenInterest(OpenInterest),
    Funding(Funding),
    Liquidation(Liquidation),
    Trade(Trade),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentMetadata {
    pub instrument: Instrument,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub is_perpetual: bool,
    pub listing_time_ms: Option<i64>,
    pub delivery_time_ms: Option<i64>,
    pub max_leverage: Option<Decimal>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    MalformedJson,
    InvalidField(&'static str),
    UnsupportedMessage,
    InstrumentMismatch,
    Limit(&'static str),
}
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedJson => f.write_str("malformed or duplicate-key feed JSON"),
            Self::InvalidField(field) => write!(f, "invalid feed field: {field}"),
            Self::UnsupportedMessage => f.write_str("unsupported feed message"),
            Self::InstrumentMismatch => f.write_str("feed instrument mismatch"),
            Self::Limit(field) => write!(f, "feed limit exceeded: {field}"),
        }
    }
}
impl std::error::Error for ParseError {}
