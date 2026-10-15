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
