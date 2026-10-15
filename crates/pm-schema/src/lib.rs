#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub mod exact_decimal;
pub mod forecast;
pub mod json;
pub mod predictor_health;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaError(pub String);
impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SchemaError {}
fn err(s: &str) -> SchemaError {
    SchemaError(s.into())
}
pub fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactDecimal(Decimal);
impl ExactDecimal {
    pub fn parse(s: &str) -> Result<Self, SchemaError> {
        if s.is_empty() {
            return Err(err("empty decimal"));
        }
        let bytes = s.as_bytes();
        let mut i = usize::from(bytes.first() == Some(&b'-'));
        if i == bytes.len() {
            return Err(err("invalid decimal"));
        }
        let integer_start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == integer_start || (i - integer_start > 1 && bytes.get(integer_start) == Some(&b'0'))
        {
            return Err(err("invalid integer digits"));
        }
        if i < bytes.len() {
            if bytes.get(i) != Some(&b'.') {
                return Err(err("invalid decimal character"));
            }
            i += 1;
            let fraction_start = i;
            while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            let fractional_digits = i - fraction_start;
            if fractional_digits == 0 || fractional_digits > 18 {
                return Err(err("invalid fractional precision"));
            }
        }
        if i != bytes.len() {
            return Err(err("invalid decimal character"));
        }
        let value = Decimal::from_str_exact(s)
            .map_err(|_| err("decimal outside supported exact representation"))?;
        Ok(Self(value))
    }
    pub fn value(&self) -> Decimal {
        self.0
    }
    pub fn as_string(&self) -> String {
        self.0.to_string()
    }
}
impl Serialize for ExactDecimal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.as_string())
    }
}
impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryPrice(ExactDecimal);
impl BinaryPrice {
    pub fn new(v: ExactDecimal) -> Result<Self, SchemaError> {
        if v.0 < Decimal::ZERO || v.0 > Decimal::ONE {
            return Err(err("binary price outside [0,1]"));
        }
        Ok(Self(v))
    }
    pub fn value(&self) -> &ExactDecimal {
        &self.0
    }
}
impl Serialize for BinaryPrice {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}
impl<'de> Deserialize<'de> for BinaryPrice {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(ExactDecimal::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonnegativeShares(ExactDecimal);
impl NonnegativeShares {
    pub fn new(v: ExactDecimal) -> Result<Self, SchemaError> {
        if v.0 < Decimal::ZERO {
            return Err(err("negative shares"));
        }
        Ok(Self(v))
    }
    pub fn value(&self) -> &ExactDecimal {
        &self.0
    }
}
impl Serialize for NonnegativeShares {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}
impl<'de> Deserialize<'de> for NonnegativeShares {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(ExactDecimal::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositiveReferencePrice(ExactDecimal);
impl PositiveReferencePrice {
    pub fn new(v: ExactDecimal) -> Result<Self, SchemaError> {
        if v.0 <= Decimal::ZERO {
            return Err(err("reference price must be positive"));
        }
        Ok(Self(v))
    }
    pub fn value(&self) -> &ExactDecimal {
        &self.0
    }
}
impl Serialize for PositiveReferencePrice {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}
impl<'de> Deserialize<'de> for PositiveReferencePrice {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(ExactDecimal::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Horizon {
    FiveMinutes,
    FifteenMinutes,
}
impl Horizon {
    fn seconds(self) -> i64 {
        match self {
            Self::FiveMinutes => 300,
            Self::FifteenMinutes => 900,
        }
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeSide {
    Up,
    Down,
}
pub fn reference_comparison(
    final_reference: PositiveReferencePrice,
    opening_reference: PositiveReferencePrice,
) -> OutcomeSide {
    if final_reference.0.0 >= opening_reference.0.0 {
        OutcomeSide::Up
    } else {
        OutcomeSide::Down
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketPhase {
    PreWindow,
    InWindow,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSpec {
    pub market_id: String,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub horizon: Horizon,
    pub listed_at: DateTime<Utc>,
    pub interval_start: DateTime<Utc>,
    pub interval_end: DateTime<Utc>,
    pub trading_close: DateTime<Utc>,
    pub rule_text: String,
    pub rule_hash: String,
    pub resolution_source: String,
    pub feed_id: String,
    pub window_seconds: u32,
    pub resolver_version: String,
    pub platform_regime_id: String,
    pub first_available_at: DateTime<Utc>,
}
#[derive(Deserialize)]
struct MarketSpecWire {
    market_id: String,
    condition_id: String,
    up_token_id: String,
    down_token_id: String,
    horizon: Horizon,
    listed_at: DateTime<Utc>,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    trading_close: DateTime<Utc>,
    rule_text: String,
    rule_hash: String,
    resolution_source: String,
    feed_id: String,
    window_seconds: u32,
    resolver_version: String,
    platform_regime_id: String,
    first_available_at: DateTime<Utc>,
}
impl MarketSpec {
    pub fn validate(&self) -> Result<(), SchemaError> {
        for (name, v) in [
            ("market_id", &self.market_id),
            ("resolution_source", &self.resolution_source),
            ("feed_id", &self.feed_id),
            ("resolver_version", &self.resolver_version),
            ("platform_regime_id", &self.platform_regime_id),
            ("rule_text", &self.rule_text),
        ] {
            if v.trim().is_empty() {
                return Err(err(name));
            }
        }
        if self.condition_id.len() != 66
            || !self.condition_id.starts_with("0x")
            || self.condition_id[2..]
                .bytes()
                .any(|b| !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b))
            || hex::decode(&self.condition_id[2..]).map_or(true, |x| x.len() != 32)
        {
            return Err(err("condition id must be canonical lowercase 32-byte hex"));
        }
        if !valid_uint256(&self.up_token_id)
            || !valid_uint256(&self.down_token_id)
            || self.up_token_id == self.down_token_id
        {
            return Err(err("invalid or duplicate uint256 outcome token id"));
        }
        if self.listed_at > self.interval_start
            || self.interval_end - self.interval_start != Duration::seconds(self.horizon.seconds())
            || self.trading_close <= self.interval_start
            || self.trading_close > self.interval_end
        {
            return Err(err("invalid market interval or trading close"));
        }
        if sha256(self.rule_text.as_bytes()) != self.rule_hash {
            return Err(err("rule hash mismatch"));
        }
        Ok(())
    }
    pub fn validate_against(&self, b: &SupportedRule) -> Result<(), SchemaError> {
        self.validate()?;
        if self.rule_hash != b.expected_rule_hash
            || self.resolution_source != b.resolution_source
            || self.horizon != b.horizon
            || self.feed_id != b.feed_id
            || self.window_seconds != b.window_seconds
            || self.resolver_version != b.resolver_version
        {
            return Err(err("unsupported rule binding"));
        }
        Ok(())
    }
    pub fn phase_at(&self, decision: DateTime<Utc>) -> Result<MarketPhase, SchemaError> {
        self.validate()?;
        if decision < self.listed_at {
            return Err(err("market not listed"));
        }
        if decision < self.first_available_at {
            return Err(err("market metadata unavailable"));
        }
        if decision < self.interval_start {
            Ok(MarketPhase::PreWindow)
        } else if decision < self.trading_close {
            Ok(MarketPhase::InWindow)
        } else {
            Ok(MarketPhase::Closed)
        }
    }
    pub fn visible_at(&self, decision: DateTime<Utc>) -> bool {
        self.validate().is_ok() && self.first_available_at <= decision
    }
    pub fn in_window_at(&self, decision: DateTime<Utc>) -> Result<bool, SchemaError> {
        Ok(self.phase_at(decision)? == MarketPhase::InWindow)
    }
}
fn valid_uint256(v: &str) -> bool {
    if v.is_empty() || (v.len() > 1 && v.starts_with('0')) || !v.bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    v.len() < max.len() || (v.len() == max.len() && v <= max)
}
impl<'de> Deserialize<'de> for MarketSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = MarketSpecWire::deserialize(d)?;
        let s = Self {
            market_id: w.market_id,
            condition_id: w.condition_id,
            up_token_id: w.up_token_id,
            down_token_id: w.down_token_id,
            horizon: w.horizon,
            listed_at: w.listed_at,
            interval_start: w.interval_start,
            interval_end: w.interval_end,
            trading_close: w.trading_close,
            rule_text: w.rule_text,
            rule_hash: w.rule_hash,
            resolution_source: w.resolution_source,
            feed_id: w.feed_id,
            window_seconds: w.window_seconds,
            resolver_version: w.resolver_version,
            platform_regime_id: w.platform_regime_id,
            first_available_at: w.first_available_at,
        };
        s.validate().map_err(serde::de::Error::custom)?;
        Ok(s)
    }
}
#[derive(Debug, Clone)]
pub struct SupportedRule {
    pub expected_rule_hash: String,
    pub resolution_source: String,
    pub horizon: Horizon,
    pub feed_id: String,
    pub window_seconds: u32,
    pub resolver_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Snapshot,
    Update,
    Gap,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClockStatus {
    Unknown,
    Uncertain,
    Healthy {
        offset_nanos: i64,
        uncertainty_nanos: u64,
    },
}
#[derive(Debug, Clone, Serialize)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub source: String,
    pub stream: String,
    pub collector_id: String,
    pub collector_region: String,
    pub connection_id: String,
    pub collector_session_id: String,
    pub ingest_sequence: u64,
    pub source_sequence: Option<u64>,
    pub source_event_time: Option<DateTime<Utc>>,
    pub source_publish_time: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub monotonic_elapsed_nanos: u64,
    pub provider_time_unit: Option<String>,
    pub kind: EventKind,
    pub raw_payload: Vec<u8>,
    pub quality_flags: Vec<String>,
    pub payload_sha256: String,
    pub clock_status: ClockStatus,
}
#[derive(Deserialize)]
struct EnvelopeWire {
    schema_version: u16,
    source: String,
    stream: String,
    collector_id: String,
    collector_region: String,
    connection_id: String,
    collector_session_id: String,
    ingest_sequence: u64,
    source_sequence: Option<u64>,
    source_event_time: Option<DateTime<Utc>>,
    source_publish_time: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    monotonic_elapsed_nanos: u64,
    provider_time_unit: Option<String>,
    kind: EventKind,
    raw_payload: Vec<u8>,
    quality_flags: Vec<String>,
    payload_sha256: String,
    clock_status: ClockStatus,
}
impl EventEnvelope {
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.schema_version != 1 {
            return Err(err("unsupported schema version"));
        }
        if [
            self.source.as_str(),
            self.stream.as_str(),
            self.collector_id.as_str(),
            self.collector_region.as_str(),
            self.connection_id.as_str(),
            self.collector_session_id.as_str(),
        ]
        .iter()
        .any(|s| s.trim().is_empty())
        {
            return Err(err("missing required envelope field"));
        }
        if self.ingest_sequence == 0 {
            return Err(err("ingest sequence must be positive"));
        }
        if self.available_at < self.received_at {
            return Err(err("available before receive"));
        }
        if (self.source_event_time.is_some() || self.source_publish_time.is_some())
            && self
                .provider_time_unit
                .as_deref()
                .is_none_or(|unit| unit.trim().is_empty())
        {
            return Err(err("provider timestamp unit required"));
        }
        if self.raw_payload.is_empty() || sha256(&self.raw_payload) != self.payload_sha256 {
            return Err(err("payload empty or checksum mismatch"));
        }
        Ok(())
    }
    pub fn visible_at(&self, decision: DateTime<Utc>) -> Result<(), SchemaError> {
        self.validate()?;
        if decision < self.available_at {
            return Err(err("event unavailable at decision"));
        }
        if !matches!(self.clock_status, ClockStatus::Healthy { .. }) {
            return Err(err("collector clock is not verified"));
        }
        if self.kind == EventKind::Gap {
            return Err(err("gap event is not decision-eligible"));
        }
        if !self.quality_flags.is_empty() {
            return Err(err("event quality is not clean"));
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for EventEnvelope {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = EnvelopeWire::deserialize(d)?;
        let e = Self {
            schema_version: w.schema_version,
            source: w.source,
            stream: w.stream,
            collector_id: w.collector_id,
            collector_region: w.collector_region,
            connection_id: w.connection_id,
            collector_session_id: w.collector_session_id,
            ingest_sequence: w.ingest_sequence,
            source_sequence: w.source_sequence,
            source_event_time: w.source_event_time,
            source_publish_time: w.source_publish_time,
            received_at: w.received_at,
            available_at: w.available_at,
            monotonic_elapsed_nanos: w.monotonic_elapsed_nanos,
            provider_time_unit: w.provider_time_unit,
            kind: w.kind,
            raw_payload: w.raw_payload,
            quality_flags: w.quality_flags,
            payload_sha256: w.payload_sha256,
            clock_status: w.clock_status,
        };
        e.validate().map_err(serde::de::Error::custom)?;
        Ok(e)
    }
}

#[cfg(test)]
mod tests;
