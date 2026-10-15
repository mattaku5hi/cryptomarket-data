//! Strict structural records for caller-produced probability forecasts.
//!
//! Construction binds a record to market metadata. It does not qualify market rules, features,
//! source clocks, model provenance, reference age, or downstream forecast eligibility.

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]
#[path = "forecast_tests.rs"]
mod forecast_tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ClockStatus, Horizon, MarketPhase, MarketSpec, sha256};

pub const FORECAST_FORMAT: &str = "probability-forecast-record-v1";
pub const MAX_FORECAST_JSON_BYTES: usize = 64 * 1024;
const CONTENT_HASH_PREFIX: &[u8] = b"probability-forecast-content-json-v1\n";

/// Input record as supplied by the caller; validation occurs in [`ProbabilityForecast::new`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForecastInput {
    pub format: String,
    pub forecast_id: String,
    pub market_id: String,
    pub horizon: Horizon,
    pub market_phase: MarketPhase,
    pub predicted_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub available_data_cutoff: DateTime<Utc>,
    pub reference_age: LocalReferenceAge,
    pub p_up: f64,
    pub p_down: f64,
    #[serde(deserialize_with = "required_option")]
    pub q_market_up: Option<f64>,
    pub model_version: String,
    pub calibration_version: String,
    pub feature_schema_hash: String,
    pub resolver_version: String,
    pub platform_regime_id: String,
    pub data_quality: ForecastDataQuality,
}

/// Caller-asserted elapsed age since the local reference-availability handoff, not source-event age.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReferenceAge {
    pub monotonic_origin_id: String,
    /// Caller-asserted elapsed nanoseconds since local reference availability, not source-event age.
    pub elapsed_ns: u64,
}

/// Opaque quality annotations retained without qualification or promotion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForecastDataQuality {
    #[serde(deserialize_with = "deserialize_strict_clock_status")]
    pub clock_status: ClockStatus,
    pub flags: Vec<String>,
}

/// Forecast record validated against one complete market specification.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbabilityForecast {
    record: ForecastInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForecastError {
    InputTooLarge,
    MalformedJson,
    Serialization,
    UnsupportedFormat,
    InvalidIdentity,
    InvalidFeatureSchemaHash,
    InvalidDataQuality,
    InvalidProbability,
    InvalidProbabilityComplement,
    InvalidMarket,
    MarketBinding,
    MarketMetadataUnavailable,
    InvalidTime,
    PhaseMismatch,
}

impl ProbabilityForecast {
    /// Validate a caller record against the complete market metadata supplied by the caller.
    ///
    /// Clock status, flags, local reference age, and provenance strings are retained as supplied;
    /// this constructor does not qualify their truth or confer forecast eligibility.
    ///
    /// # Errors
    /// Returns an error for invalid record values, malformed market metadata, identity mismatch,
    /// unavailable market metadata, invalid time ordering, or a mismatched market phase.
    pub fn new(mut record: ForecastInput, market: &MarketSpec) -> Result<Self, ForecastError> {
        if market.validate().is_err() {
            return Err(ForecastError::InvalidMarket);
        }
        if record.format != FORECAST_FORMAT {
            return Err(ForecastError::UnsupportedFormat);
        }
        for identity in [
            record.forecast_id.as_str(),
            record.market_id.as_str(),
            record.model_version.as_str(),
            record.calibration_version.as_str(),
            record.resolver_version.as_str(),
            record.platform_regime_id.as_str(),
            record.reference_age.monotonic_origin_id.as_str(),
        ] {
            if identity.trim().is_empty() || identity.len() > 128 {
                return Err(ForecastError::InvalidIdentity);
            }
        }
        if !valid_sha256(&record.feature_schema_hash) {
            return Err(ForecastError::InvalidFeatureSchemaHash);
        }
        if record.data_quality.flags.len() > 64
            || record
                .data_quality
                .flags
                .iter()
                .any(|flag| flag.trim().is_empty() || flag.len() > 128)
        {
            return Err(ForecastError::InvalidDataQuality);
        }
        if !valid_probability(record.p_up)
            || !valid_probability(record.p_down)
            || record
                .q_market_up
                .is_some_and(|probability| !valid_probability(probability))
        {
            return Err(ForecastError::InvalidProbability);
        }
        if record.p_down != 1.0 - record.p_up {
            return Err(ForecastError::InvalidProbabilityComplement);
        }
        record.p_up = canonical_zero(record.p_up);
        record.p_down = canonical_zero(record.p_down);
        record.q_market_up = record.q_market_up.map(canonical_zero);

        if record.market_id != market.market_id
            || record.horizon != market.horizon
            || record.resolver_version != market.resolver_version
            || record.platform_regime_id != market.platform_regime_id
        {
            return Err(ForecastError::MarketBinding);
        }
        if record.available_data_cutoff < market.first_available_at {
            return Err(ForecastError::MarketMetadataUnavailable);
        }
        if record.available_data_cutoff > record.predicted_at
            || record.predicted_at > record.valid_until
            || record.valid_until > market.trading_close
        {
            return Err(ForecastError::InvalidTime);
        }
        let actual_phase = market
            .phase_at(record.predicted_at)
            .map_err(|_| ForecastError::InvalidTime)?;
        if record.market_phase != actual_phase {
            return Err(ForecastError::PhaseMismatch);
        }
        Ok(Self { record })
    }

    /// Parse bounded JSON and validate the record against supplied market metadata.
    ///
    /// # Errors
    /// Returns an error for input above the fixed byte cap, malformed or oversized record fields,
    /// or any validation error returned by [`Self::new`].
    pub fn from_json(raw: &[u8], market: &MarketSpec) -> Result<Self, ForecastError> {
        if raw.len() > MAX_FORECAST_JSON_BYTES {
            return Err(ForecastError::InputTooLarge);
        }
        let input = serde_json::from_slice::<ForecastInput>(raw)
            .map_err(|_| ForecastError::MalformedJson)?;
        Self::new(input, market)
    }

    /// Return the immutable validated input record.
    #[must_use]
    pub const fn record(&self) -> &ForecastInput {
        &self.record
    }

    /// Hash the versioned serialized-record JSON recipe, not the raw input bytes.
    ///
    /// # Errors
    /// Returns an error if the validated record cannot be serialized.
    pub fn content_sha256(&self) -> Result<String, ForecastError> {
        let serialized =
            serde_json::to_vec(&self.record).map_err(|_| ForecastError::Serialization)?;
        let mut content = Vec::with_capacity(CONTENT_HASH_PREFIX.len() + serialized.len());
        content.extend_from_slice(CONTENT_HASH_PREFIX);
        content.extend_from_slice(&serialized);
        Ok(sha256(&content))
    }
}

impl Serialize for ProbabilityForecast {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.record.serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum StrictClockStatus {
    Unknown,
    Uncertain,
    Healthy {
        offset_nanos: i64,
        uncertainty_nanos: u64,
    },
}

fn deserialize_strict_clock_status<'de, D>(deserializer: D) -> Result<ClockStatus, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let status = StrictClockStatus::deserialize(deserializer)?;
    Ok(match status {
        StrictClockStatus::Unknown => ClockStatus::Unknown,
        StrictClockStatus::Uncertain => ClockStatus::Uncertain,
        StrictClockStatus::Healthy {
            offset_nanos,
            uncertainty_nanos,
        } => ClockStatus::Healthy {
            offset_nanos,
            uncertainty_nanos,
        },
    })
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn valid_probability(probability: f64) -> bool {
    probability.is_finite() && (0.0..=1.0).contains(&probability)
}

fn canonical_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
