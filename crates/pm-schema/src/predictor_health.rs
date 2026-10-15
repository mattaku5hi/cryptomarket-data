//! Structural caller-reported predictor and source health.
//!
//! Statuses and source ages are retained declarations only. This record does not verify a source,
//! infer health, or grant forecast eligibility. Heartbeat freshness is a separate comparison
//! against a caller-supplied UTC instant.

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]
#[path = "predictor_health_tests.rs"]
mod tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::forecast::ForecastDataQuality;

pub const HEALTH_FORMAT: &str = "predictor-health-record-v1";
pub const MAX_HEALTH_JSON_BYTES: usize = 64 * 1024;
pub const MAX_REQUIRED_SOURCES: usize = 64;
pub const MAX_MODEL_VERSIONS: usize = 64;
const MAX_IDENTITY_BYTES: usize = 128;

/// Caller-declared health status; values are never verified or inferred here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Unknown,
    Unhealthy,
    Healthy,
}

/// Input health record before structural validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthInput {
    pub format: String,
    pub status: HealthStatus,
    pub reported_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    #[serde(deserialize_with = "required_option")]
    pub last_forecast_at: Option<DateTime<Utc>>,
    pub required_sources: Vec<RequiredSourceHealth>,
    pub model_versions: Vec<String>,
    pub data_quality: ForecastDataQuality,
}

/// One caller-declared required source and its producer-local monotonic age, if available.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredSourceHealth {
    pub source_id: String,
    pub status: HealthStatus,
    #[serde(deserialize_with = "required_option")]
    pub age: Option<LocalSourceAge>,
}

/// Caller-declared elapsed age from source availability to report time on one producer-local
/// monotonic-clock origin; it is not a provider timestamp or a receiver-side age.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSourceAge {
    pub monotonic_origin_id: String,
    pub elapsed_ns: u64,
}

/// Immutable structurally validated predictor health record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredictorHealth {
    record: HealthInput,
}

/// Structural health record or bounded JSON rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictorHealthError {
    InputTooLarge,
    MalformedJson,
    Serialization,
    UnsupportedFormat,
    InvalidIdentity,
    DuplicateSourceId,
    DuplicateModelVersion,
    TooManyRequiredSources,
    TooManyModelVersions,
    InvalidTime,
    InvalidDataQuality,
}

impl std::fmt::Display for PredictorHealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InputTooLarge => "predictor health JSON exceeds the byte limit",
            Self::MalformedJson => "predictor health JSON is malformed",
            Self::Serialization => "predictor health record serialization failed",
            Self::UnsupportedFormat => "predictor health record format is unsupported",
            Self::InvalidIdentity => "predictor health identity is invalid",
            Self::DuplicateSourceId => "required source IDs must be unique",
            Self::DuplicateModelVersion => "model versions must be unique",
            Self::TooManyRequiredSources => "too many required sources",
            Self::TooManyModelVersions => "too many model versions",
            Self::InvalidTime => "predictor health times are out of order",
            Self::InvalidDataQuality => "predictor health data quality is invalid",
        })
    }
}

impl std::error::Error for PredictorHealthError {}

impl PredictorHealth {
    /// Structurally validate and retain a caller-declared health record.
    ///
    /// # Errors
    /// Returns an error for unsupported format, invalid or duplicate identities, excessive
    /// metadata counts, invalid time ordering, or invalid quality annotations.
    pub fn new(record: HealthInput) -> Result<Self, PredictorHealthError> {
        if record.format != HEALTH_FORMAT {
            return Err(PredictorHealthError::UnsupportedFormat);
        }
        if record.required_sources.len() > MAX_REQUIRED_SOURCES {
            return Err(PredictorHealthError::TooManyRequiredSources);
        }
        if record.model_versions.len() > MAX_MODEL_VERSIONS {
            return Err(PredictorHealthError::TooManyModelVersions);
        }

        if record
            .model_versions
            .iter()
            .any(|value| !valid_identity(value))
        {
            return Err(PredictorHealthError::InvalidIdentity);
        }
        for (index, source) in record.required_sources.iter().enumerate() {
            if !valid_identity(&source.source_id)
                || source
                    .age
                    .as_ref()
                    .is_some_and(|age| !valid_identity(&age.monotonic_origin_id))
            {
                return Err(PredictorHealthError::InvalidIdentity);
            }
            if record
                .required_sources
                .iter()
                .take(index)
                .any(|seen| seen.source_id == source.source_id)
            {
                return Err(PredictorHealthError::DuplicateSourceId);
            }
        }
        for (index, version) in record.model_versions.iter().enumerate() {
            if record
                .model_versions
                .iter()
                .take(index)
                .any(|seen| seen == version)
            {
                return Err(PredictorHealthError::DuplicateModelVersion);
            }
        }

        if record.reported_at > record.valid_until
            || record
                .last_forecast_at
                .is_some_and(|last| last > record.reported_at)
        {
            return Err(PredictorHealthError::InvalidTime);
        }
        if record.data_quality.flags.len() > 64
            || record
                .data_quality
                .flags
                .iter()
                .any(|flag| !valid_identity(flag))
        {
            return Err(PredictorHealthError::InvalidDataQuality);
        }

        Ok(Self { record })
    }

    /// Parse bounded strict JSON, then validate its structure.
    ///
    /// # Errors
    /// Returns an error when input is oversized, malformed, or structurally invalid.
    pub fn from_json(raw: &[u8]) -> Result<Self, PredictorHealthError> {
        if raw.len() > MAX_HEALTH_JSON_BYTES {
            return Err(PredictorHealthError::InputTooLarge);
        }
        let input = serde_json::from_slice::<HealthInput>(raw)
            .map_err(|_| PredictorHealthError::MalformedJson)?;
        Self::new(input)
    }

    /// Return the immutable validated caller record.
    #[must_use]
    pub const fn record(&self) -> &HealthInput {
        &self.record
    }

    /// Serialize the validated record as its typed JSON representation.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<Vec<u8>, PredictorHealthError> {
        serde_json::to_vec(self).map_err(|_| PredictorHealthError::Serialization)
    }
}

impl Serialize for PredictorHealth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.record.serialize(serializer)
    }
}

/// Freshness of a declared heartbeat relative to caller-supplied comparable UTC time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeartbeatFreshness {
    Missing,
    Future,
    Expired,
    DeclaredUnexpired,
}

/// Compare caller-provided UTC `now` with the record's declared interval.
///
/// `DeclaredUnexpired` only means the interval contains `now`; it does not qualify health,
/// sources, or forecasts and does not authorize trading or forecast publication.
#[must_use]
pub fn freshness_at(health: Option<&PredictorHealth>, now: DateTime<Utc>) -> HeartbeatFreshness {
    let Some(health) = health else {
        return HeartbeatFreshness::Missing;
    };
    if now < health.record.reported_at {
        HeartbeatFreshness::Future
    } else if now >= health.record.valid_until {
        HeartbeatFreshness::Expired
    } else {
        HeartbeatFreshness::DeclaredUnexpired
    }
}

fn valid_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_IDENTITY_BYTES
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
