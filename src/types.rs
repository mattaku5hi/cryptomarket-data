//! Caller-bound identity and local clock annotations; neither establishes source health.
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentId(Arc<str>);
impl InstrumentId {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || value.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return None;
        }
        Some(Self(Arc::from(value)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockStatus {
    Unknown,
    Uncertain,
    Healthy {
        offset_nanos: i64,
        uncertainty_nanos: u64,
    },
}
