//! Causal finalized trade candles for one explicit instrument and fixed UTC cadence.

use crate::types::{ClockStatus, InstrumentId};
use chrono::{DateTime, Utc};

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Candle formula and finalization convention.
pub const TRADE_CANDLE_FORMULA_VERSION: &str = "causal-finalized-trade-candles-v2";

/// Explicit source volume field used to weight candle volume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeConvention {
    /// Caller-selected total base quantity.
    TotalBase,
    /// Caller-selected base quantity excluding RPI, when provided by the source.
    NonRpiBase,
}

/// Positive fixed-duration UTC epoch-aligned interval. Calendar months are separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandleCadence {
    seconds: u32,
}
impl CandleCadence {
    pub fn from_seconds(seconds: u32) -> Result<Self, CandleError> {
        if seconds == 0 {
            return Err(CandleError::InvalidCadence);
        }
        Ok(Self { seconds })
    }
    pub fn width_seconds(self) -> i64 {
        i64::from(self.seconds)
    }
    fn width_nanos(self) -> i128 {
        i128::from(self.seconds) * NANOS_PER_SECOND
    }
}

/// One caller-projected trade with instrument identity and local availability evidence.
#[derive(Clone, Debug)]
pub struct TradeObservation {
    pub source: InstrumentId,
    pub trade_time: DateTime<Utc>,
    pub aggregate_trade_id: u64,
    pub available_at: DateTime<Utc>,
    pub clock_status: ClockStatus,
    /// Positive projected quote-currency price per base unit.
    pub price: f64,
    /// Projected base quantity from the configured source field.
    pub base_volume: f64,
}

/// One candle emitted when a trade from a later interval arrives.
#[derive(Clone, Debug, PartialEq)]
pub struct FinalizedCandle {
    pub source: InstrumentId,
    pub volume_convention: VolumeConvention,
    pub cadence: CandleCadence,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub base_volume: f64,
    /// Price weighted by configured base volume; missing when total volume is zero.
    pub observed_trade_vwap: Option<f64>,
    pub trade_count: u64,
    pub last_aggregate_trade_id: u64,
    pub source_last_trade_time: DateTime<Utc>,
    /// Availability is the later trade's handoff time, not this candle's bucket end.
    pub available_at: DateTime<Utc>,
    /// Conservative status over candle members and the finalizing trigger.
    pub clock_status: ClockStatus,
    /// Event-time buckets with no observed input between this bar and its trigger.
    pub skipped_intervals: u64,
}

/// Rejected input or arithmetic with no builder state change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandleError {
    InvalidVolumeConvention,
    InvalidCadence,
    WrongInstrument,
    NonFinitePrice,
    NonPositivePrice,
    NonFiniteVolume,
    NegativeVolume,
    TradeTimeRegressed,
    AggregateIdNotIncreasing,
    AvailabilityRegressed,
    TimestampOutOfRange,
    NumericOverflow,
}

#[derive(Clone, Debug)]
struct PendingCandle {
    bucket_index: i128,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    base_volume: f64,
    observed_trade_vwap: Option<f64>,
    trade_count: u64,
    last_aggregate_trade_id: u64,
    source_last_trade_time: DateTime<Utc>,
    clock_status: ClockStatus,
}

/// O(1) pending candle that emits only after a newer event-time bucket is observed.
///
/// After any returned error, the caller should invalidate dependent warmup before using later
/// output: this primitive preserves transactional state, but cannot know whether the rejection
/// represents lost source history. It never fills skipped intervals or finalizes its open tail.
#[derive(Clone, Debug)]
pub struct TradeCandleBuilder {
    source: InstrumentId,
    volume_convention: VolumeConvention,
    cadence: CandleCadence,
    pending: Option<PendingCandle>,
    last_trade_time: Option<DateTime<Utc>>,
    last_aggregate_trade_id: Option<u64>,
    last_available_at: Option<DateTime<Utc>>,
}

impl TradeCandleBuilder {
    /// Create an isolated source builder with explicit cadence and volume semantics.
    pub fn new(
        source: InstrumentId,
        cadence: CandleCadence,
        volume_convention: VolumeConvention,
    ) -> Result<Self, CandleError> {
        Ok(Self {
            source,
            volume_convention,
            cadence,
            pending: None,
            last_trade_time: None,
            last_aggregate_trade_id: None,
            last_available_at: None,
        })
    }

    /// Add one already-available trade; emit the previous candle only across a bucket boundary.
    ///
    /// Equal trade timestamps are accepted when aggregate IDs increase. Aggregate IDs need not
    /// be dense. A later bucket's input finalizes the preceding bucket at that input's
    /// `available_at`; no inferred candle is emitted for skipped intervals or for the open tail.
    pub fn push(
        &mut self,
        trade: &TradeObservation,
    ) -> Result<Option<FinalizedCandle>, CandleError> {
        if trade.source != self.source {
            return Err(CandleError::WrongInstrument);
        }
        validate_trade(trade)?;
        if self
            .last_trade_time
            .is_some_and(|last| trade.trade_time < last)
        {
            return Err(CandleError::TradeTimeRegressed);
        }
        if self
            .last_aggregate_trade_id
            .is_some_and(|last| trade.aggregate_trade_id <= last)
        {
            return Err(CandleError::AggregateIdNotIncreasing);
        }
        if self
            .last_available_at
            .is_some_and(|last| trade.available_at < last)
        {
            return Err(CandleError::AvailabilityRegressed);
        }

        let width = self.cadence.width_nanos();
        let trade_nanos = epoch_nanos(trade.trade_time)?;
        let bucket_index = trade_nanos.div_euclid(width);
        let bucket_start_nanos = bucket_index
            .checked_mul(width)
            .ok_or(CandleError::NumericOverflow)?;
        let bucket_end_nanos = bucket_start_nanos
            .checked_add(width)
            .ok_or(CandleError::NumericOverflow)?;
        let window_start = datetime_from_epoch_nanos(bucket_start_nanos)?;
        let window_end = datetime_from_epoch_nanos(bucket_end_nanos)?;

        let (next_pending, finalized) = match self.pending.as_ref() {
            None => (
                new_pending(bucket_index, window_start, window_end, trade),
                None,
            ),
            Some(pending) if bucket_index < pending.bucket_index => {
                return Err(CandleError::TradeTimeRegressed);
            }
            Some(pending) if bucket_index == pending.bucket_index => {
                (update_pending(pending, trade)?, None)
            }
            Some(pending) => {
                let skipped_intervals = bucket_index
                    .checked_sub(pending.bucket_index)
                    .and_then(|difference| difference.checked_sub(1))
                    .and_then(|skipped| u64::try_from(skipped).ok())
                    .ok_or(CandleError::NumericOverflow)?;
                let finalized = finalize(
                    pending,
                    self.source.clone(),
                    self.volume_convention,
                    self.cadence,
                    trade,
                    skipped_intervals,
                );
                (
                    new_pending(bucket_index, window_start, window_end, trade),
                    Some(finalized),
                )
            }
        };

        self.pending = Some(next_pending);
        self.last_trade_time = Some(trade.trade_time);
        self.last_aggregate_trade_id = Some(trade.aggregate_trade_id);
        self.last_available_at = Some(trade.available_at);
        Ok(finalized)
    }

    /// Discard the pending candle and all ordering history; warmup starts from the next trade.
    pub fn reset(&mut self) {
        self.pending = None;
        self.last_trade_time = None;
        self.last_aggregate_trade_id = None;
        self.last_available_at = None;
    }
}

fn validate_trade(trade: &TradeObservation) -> Result<(), CandleError> {
    if !trade.price.is_finite() {
        return Err(CandleError::NonFinitePrice);
    }
    if trade.price <= 0.0 {
        return Err(CandleError::NonPositivePrice);
    }
    if !trade.base_volume.is_finite() {
        return Err(CandleError::NonFiniteVolume);
    }
    if trade.base_volume < 0.0 {
        return Err(CandleError::NegativeVolume);
    }
    Ok(())
}

fn new_pending(
    bucket_index: i128,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    trade: &TradeObservation,
) -> PendingCandle {
    PendingCandle {
        bucket_index,
        window_start,
        window_end,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        base_volume: trade.base_volume,
        observed_trade_vwap: (trade.base_volume > 0.0).then_some(trade.price),
        trade_count: 1,
        last_aggregate_trade_id: trade.aggregate_trade_id,
        source_last_trade_time: trade.trade_time,
        clock_status: trade.clock_status,
    }
}

fn update_pending(
    pending: &PendingCandle,
    trade: &TradeObservation,
) -> Result<PendingCandle, CandleError> {
    let base_volume = pending.base_volume + trade.base_volume;
    if !base_volume.is_finite() {
        return Err(CandleError::NumericOverflow);
    }
    let trade_count = pending
        .trade_count
        .checked_add(1)
        .ok_or(CandleError::NumericOverflow)?;
    let observed_trade_vwap = update_weighted_mean(
        pending.observed_trade_vwap,
        pending.base_volume,
        base_volume,
        trade.price,
        trade.base_volume,
    )?;
    Ok(PendingCandle {
        bucket_index: pending.bucket_index,
        window_start: pending.window_start,
        window_end: pending.window_end,
        open: pending.open,
        high: pending.high.max(trade.price),
        low: pending.low.min(trade.price),
        close: trade.price,
        base_volume,
        observed_trade_vwap,
        trade_count,
        last_aggregate_trade_id: trade.aggregate_trade_id,
        source_last_trade_time: trade.trade_time,
        clock_status: combine_clock_status(pending.clock_status, trade.clock_status),
    })
}

/// Update the positive-volume arithmetic weighted mean without forming price*volume products.
/// The endpoint carrying more weight anchors the interpolation to minimize rounding loss.
fn update_weighted_mean(
    previous_mean: Option<f64>,
    previous_weight: f64,
    next_weight: f64,
    price: f64,
    trade_weight: f64,
) -> Result<Option<f64>, CandleError> {
    if trade_weight == 0.0 {
        return Ok(previous_mean);
    }
    if previous_weight == 0.0 {
        return Ok(Some(price));
    }
    let previous_mean = previous_mean.ok_or(CandleError::NumericOverflow)?;
    let (anchor, difference, weight) = if trade_weight <= previous_weight {
        (previous_mean, price - previous_mean, trade_weight)
    } else {
        (price, previous_mean - price, previous_weight)
    };
    let contribution = weighted_contribution(difference, weight, next_weight);
    let mean = anchor + contribution;
    if !mean.is_finite() {
        return Err(CandleError::NumericOverflow);
    }
    let mean = mean.clamp(previous_mean.min(price), previous_mean.max(price));
    if mean <= 0.0 {
        return Err(CandleError::NumericOverflow);
    }
    Ok(Some(mean))
}

fn finalize(
    pending: &PendingCandle,
    source: InstrumentId,
    volume_convention: VolumeConvention,
    cadence: CandleCadence,
    trigger: &TradeObservation,
    skipped_intervals: u64,
) -> FinalizedCandle {
    FinalizedCandle {
        source,
        volume_convention,
        cadence,
        window_start: pending.window_start,
        window_end: pending.window_end,
        open: pending.open,
        high: pending.high,
        low: pending.low,
        close: pending.close,
        base_volume: pending.base_volume,
        observed_trade_vwap: pending.observed_trade_vwap,
        trade_count: pending.trade_count,
        last_aggregate_trade_id: pending.last_aggregate_trade_id,
        source_last_trade_time: pending.source_last_trade_time,
        available_at: trigger.available_at,
        clock_status: combine_clock_status(pending.clock_status, trigger.clock_status),
        skipped_intervals,
    }
}

fn combine_clock_status(first: ClockStatus, second: ClockStatus) -> ClockStatus {
    match (first, second) {
        (ClockStatus::Unknown, _) | (_, ClockStatus::Unknown) => ClockStatus::Unknown,
        (ClockStatus::Uncertain, _) | (_, ClockStatus::Uncertain) => ClockStatus::Uncertain,
        (first @ ClockStatus::Healthy { .. }, second @ ClockStatus::Healthy { .. })
            if first == second =>
        {
            first
        }
        (ClockStatus::Healthy { .. }, ClockStatus::Healthy { .. }) => ClockStatus::Uncertain,
    }
}

fn epoch_nanos(time: DateTime<Utc>) -> Result<i128, CandleError> {
    i128::from(time.timestamp())
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|seconds| seconds.checked_add(i128::from(time.timestamp_subsec_nanos())))
        .ok_or(CandleError::TimestampOutOfRange)
}

fn datetime_from_epoch_nanos(nanos: i128) -> Result<DateTime<Utc>, CandleError> {
    let seconds = i64::try_from(nanos.div_euclid(NANOS_PER_SECOND))
        .map_err(|_| CandleError::TimestampOutOfRange)?;
    let subsecond_nanos = u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND))
        .map_err(|_| CandleError::TimestampOutOfRange)?;
    DateTime::from_timestamp(seconds, subsecond_nanos).ok_or(CandleError::TimestampOutOfRange)
}

fn weighted_contribution(difference: f64, weight: f64, total: f64) -> f64 {
    let fraction = weight / total;
    if fraction < f64::MIN_POSITIVE && weight > 0.0 {
        let product = difference * weight;
        if product.is_finite() {
            product / total
        } else {
            (difference / total) * weight
        }
    } else {
        difference * fraction
    }
}
