//! Zero-dependency numeric helpers for market-data research.
//!
//! Each function states its exact semantics. Similar functions that differ in warm-up,
//! NaN handling, variance denominator or quantile index rule are separate functions on
//! purpose: switching a caller to this crate must not change a single output bit.
//!
//! Modules:
//! - [`summary`]: means, variance, medians, trimmed mean, Welch t, log ratio.
//! - [`quantile`]: nearest-rank style quantiles on sorted or unsorted data.
//! - [`rolling`]: trailing-window statistics over series with NaN gaps.
//! - [`indicators`]: EMA, RSI and MACD histogram over price series.
//! - [`inference`]: Wilson interval, binomial tail, normal quantile, AUC, Youden
//!   threshold, cluster-robust standard error.
//! - [`bootstrap`]: seeded xorshift generator and bootstrap confidence intervals.

#![forbid(unsafe_code)]
// The numeric kernels are ported verbatim from their original callers so that results
// stay bit-identical; bounds are established by the surrounding checks and `as`
// conversions between counts and f64 are intended.
#![allow(
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::integer_division
)]

pub mod bootstrap;
pub mod indicators;
pub mod inference;
pub mod quantile;
pub mod rolling;
pub mod summary;
