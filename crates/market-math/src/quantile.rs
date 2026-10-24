//! Quantiles without interpolation.
//!
//! Index rules used here:
//! - "rounded index": `round((len - 1) * q)` (half away from zero), the element at that
//!   position of the ascending data. `q` is expected in `[0, 1]`.
//! - "ceil rank": `clamp(ceil(q * len), 1, len) - 1`.

/// Rounded-index quantile; sorts `v` in place with `f64::total_cmp` (NaN sorts last).
/// NaN when `v` is empty.
pub fn quantile_nearest(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * q).round() as usize]
}

/// Rounded-index quantile of already ascending data; NaN when empty.
pub fn quantile_nearest_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

/// Rounded-index quantile of already ascending data with the index clamped to the last
/// element (so `p > 1` returns the maximum); `0.0` when empty.
pub fn quantile_nearest_sorted_or_zero(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Ceil-rank quantile of already ascending data: index `clamp(ceil(q * len), 1, len) - 1`
/// (so `q <= 0` gives the minimum); `None` when empty.
pub fn quantile_ceil_rank_sorted(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    Some(sorted[idx])
}
