//! Means, variance, medians and small summary statistics.

/// Arithmetic mean via `iter().sum()`; `None` for an empty slice. NaN propagates.
pub fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

/// Arithmetic mean via `iter().sum()`; `0.0` for an empty slice. NaN propagates.
pub fn mean_or_zero(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

/// Arithmetic mean of an iterator, summed left to right from `0.0`; `None` when the
/// iterator is empty. NaN propagates.
pub fn mean_iter(v: impl Iterator<Item = f64>) -> Option<f64> {
    let (s, n) = v.fold((0.0, 0usize), |(s, n), x| (s + x, n + 1));
    (n > 0).then(|| s / n as f64)
}

/// Arithmetic mean of an iterator, summed left to right from `0.0`; NaN when the
/// iterator is empty. NaN propagates.
pub fn mean_iter_or_nan(v: impl Iterator<Item = f64>) -> f64 {
    let (s, n) = v.fold((0.0, 0usize), |a, x| (a.0 + x, a.1 + 1));
    if n == 0 { f64::NAN } else { s / n as f64 }
}

/// Sample variance (denominator `n - 1`) around [`mean`]; `None` with fewer than two
/// values. NaN propagates.
pub fn sample_variance(values: &[f64]) -> Option<f64> {
    let m = mean(values)?;
    (values.len() > 1)
        .then(|| values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64)
}

/// Welch t statistic of `mean(a) - mean(b)` using [`sample_variance`] on each side.
/// `None` when either side has fewer than two values or the standard error is not
/// positive.
pub fn welch_t(a: &[f64], b: &[f64]) -> Option<f64> {
    let (va, vb) = (sample_variance(a)?, sample_variance(b)?);
    let se = (va / a.len() as f64 + vb / b.len() as f64).sqrt();
    let diff = mean(a)? - mean(b)?;
    (se > 0.0).then_some(diff / se)
}

/// Median of a copy sorted with `f64::total_cmp` (NaN sorts last, `-0.0` before `0.0`);
/// the mean of the two middle values for an even length; `None` when empty.
pub fn median(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    match sorted.len() {
        0 => None,
        n if n % 2 == 1 => Some(sorted[n / 2]),
        n => Some((sorted[n / 2 - 1] + sorted[n / 2]) / 2.0),
    }
}

/// Median that sorts `v` in place with `f64::total_cmp`; the mean of the two middle
/// values for an even length.
///
/// # Panics
///
/// Panics when `v` is empty (the caller guarantees at least one value).
pub fn median_in_place(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Median of the finite values: removes non-finite values from `v` (in place), then
/// sorts it with `f64::total_cmp`; the mean of the two middle values for an even
/// length; NaN when no finite value remains.
pub fn median_finite(v: &mut Vec<f64>) -> f64 {
    v.retain(|x| x.is_finite());
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Mean after dropping `len / 100` values (integer division) from each end of a copy
/// sorted with `f64::total_cmp`, so nothing is trimmed below 100 values; NaN when empty.
pub fn trimmed_mean_1pct(pct: &[f64]) -> f64 {
    let mut v = pct.to_vec();
    v.sort_by(f64::total_cmp);
    let cut = v.len() / 100;
    let core = &v[cut..v.len() - cut];
    if core.is_empty() {
        f64::NAN
    } else {
        core.iter().sum::<f64>() / core.len() as f64
    }
}

/// `ln(a / b)` when both are positive, otherwise NaN (also for NaN inputs).
pub fn ln_ratio(a: f64, b: f64) -> f64 {
    if a > 0.0 && b > 0.0 {
        (a / b).ln()
    } else {
        f64::NAN
    }
}
