//! Trailing-window statistics over series that may contain NaN gaps.
//!
//! Each output has the input length. Non-finite inputs are skipped inside windows;
//! "no value" is reported as NaN.

use std::collections::VecDeque;

/// z-score of `x[k]` against the previous `window` values `x[k-window..k]` (the current
/// value is excluded; fewer at the start).
///
/// - Uses only finite values; needs at least `max(window / 2, 2)` of them.
/// - Population variance (`E[x^2] - mean^2`, from prefix sums, floored at 0).
/// - NaN at `k = 0`, for a non-finite `x[k]`, with too few values, or when the standard
///   deviation is `<= 1e-12`.
///
/// Never reads `x[k+1..]`.
pub fn trailing_zscore(x: &[f64], window: usize) -> Vec<f64> {
    let n = x.len();
    let mut cnt = vec![0u32; n + 1];
    let mut s1 = vec![0.0; n + 1];
    let mut s2 = vec![0.0; n + 1];
    for (i, v) in x.iter().enumerate() {
        let ok = v.is_finite();
        cnt[i + 1] = cnt[i] + u32::from(ok);
        s1[i + 1] = s1[i] + if ok { *v } else { 0.0 };
        s2[i + 1] = s2[i] + if ok { v * v } else { 0.0 };
    }
    let min_obs = (window / 2).max(2) as u32;
    let mut z = vec![f64::NAN; n];
    for k in 0..n {
        if !x[k].is_finite() || k == 0 {
            continue;
        }
        let lo = k.saturating_sub(window);
        let c = cnt[k] - cnt[lo];
        if c < min_obs {
            continue;
        }
        let nn = f64::from(c);
        let mean = (s1[k] - s1[lo]) / nn;
        let var = ((s2[k] - s2[lo]) / nn - mean * mean).max(0.0);
        let sd = var.sqrt();
        if sd > 1e-12 {
            z[k] = (x[k] - mean) / sd;
        }
    }
    z
}

/// Maximum of the finite values in `x[k+1-cap..=k]` (the current value included;
/// monotone deque). NaN unless `x[k]` is finite and the window holds at least `min_obs`
/// finite values.
pub fn trailing_max(x: &[f64], cap: usize, min_obs: usize) -> Vec<f64> {
    let n = x.len();
    let mut cnt = vec![0usize; n + 1];
    for (i, v) in x.iter().enumerate() {
        cnt[i + 1] = cnt[i] + usize::from(v.is_finite());
    }
    let mut out = vec![f64::NAN; n];
    let mut dq: VecDeque<usize> = VecDeque::new();
    for k in 0..n {
        if x[k].is_finite() {
            while dq.back().is_some_and(|b| x[*b] <= x[k]) {
                dq.pop_back();
            }
            dq.push_back(k);
        }
        let lo = (k + 1).saturating_sub(cap);
        while dq.front().is_some_and(|f| *f < lo) {
            dq.pop_front();
        }
        if x[k].is_finite() && cnt[k + 1] - cnt[lo] >= min_obs {
            if let Some(f) = dq.front() {
                out[k] = x[*f];
            }
        }
    }
    out
}

/// Percentile rank of `x[k]`: the fraction of the finite values in `x[k-cap..k]` (the
/// current value excluded) that are `<= x[k]`. NaN at `k = 0`, for a non-finite `x[k]`,
/// or with fewer than `min_obs` finite values in the window.
pub fn trailing_pct_rank(x: &[f64], cap: usize, min_obs: usize) -> Vec<f64> {
    let n = x.len();
    let mut out = vec![f64::NAN; n];
    for k in 1..n {
        if !x[k].is_finite() {
            continue;
        }
        let (mut c, mut le) = (0usize, 0usize);
        for v in &x[k.saturating_sub(cap)..k] {
            if v.is_finite() {
                c += 1;
                le += usize::from(*v <= x[k]);
            }
        }
        if c >= min_obs {
            out[k] = le as f64 / c as f64;
        }
    }
    out
}

/// Mean of the finite values in `x[k+1-w..=k]` (the current value included), kept as a
/// running sum (add the new value, subtract the one leaving). NaN with fewer than `min`
/// finite values; `x[k]` itself may be NaN.
pub fn rolling_mean(x: &[f64], w: usize, min: usize) -> Vec<f64> {
    let n = x.len();
    let mut out = vec![f64::NAN; n];
    let (mut s, mut c) = (0.0, 0usize);
    for k in 0..n {
        if x[k].is_finite() {
            s += x[k];
            c += 1;
        }
        if k >= w && x[k - w].is_finite() {
            s -= x[k - w];
            c -= 1;
        }
        if c >= min {
            out[k] = s / c as f64;
        }
    }
    out
}
