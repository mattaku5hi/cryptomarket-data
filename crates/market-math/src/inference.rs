//! Intervals, tail probabilities and two-sample ranking statistics.

use std::collections::BTreeMap;

/// Wilson score interval for `hits` successes out of `n` trials at `z = 1.96`
/// (95%), as `[low, high]`; `None` when `n == 0`. No clamping to `[0, 1]`.
pub fn wilson_95(hits: usize, n: usize) -> Option<[f64; 2]> {
    if n == 0 {
        return None;
    }
    let (p, nn, z) = (hits as f64 / n as f64, n as f64, 1.96_f64);
    let den = 1.0 + z * z / nn;
    let c = (p + z * z / (2.0 * nn)) / den;
    let h = z * (p * (1.0 - p) / nn + z * z / (4.0 * nn * nn)).sqrt() / den;
    Some([c - h, c + h])
}

/// `P(X >= x)` for `X ~ Binomial(n, p)`, as `1 - P(X < x)` from the pmf recurrence and
/// clamped to `[0, 1]`. Exact edge cases: `x == 0` gives 1, `x > n` or `p <= 0` gives
/// 0, `p >= 1` gives 1; a NaN `p` with `1 <= x <= n` gives NaN.
pub fn binomial_upper_tail(x: usize, n: usize, p: f64) -> f64 {
    if x == 0 {
        return 1.0;
    }
    if x > n || p <= 0.0 {
        return 0.0;
    }
    if p >= 1.0 {
        return 1.0;
    }
    let mut pmf = (1.0 - p).powi(n as i32);
    let mut below = 0.0;
    for i in 0..x {
        below += pmf;
        pmf *= (n - i) as f64 / (i + 1) as f64 * p / (1.0 - p);
    }
    (1.0 - below).clamp(0.0, 1.0)
}

/// Two-sided standard-normal critical value for total tail probability `p`: the `z`
/// with `P(|Z| > z) = p`.
///
/// Bisection (60 steps on `[0, 10]`) over the Abramowitz-Stegun 26.2.17 normal CDF
/// approximation (absolute error below `7.5e-8`); returns the lower bracket.
pub fn normal_two_sided_quantile(p: f64) -> f64 {
    let cdf = |x: f64| {
        let t = 1.0 / (1.0 + 0.2316419 * x.abs());
        let poly = t
            * (0.319381530
                + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
        let tail = (-x * x / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt() * poly;
        if x >= 0.0 { 1.0 - tail } else { tail }
    };
    let (mut lo, mut hi) = (0.0, 10.0);
    for _ in 0..60 {
        let mid = (lo + hi) / 2.0;
        if 1.0 - cdf(mid) > p / 2.0 {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Mann-Whitney AUC: the probability that a value from `pos` exceeds one from `neg`,
/// ties counting one half (average ranks). Non-finite values are dropped first; `None`
/// when either side has no finite value.
pub fn mann_whitney_auc(pos: &[f64], neg: &[f64]) -> Option<f64> {
    let mut v: Vec<(f64, bool)> = pos
        .iter()
        .filter(|x| x.is_finite())
        .map(|x| (*x, true))
        .chain(neg.iter().filter(|x| x.is_finite()).map(|x| (*x, false)))
        .collect();
    let (np, nn) = (
        v.iter().filter(|x| x.1).count() as f64,
        v.iter().filter(|x| !x.1).count() as f64,
    );
    if np == 0.0 || nn == 0.0 {
        return None;
    }
    v.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut rank_sum = 0.0;
    let mut i = 0;
    while i < v.len() {
        let mut j = i;
        while j < v.len() && v[j].0 == v[i].0 {
            j += 1;
        }
        let avg = (i + j + 1) as f64 / 2.0;
        rank_sum += avg * v[i..j].iter().filter(|x| x.1).count() as f64;
        i = j;
    }
    Some((rank_sum - np * (np + 1.0) / 2.0) / (np * nn))
}

/// Threshold maximising Youden's J (`TPR - FPR`) for the flag `value >= thr` (`ge`) or
/// `value <= thr` (`!ge`).
///
/// Candidates are the distinct finite values of both samples in ascending order, taking
/// every `max(len / 200, 1)`-th one. Rates are over each sample's finite values (0 when
/// a sample has none). On equal J the last candidate wins. `None` when either input
/// slice is empty or no value is finite.
pub fn youden_threshold(pos: &[f64], neg: &[f64], ge: bool) -> Option<f64> {
    let mut all: Vec<f64> = pos
        .iter()
        .chain(neg)
        .copied()
        .filter(|x| x.is_finite())
        .collect();
    if pos.is_empty() || neg.is_empty() || all.is_empty() {
        return None;
    }
    all.sort_by(f64::total_cmp);
    all.dedup();
    let rate = |v: &[f64], t: f64| {
        let ok: Vec<f64> = v.iter().copied().filter(|x| x.is_finite()).collect();
        if ok.is_empty() {
            return 0.0;
        }
        ok.iter()
            .filter(|x| if ge { **x >= t } else { **x <= t })
            .count() as f64
            / ok.len() as f64
    };
    let step = (all.len() / 200).max(1);
    all.iter()
        .step_by(step)
        .map(|t| (*t, rate(pos, *t) - rate(neg, *t)))
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|x| x.0)
}

/// Mean and cluster-robust standard error of the mean for `(cluster, value)` items.
///
/// `se = sqrt(sum over clusters of (sum of (value - mean))^2) / n`, without a
/// small-sample `g / (g - 1)` factor. `None` with fewer than two items.
pub fn cluster_mean_se(items: &[(i64, f64)]) -> Option<(f64, f64)> {
    let n = items.len();
    if n < 2 {
        return None;
    }
    let mean = items.iter().map(|x| x.1).sum::<f64>() / n as f64;
    let mut clusters: BTreeMap<i64, f64> = BTreeMap::new();
    for (t, v) in items {
        *clusters.entry(*t).or_default() += v - mean;
    }
    let ss: f64 = clusters.values().map(|s| s * s).sum();
    Some((mean, ss.sqrt() / n as f64))
}
