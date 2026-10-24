//! Seeded xorshift generator and bootstrap confidence intervals.
//!
//! All resampling is deterministic for a given seed. Percentile intervals differ in
//! their index rule; each function names the rule it uses.

use std::collections::BTreeMap;

/// Marsaglia xorshift64 with shifts `13, 7, 17`; [`next_u64`](Self::next_u64) returns
/// the new state. A zero state stays zero forever, so callers seed with an odd value
/// (for example `seed | 1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct XorShift64(pub u64);

impl XorShift64 {
    /// Advances the state and returns it.
    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// `next_u64() % n` as an index (modulo bias accepted).
    ///
    /// # Panics
    ///
    /// Panics when `n == 0`.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// `[q(0.025), q(0.975)]` of `means` with the rounded index `round((len - 1) * q)`
/// after sorting with `f64::total_cmp`; `None` when empty.
pub fn percentile_ci_95(mut means: Vec<f64>) -> Option<[f64; 2]> {
    if means.is_empty() {
        return None;
    }
    means.sort_by(f64::total_cmp);
    let at = |q: f64| means[((means.len() - 1) as f64 * q).round() as usize];
    Some([at(0.025), at(0.975)])
}

/// 95% bootstrap interval of the mean of `(cluster, value)` items.
///
/// - `clustered = false`: each replicate draws `len` single items with replacement and
///   takes their mean.
/// - `clustered = true`: items are grouped by equal `cluster` key (ascending key
///   order); each replicate draws as many whole clusters as exist and takes the mean
///   of all values in the drawn clusters.
///
/// The generator is [`XorShift64`] seeded with `seed | 1`; the interval is
/// [`percentile_ci_95`] of the `iters` replicate means. `None` with fewer than two
/// items or `iters == 0`.
pub fn bootstrap_mean_ci(
    items: &[(i64, f64)],
    iters: usize,
    clustered: bool,
    seed: u64,
) -> Option<[f64; 2]> {
    if items.len() < 2 {
        return None;
    }
    let mut rng = XorShift64(seed | 1);
    let mut means = Vec::with_capacity(iters);
    if clustered {
        let mut clusters: BTreeMap<i64, (f64, usize)> = BTreeMap::new();
        for (t, v) in items {
            let c = clusters.entry(*t).or_default();
            c.0 += v;
            c.1 += 1;
        }
        let clusters: Vec<(f64, usize)> = clusters.into_values().collect();
        for _ in 0..iters {
            let (mut sum, mut count) = (0.0, 0usize);
            for _ in 0..clusters.len() {
                let c = clusters[rng.below(clusters.len())];
                sum += c.0;
                count += c.1;
            }
            means.push(sum / count as f64);
        }
    } else {
        for _ in 0..iters {
            let sum: f64 = (0..items.len())
                .map(|_| items[rng.below(items.len())].1)
                .sum();
            means.push(sum / items.len() as f64);
        }
    }
    percentile_ci_95(means)
}

/// 95% cluster bootstrap interval of a ratio mean from pre-aggregated clusters
/// `(sum, count)`: each replicate draws `len` clusters with replacement and takes
/// `sum of sums / sum of counts`.
///
/// The generator is [`XorShift64`] seeded with `seed | 1`. The interval uses the
/// truncated index `floor(iters * q)` (not the rounded `(len - 1) * q` rule) for
/// `q = 0.025` and `0.975`. `None` with fewer than five clusters.
///
/// # Panics
///
/// Panics when `iters == 0`.
pub fn cluster_bootstrap_ratio_ci(
    days: &[(f64, u32)],
    iters: usize,
    seed: u64,
) -> Option<[f64; 2]> {
    if days.len() < 5 {
        return None;
    }
    let mut x = seed | 1;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut means = Vec::with_capacity(iters);
    for _ in 0..iters {
        let (mut s, mut c) = (0.0, 0u64);
        for _ in 0..days.len() {
            let d = days[(next() % days.len() as u64) as usize];
            s += d.0;
            c += u64::from(d.1);
        }
        means.push(s / c as f64);
    }
    means.sort_by(f64::total_cmp);
    Some([
        means[(iters as f64 * 0.025) as usize],
        means[(iters as f64 * 0.975) as usize],
    ])
}

/// Moving-block bootstrap of `mean(ret | flag) - mean(ret)` over `[k0, k1)`.
///
/// Positions with `ok[j]` false or non-finite `ret[j]` are ignored. The range is cut
/// into consecutive non-overlapping blocks of `block` positions (the last may be
/// shorter). Each replicate draws as many blocks as exist with replacement
/// ([`XorShift64`] seeded with `seed | 1`); replicates without any flagged or any
/// usable position are dropped.
///
/// Returns `(point estimate over all blocks, [q(0.025), q(0.975)])` with the rounded
/// index `round((len - 1) * q)`. `None` when the point estimate is undefined or fewer
/// than `iters / 2` replicates are usable.
///
/// # Panics
///
/// Panics when `iters == 0` with a defined point estimate, when `block == 0` and
/// `k0 < k1`, or when `ret`, `flag` or `ok` is shorter than `k1`.
#[allow(clippy::too_many_arguments)]
pub fn block_bootstrap_flagged_diff(
    ret: &[f64],
    flag: &[bool],
    ok: &[bool],
    k0: usize,
    k1: usize,
    block: usize,
    iters: usize,
    seed: u64,
) -> Option<(f64, [f64; 2])> {
    let mut blocks: Vec<[f64; 4]> = Vec::new();
    let mut k = k0;
    while k < k1 {
        let mut b = [0.0; 4]; // flagged sum, flagged n, all sum, all n
        for j in k..(k + block).min(k1) {
            if ok[j] && ret[j].is_finite() {
                b[2] += ret[j];
                b[3] += 1.0;
                if flag[j] {
                    b[0] += ret[j];
                    b[1] += 1.0;
                }
            }
        }
        blocks.push(b);
        k += block;
    }
    let stat = |ids: &mut dyn Iterator<Item = usize>| {
        let mut t = [0.0; 4];
        for i in ids {
            for (a, b) in t.iter_mut().zip(blocks[i]) {
                *a += b;
            }
        }
        (t[1] > 0.0 && t[3] > 0.0).then(|| t[0] / t[1] - t[2] / t[3])
    };
    let point = stat(&mut (0..blocks.len()))?;
    let mut x = seed | 1;
    let mut diffs = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut pick = Vec::with_capacity(blocks.len());
        for _ in 0..blocks.len() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            pick.push((x % blocks.len() as u64) as usize);
        }
        if let Some(d) = stat(&mut pick.into_iter()) {
            diffs.push(d);
        }
    }
    if diffs.len() < iters / 2 {
        return None;
    }
    diffs.sort_by(f64::total_cmp);
    let q = |p: f64| diffs[((diffs.len() - 1) as f64 * p).round() as usize];
    Some((point, [q(0.025), q(0.975)]))
}
