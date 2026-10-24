#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_math::rolling::{rolling_mean, trailing_max, trailing_pct_rank, trailing_zscore};

fn same(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
}

#[test]
fn zscore_uses_previous_window_and_population_sd() {
    let x = [1.0, 2.0, 3.0, 4.0, 10.0];
    let z = trailing_zscore(&x, 4);
    // window [1, 2, 3, 4]: mean 2.5, population sd sqrt(1.25)
    assert!((z[4] - (10.0 - 2.5) / 1.25_f64.sqrt()).abs() < 1e-9);
    // k = 0 and fewer than max(4 / 2, 2) = 2 previous values are NaN
    assert!(z[0].is_nan() && z[1].is_nan());
    assert!(z[2].is_finite());
}

#[test]
fn zscore_is_causal_and_needs_spread() {
    let mut x: Vec<f64> = (0..200).map(|i| f64::from(i % 7)).collect();
    let base = trailing_zscore(&x, 100);
    x[150] = 1000.0;
    let changed = trailing_zscore(&x, 100);
    assert!(same(&base[..150], &changed[..150]));
    assert!(changed[150] > 10.0);
    // the current value is not in its own window: flat history gives NaN
    assert!(trailing_zscore(&[1.0; 120], 100).iter().all(|v| v.is_nan()));
}

#[test]
fn zscore_skips_nan_and_warm_up_counts_finite_values() {
    let x = [1.0, f64::NAN, 3.0, f64::NAN, 5.0];
    let z = trailing_zscore(&x, 4);
    // at k = 4 the window [1, NaN, 3, NaN] has 2 finite values: mean 2, sd 1
    assert_eq!(z[4], 3.0);
    assert!(z[1].is_nan() && z[3].is_nan());
    // at k = 2 only one finite value precedes it
    assert!(z[2].is_nan());
    assert!(trailing_zscore(&[], 10).is_empty());
}

#[test]
fn trailing_max_includes_current_and_needs_min_obs() {
    let x = [1.0, 5.0, 2.0, 3.0, 1.0, 1.0];
    let m = trailing_max(&x, 3, 2);
    assert!(m[0].is_nan());
    assert_eq!(&m[1..], &[5.0, 5.0, 5.0, 3.0, 3.0]);
    assert_eq!(trailing_max(&x, usize::MAX, 1)[5], 5.0);
    // a NaN current value gives NaN; NaN neighbours are skipped
    let g = trailing_max(&[4.0, f64::NAN, 2.0], 3, 2);
    assert!(g[1].is_nan());
    assert_eq!(g[2], 4.0);
    assert!(trailing_max(&[], 3, 1).is_empty());
}

#[test]
fn pct_rank_excludes_current() {
    let x = [1.0, 2.0, 3.0, 2.5];
    let r = trailing_pct_rank(&x, 10, 3);
    assert!(r[0].is_nan() && r[2].is_nan());
    assert_eq!(r[3], 2.0 / 3.0);
    // ties count as "<="; window cap 1 sees only the previous value
    let t = trailing_pct_rank(&[2.0, 2.0, 1.0], 1, 1);
    assert_eq!(&t[1..], &[1.0, 0.0]);
    let n = trailing_pct_rank(&[1.0, f64::NAN, 2.0], 5, 1);
    assert!(n[1].is_nan());
    assert_eq!(n[2], 1.0);
}

#[test]
fn rolling_mean_includes_current_and_skips_nan() {
    let x = [1.0, 2.0, f64::NAN, 4.0, 6.0];
    let m = rolling_mean(&x, 3, 2);
    assert!(m[0].is_nan());
    assert_eq!(m[1], 1.5);
    // window [1, 2, NaN]: two finite values even though x[2] is NaN
    assert_eq!(m[2], 1.5);
    assert_eq!(m[3], 3.0);
    assert_eq!(m[4], 5.0);
    assert!(rolling_mean(&[], 3, 1).is_empty());
}
