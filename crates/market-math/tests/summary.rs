#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_math::summary::{
    ln_ratio, mean, mean_iter, mean_iter_or_nan, mean_or_zero, median, median_finite,
    median_in_place, sample_variance, trimmed_mean_1pct, welch_t,
};

#[test]
fn means_differ_only_on_empty_input() {
    let v = [1.0, 2.0, 6.0];
    assert_eq!(mean(&v), Some(3.0));
    assert_eq!(mean_or_zero(&v), 3.0);
    assert_eq!(mean_iter(v.iter().copied()), Some(3.0));
    assert_eq!(mean_iter_or_nan(v.iter().copied()), 3.0);

    assert_eq!(mean(&[]), None);
    assert_eq!(mean_or_zero(&[]), 0.0);
    assert_eq!(mean_iter(std::iter::empty()), None);
    assert!(mean_iter_or_nan(std::iter::empty()).is_nan());
}

#[test]
fn means_propagate_nan() {
    let v = [1.0, f64::NAN];
    assert!(mean(&v).unwrap().is_nan());
    assert!(mean_or_zero(&v).is_nan());
    assert!(mean_iter(v.iter().copied()).unwrap().is_nan());
    assert!(mean_iter_or_nan(v.iter().copied()).is_nan());
}

#[test]
fn sample_variance_uses_n_minus_one() {
    assert_eq!(sample_variance(&[1.0, 2.0, 3.0, 4.0]), Some(5.0 / 3.0));
    assert_eq!(sample_variance(&[7.0]), None);
    assert_eq!(sample_variance(&[]), None);
    assert!(sample_variance(&[1.0, f64::NAN]).unwrap().is_nan());
}

#[test]
fn welch_t_needs_two_values_per_side_and_spread() {
    assert!(welch_t(&[1.0], &[1.0, 2.0]).is_none());
    assert!(welch_t(&[1.0, 1.0], &[2.0, 2.0]).is_none());
    // means 4 and 2, variances 1 and 1, n 3 and 3: se = sqrt(2/3)
    let t = welch_t(&[3.0, 4.0, 5.0], &[1.0, 2.0, 3.0]).unwrap();
    assert!((t - 2.0 / (2.0f64 / 3.0).sqrt()).abs() < 1e-12);
}

#[test]
fn median_variants() {
    assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
    assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.5));
    assert_eq!(median(&[]), None);
    // total_cmp sorts NaN last, so it can become the upper middle value.
    assert!(median(&[1.0, f64::NAN]).unwrap().is_nan());
    assert_eq!(median(&[1.0, 2.0, f64::NAN]), Some(2.0));

    let mut v = [4.0, 1.0, 3.0, 2.0];
    assert_eq!(median_in_place(&mut v), 2.5);
    assert_eq!(v, [1.0, 2.0, 3.0, 4.0]);
    assert_eq!(median_in_place(&mut [5.0]), 5.0);

    let mut f = vec![f64::NAN, 3.0, f64::INFINITY, 1.0];
    assert_eq!(median_finite(&mut f), 2.0);
    assert_eq!(f, vec![1.0, 3.0]);
    assert!(median_finite(&mut vec![f64::NAN, f64::NEG_INFINITY]).is_nan());
    assert!(median_finite(&mut Vec::new()).is_nan());
}

#[test]
fn median_in_place_panics_on_empty() {
    assert!(std::panic::catch_unwind(|| median_in_place(&mut [])).is_err());
}

#[test]
fn trimmed_mean_drops_one_percent_per_side() {
    // Below 100 values nothing is trimmed.
    assert_eq!(trimmed_mean_1pct(&[1.0, 2.0, 30.0]), 11.0);
    // 100 values: the minimum and the maximum go.
    let mut v: Vec<f64> = (0..100).map(f64::from).collect();
    v[99] = 1e9;
    v[0] = -1e9;
    let core: f64 = (1..99).map(f64::from).sum::<f64>() / 98.0;
    assert_eq!(trimmed_mean_1pct(&v), core);
    assert!(trimmed_mean_1pct(&[]).is_nan());
}

#[test]
fn ln_ratio_needs_positive_inputs() {
    assert_eq!(ln_ratio(std::f64::consts::E, 1.0), 1.0);
    assert!(ln_ratio(0.0, 1.0).is_nan());
    assert!(ln_ratio(1.0, -1.0).is_nan());
    assert!(ln_ratio(f64::NAN, 1.0).is_nan());
}
