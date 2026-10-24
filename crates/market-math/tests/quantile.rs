#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_math::quantile::{
    quantile_ceil_rank_sorted, quantile_nearest, quantile_nearest_sorted,
    quantile_nearest_sorted_or_zero,
};

const TEN: [f64; 10] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

#[test]
fn rounded_index_rule() {
    // round(9 * 0.5) = round(4.5) = 5 (half away from zero) -> 6.0
    assert_eq!(quantile_nearest_sorted(&TEN, 0.5), 6.0);
    assert_eq!(quantile_nearest_sorted(&TEN, 0.0), 1.0);
    assert_eq!(quantile_nearest_sorted(&TEN, 1.0), 10.0);
    assert_eq!(quantile_nearest_sorted(&TEN, 0.95), 10.0);
    assert_eq!(quantile_nearest_sorted(&TEN, 0.9), 9.0);
    assert!(quantile_nearest_sorted(&[], 0.5).is_nan());
}

#[test]
fn unsorted_variant_sorts_in_place_and_nan_sorts_last() {
    let mut v = [3.0, f64::NAN, 1.0, 2.0];
    assert_eq!(quantile_nearest(&mut v, 0.0), 1.0);
    assert_eq!(v[..3], [1.0, 2.0, 3.0]);
    assert!(v[3].is_nan());
    assert!(quantile_nearest(&mut v, 1.0).is_nan());
    assert!(quantile_nearest(&mut [], 0.5).is_nan());
}

#[test]
fn or_zero_variant_clamps_and_defaults_to_zero() {
    assert_eq!(quantile_nearest_sorted_or_zero(&TEN, 0.5), 6.0);
    assert_eq!(quantile_nearest_sorted_or_zero(&TEN, 2.0), 10.0);
    assert_eq!(quantile_nearest_sorted_or_zero(&[], 0.5), 0.0);
}

#[test]
fn ceil_rank_rule() {
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 0.5), Some(5.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 0.9), Some(9.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 0.91), Some(10.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 1.0), Some(10.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 0.0), Some(1.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, -1.0), Some(1.0));
    assert_eq!(quantile_ceil_rank_sorted(&TEN, 5.0), Some(10.0));
    assert_eq!(quantile_ceil_rank_sorted(&[], 0.5), None);
}

#[test]
fn rules_disagree_on_the_median_of_an_even_count() {
    assert_ne!(
        quantile_nearest_sorted(&TEN, 0.5),
        quantile_ceil_rank_sorted(&TEN, 0.5).unwrap()
    );
}
