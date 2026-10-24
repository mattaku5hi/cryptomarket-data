#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_math::inference::{
    binomial_upper_tail, cluster_mean_se, mann_whitney_auc, normal_two_sided_quantile, wilson_95,
    youden_threshold,
};

#[test]
fn wilson_interval() {
    let w = wilson_95(5, 10).unwrap();
    assert!((w[0] + w[1] - 1.0).abs() < 1e-12);
    assert!(w[0] > 0.2 && w[1] < 0.8);
    let zero = wilson_95(0, 10).unwrap();
    assert!(zero[0].abs() < 1e-12 && zero[1] > 0.0);
    assert!(wilson_95(0, 0).is_none());
}

#[test]
fn binomial_tail_known_values_and_edges() {
    assert!((binomial_upper_tail(1, 2, 0.5) - 0.75).abs() < 1e-12);
    assert!((binomial_upper_tail(2, 2, 0.5) - 0.25).abs() < 1e-12);
    assert_eq!(binomial_upper_tail(0, 5, 0.2), 1.0);
    assert_eq!(binomial_upper_tail(3, 2, 0.5), 0.0);
    assert_eq!(binomial_upper_tail(1, 5, 0.0), 0.0);
    assert_eq!(binomial_upper_tail(1, 5, 1.0), 1.0);
    assert!(binomial_upper_tail(1, 5, f64::NAN).is_nan());
}

#[test]
fn normal_quantile_known_values() {
    assert!((normal_two_sided_quantile(0.05) - 1.959_964).abs() < 1e-4);
    assert!((normal_two_sided_quantile(0.01) - 2.575_829).abs() < 1e-4);
    assert!((normal_two_sided_quantile(0.05 / 46.0) - 3.265).abs() < 1e-2);
    assert!(normal_two_sided_quantile(1.0) < 1e-12);
}

#[test]
fn auc_cases() {
    assert_eq!(mann_whitney_auc(&[3.0, 4.0], &[1.0, 2.0]), Some(1.0));
    assert_eq!(mann_whitney_auc(&[1.0, 2.0], &[3.0, 4.0]), Some(0.0));
    assert_eq!(mann_whitney_auc(&[1.0, 1.0], &[1.0, 1.0]), Some(0.5));
    assert_eq!(mann_whitney_auc(&[f64::NAN], &[1.0]), None);
    assert_eq!(mann_whitney_auc(&[1.0], &[]), None);
    let a = mann_whitney_auc(&[2.0, 5.0, f64::INFINITY], &[1.0, 3.0, 4.0]).unwrap();
    assert!((a - 4.0 / 6.0).abs() < 1e-12);
}

#[test]
fn youden_threshold_cases() {
    assert_eq!(
        youden_threshold(&[8.0, 9.0, 10.0], &[1.0, 2.0, 3.0], true),
        Some(8.0)
    );
    assert_eq!(youden_threshold(&[1.0, 2.0], &[8.0, 9.0], false), Some(2.0));
    assert_eq!(youden_threshold(&[], &[1.0], true), None);
    assert_eq!(youden_threshold(&[f64::NAN], &[f64::NAN], true), None);
    // equal J: the last (highest) candidate wins
    assert_eq!(youden_threshold(&[5.0], &[5.0], true), Some(5.0));
}

#[test]
fn cluster_se_sums_residuals_within_clusters() {
    assert!(cluster_mean_se(&[(0, 1.0)]).is_none());
    // mean 2; cluster 0 residuals -1 and +1 cancel, cluster 1 residual 0
    assert_eq!(
        cluster_mean_se(&[(0, 1.0), (0, 3.0), (1, 2.0)]),
        Some((2.0, 0.0))
    );
    // independent clusters: sqrt(1 + 1) / 2
    let (m, se) = cluster_mean_se(&[(0, 1.0), (1, 3.0)]).unwrap();
    assert_eq!(m, 2.0);
    assert_eq!(se, 2f64.sqrt() / 2.0);
}
