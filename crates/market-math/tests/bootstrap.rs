#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::integer_division,
    clippy::as_conversions
)]

use market_math::bootstrap::{
    XorShift64, block_bootstrap_flagged_diff, bootstrap_mean_ci, cluster_bootstrap_ratio_ci,
    percentile_ci_95,
};

#[test]
fn xorshift_sequence_is_pinned() {
    let mut r = XorShift64(1);
    assert_eq!(r.next_u64(), 1_082_269_761);
    assert_eq!(r.next_u64(), 1_152_992_998_833_853_505);
    let mut z = XorShift64(0);
    assert_eq!(z.next_u64(), 0);
    let mut b = XorShift64(1);
    assert_eq!(b.below(10), 1_082_269_761 % 10);
}

#[test]
fn percentile_ci_uses_rounded_index() {
    let v: Vec<f64> = (0..101).map(f64::from).collect();
    // round(100 * 0.025) = 3 (2.5 rounds away from zero), round(97.5) = 98
    assert_eq!(percentile_ci_95(v), Some([3.0, 98.0]));
    assert_eq!(percentile_ci_95(Vec::new()), None);
    assert_eq!(percentile_ci_95(vec![7.0]), Some([7.0, 7.0]));
}

fn items() -> Vec<(i64, f64)> {
    (0..200)
        .map(|i: i64| (i / 24, ((i * 37 + i / 5) % 11) as f64 - 4.0))
        .collect()
}

#[test]
fn bootstrap_mean_ci_brackets_the_mean_and_is_deterministic() {
    let items = items();
    let m = items.iter().map(|x| x.1).sum::<f64>() / 200.0;
    for clustered in [false, true] {
        let a = bootstrap_mean_ci(&items, 500, clustered, 7).unwrap();
        assert_eq!(a, bootstrap_mean_ci(&items, 500, clustered, 7).unwrap());
        assert!(a[0] < m && m < a[1], "{a:?} {m}");
        // seed 6 | 1 == 7 | 1
        assert_eq!(a, bootstrap_mean_ci(&items, 500, clustered, 6).unwrap());
    }
    assert!(bootstrap_mean_ci(&items[..1], 10, false, 1).is_none());
    assert!(bootstrap_mean_ci(&items, 0, false, 1).is_none());
}

#[test]
fn clustered_bootstrap_keeps_clusters_whole() {
    // every cluster has the same mean, so every replicate does too
    let items: Vec<(i64, f64)> = (0..40)
        .map(|i| (i / 2, if i % 2 == 0 { 1.0 } else { 3.0 }))
        .collect();
    assert_eq!(bootstrap_mean_ci(&items, 200, true, 3), Some([2.0, 2.0]));
    let single = bootstrap_mean_ci(&items, 200, false, 3).unwrap();
    assert!(single[0] < 2.0 && single[1] > 2.0);
}

#[test]
fn cluster_ratio_ci_needs_five_clusters_and_uses_truncated_index() {
    assert!(cluster_bootstrap_ratio_ci(&[(1.0, 1); 4], 100, 1).is_none());
    let same = [(4.0, 2); 6];
    assert_eq!(cluster_bootstrap_ratio_ci(&same, 100, 1), Some([2.0, 2.0]));
    let days: Vec<(f64, u32)> = (0..30)
        .map(|i| (f64::from(i), 1 + (i % 3) as u32))
        .collect();
    let a = cluster_bootstrap_ratio_ci(&days, 1000, 9).unwrap();
    assert_eq!(a, cluster_bootstrap_ratio_ci(&days, 1000, 9).unwrap());
    let point =
        days.iter().map(|d| d.0).sum::<f64>() / days.iter().map(|d| f64::from(d.1)).sum::<f64>();
    assert!(a[0] < point && point < a[1]);
}

#[test]
fn block_bootstrap_detects_shift_and_is_deterministic() {
    let n = 48 * 20;
    let ret: Vec<f64> = (0..n)
        .map(|k| if k % 48 == 0 { 0.05 } else { 0.0 })
        .collect();
    let flag: Vec<bool> = (0..n).map(|k| k % 48 == 0).collect();
    let ok = vec![true; n];
    let a = block_bootstrap_flagged_diff(&ret, &flag, &ok, 0, n, 48, 500, 3).unwrap();
    let b = block_bootstrap_flagged_diff(&ret, &flag, &ok, 0, n, 48, 500, 3).unwrap();
    assert_eq!(a.0.to_bits(), b.0.to_bits());
    assert_eq!(a.1, b.1);
    assert!(a.0 > 0.04 && a.1[0] > 0.0);
}

#[test]
fn block_bootstrap_undefined_cases() {
    let ret = [0.1, f64::NAN, 0.2];
    let ok = [true; 3];
    // nothing flagged
    assert!(block_bootstrap_flagged_diff(&ret, &[false; 3], &ok, 0, 3, 1, 10, 1).is_none());
    // empty range
    assert!(block_bootstrap_flagged_diff(&ret, &[true; 3], &ok, 2, 2, 1, 10, 1).is_none());
    // only position 1 flagged, and its return is NaN
    assert!(
        block_bootstrap_flagged_diff(&ret, &[false, true, false], &ok, 0, 3, 1, 10, 1).is_none()
    );
}
