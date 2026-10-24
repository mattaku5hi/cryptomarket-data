#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_math::indicators::{ema_first_seeded, ema_sma_seeded, macd_hist_12_26_9, rsi_wilder};

#[test]
fn first_seeded_ema_starts_at_the_first_value() {
    let e = ema_first_seeded(&[2.0, 4.0, 4.0], 3);
    assert_eq!(e, vec![2.0, 3.0, 3.5]);
    assert!(ema_first_seeded(&[], 3).is_empty());
}

#[test]
fn first_seeded_ema_restarts_after_nan() {
    let e = ema_first_seeded(&[f64::NAN, 2.0, f64::NAN, 8.0, 10.0], 3);
    assert!(e[0].is_nan());
    assert_eq!(e[1], 2.0);
    assert!(e[2].is_nan());
    assert_eq!(e[3], 8.0);
    assert_eq!(e[4], 9.0);
}

#[test]
fn sma_seeded_ema_warms_up_for_n_values() {
    let e = ema_sma_seeded(&[1.0, 2.0, 3.0, 6.0], 3);
    assert_eq!(e, vec![None, None, Some(2.0), Some(4.0)]);
    assert_eq!(ema_sma_seeded(&[1.0, 2.0], 3), vec![None, None]);
    assert_eq!(ema_sma_seeded(&[1.0, 2.0], 0), vec![None, None]);
    let nan = ema_sma_seeded(&[1.0, f64::NAN, 3.0, 4.0], 2);
    assert!(nan[1].unwrap().is_nan() && nan[3].unwrap().is_nan());
}

#[test]
fn rsi_warm_up_and_extremes() {
    let up: Vec<f64> = (0..30).map(f64::from).collect();
    let r = rsi_wilder(&up, 14);
    assert!(r[13].is_nan());
    assert_eq!(r[14], 100.0);
    assert_eq!(r[29], 100.0);
    let dn: Vec<f64> = (0..30).map(|i| 100.0 - f64::from(i)).collect();
    assert_eq!(rsi_wilder(&dn, 14)[29], 0.0);
    assert!(rsi_wilder(&up[..14], 14).iter().all(|v| v.is_nan()));
    assert!(rsi_wilder(&[], 14).is_empty());
}

#[test]
fn rsi_matches_hand_computed_wilder_smoothing() {
    // changes +1, -1 (n = 2): ag = al = 0.5 -> 50; then +2: ag = 1.25, al = 0.25
    let r = rsi_wilder(&[1.0, 2.0, 1.0, 3.0], 2);
    assert_eq!(r[2], 50.0);
    assert!((r[3] - (100.0 - 100.0 / (1.0 + 5.0))).abs() < 1e-12);
}

#[test]
fn macd_hist_warm_up_and_sign() {
    let mut c: Vec<f64> = (0..60).map(|i| 100.0 + f64::from(i)).collect();
    c.extend((0..20).map(|i| 159.0 - 3.0 * f64::from(i)));
    let h = macd_hist_12_26_9(&c);
    assert_eq!(h.len(), c.len());
    assert!(h[..35].iter().all(|v| v.is_nan()));
    assert!(h[55] > 0.0 && h[79] < 0.0);
    // flat prices give a zero histogram after warm-up
    assert!(
        macd_hist_12_26_9(&[5.0; 40])[35..]
            .iter()
            .all(|v| *v == 0.0)
    );
}
