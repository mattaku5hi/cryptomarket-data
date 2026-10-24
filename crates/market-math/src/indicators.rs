//! Price indicators: EMA, Wilder RSI and MACD histogram.

/// EMA with `alpha = 2 / (n + 1)`, seeded by the first value (no warm-up NaNs).
///
/// While the running value is NaN the next input replaces it, so leading NaNs pass
/// through and the average restarts at the first value after them. A NaN after the
/// start makes that output NaN and restarts at the following value.
pub fn ema_first_seeded(x: &[f64], n: usize) -> Vec<f64> {
    let a = 2.0 / (n as f64 + 1.0);
    let mut out = Vec::with_capacity(x.len());
    let mut prev = f64::NAN;
    for v in x {
        prev = if prev.is_nan() {
            *v
        } else {
            prev + a * (v - prev)
        };
        out.push(prev);
    }
    out
}

/// EMA with `alpha = 2 / (n + 1)` seeded by the simple mean of the first `n` values.
///
/// `None` before index `n - 1`; the value at `n - 1` is that mean. No NaN handling: a NaN
/// input makes every later value NaN. `n = 0` gives all `None`.
pub fn ema_sma_seeded(x: &[f64], n: usize) -> Vec<Option<f64>> {
    let len = x.len();
    let alpha = 2.0 / (n as f64 + 1.0);
    let mut out = vec![None; len];
    let mut prev: Option<f64> = None;
    for i in 0..len {
        if i + 1 == n {
            prev = Some(x[..n].iter().sum::<f64>() / n as f64);
        } else if let Some(p) = prev {
            prev = Some(p + alpha * (x[i] - p));
        }
        out[i] = prev;
    }
    out
}

/// Wilder RSI of closes `c` with period `n`.
///
/// The first average gain and loss are simple means of the first `n` changes, then
/// Wilder smoothing `(avg * (n - 1) + new) / n`. Outputs are NaN before index `n` (all
/// NaN when `c.len() <= n`); 100 whenever the average loss is exactly zero. No NaN
/// handling in the input.
pub fn rsi_wilder(c: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; c.len()];
    if c.len() <= n {
        return out;
    }
    let (mut gain, mut loss) = (0.0, 0.0);
    for i in 1..=n {
        let d = c[i] - c[i - 1];
        gain += d.max(0.0);
        loss += (-d).max(0.0);
    }
    let (mut ag, mut al) = (gain / n as f64, loss / n as f64);
    let rsi = |ag: f64, al: f64| {
        if al == 0.0 {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + ag / al)
        }
    };
    out[n] = rsi(ag, al);
    for i in n + 1..c.len() {
        let d = c[i] - c[i - 1];
        ag = (ag * (n - 1) as f64 + d.max(0.0)) / n as f64;
        al = (al * (n - 1) as f64 + (-d).max(0.0)) / n as f64;
        out[i] = rsi(ag, al);
    }
    out
}

/// MACD(12, 26, 9) histogram: `macd - signal` with `macd = ema12 - ema26` and
/// `signal = ema9(macd)`, every EMA from [`ema_first_seeded`]. The first 35 values are
/// forced to NaN as warm-up.
pub fn macd_hist_12_26_9(c: &[f64]) -> Vec<f64> {
    let (e12, e26) = (ema_first_seeded(c, 12), ema_first_seeded(c, 26));
    let macd: Vec<f64> = e12.iter().zip(&e26).map(|(a, b)| a - b).collect();
    let sig = ema_first_seeded(&macd, 9);
    macd.iter()
        .zip(&sig)
        .enumerate()
        .map(|(i, (m, s))| if i < 35 { f64::NAN } else { m - s })
        .collect()
}
