# market-math

Zero-dependency numeric helpers for market-data research: summary statistics,
quantiles, trailing windows, price indicators, confidence intervals and seeded
bootstrap resampling.

Every function documents its exact semantics (warm-up, NaN handling, population
or sample variance, quantile index rule). Functions that look alike but differ
in one of these rules are kept as separate, separately named functions so that
consumers get bit-identical results when they switch to this crate. Nothing here
merges two variants into one "improved" definition.

`std` only, `unsafe` forbidden, Rust 1.85.
