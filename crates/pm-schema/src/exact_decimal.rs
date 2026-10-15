//! Exact stored Decimal arithmetic shared by market and underlying diagnostics.
//!
//! Operations reject unrepresentable results and conservative integer-alignment overflow rather
//! than silently rescaling or rounding. They do not impose the lexical `ExactDecimal` bound.

use rust_decimal::Decimal;

// Decimal checked arithmetic can rescale and round without returning None. These sums must
// retain every represented unit, so align integer mantissas before constructing the result.
/// Add without rounding, returning `None` for unrepresentable or alignment-overflow results.
pub fn exact_add(left: Decimal, right: Decimal) -> Option<Decimal> {
    let left = left.normalize();
    let right = right.normalize();
    let scale = left.scale().max(right.scale());
    let left = scaled_mantissa(left, scale)?;
    let right = scaled_mantissa(right, scale)?;
    decimal_from_mantissa(left.checked_add(right)?, scale)
}

/// Subtract without rounding, returning `None` for unrepresentable or alignment-overflow results.
pub fn exact_sub(left: Decimal, right: Decimal) -> Option<Decimal> {
    exact_add(left, -right)
}

fn scaled_mantissa(value: Decimal, scale: u32) -> Option<i128> {
    let factor = 10_i128.checked_pow(scale.checked_sub(value.scale())?)?;
    value.mantissa().checked_mul(factor)
}

fn decimal_from_mantissa(mut mantissa: i128, mut scale: u32) -> Option<Decimal> {
    while scale > 0 && mantissa.rem_euclid(10) == 0 {
        mantissa = mantissa.div_euclid(10);
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(mantissa, scale).ok()
}
