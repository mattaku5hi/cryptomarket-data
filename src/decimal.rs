//! Exact lexical decimal values; no float or exponent notation.
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalError(&'static str);
impl std::fmt::Display for DecimalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for DecimalError {}
fn err(message: &'static str) -> DecimalError {
    DecimalError(message)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactDecimal(Decimal);
impl ExactDecimal {
    pub fn parse(s: &str) -> Result<Self, DecimalError> {
        if s.is_empty() {
            return Err(err("empty decimal"));
        }
        let bytes = s.as_bytes();
        let mut i = usize::from(bytes.first() == Some(&b'-'));
        if i == bytes.len() {
            return Err(err("invalid decimal"));
        }
        let integer_start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == integer_start || (i - integer_start > 1 && bytes.get(integer_start) == Some(&b'0'))
        {
            return Err(err("invalid integer digits"));
        }
        if i < bytes.len() {
            if bytes.get(i) != Some(&b'.') {
                return Err(err("invalid decimal character"));
            }
            i += 1;
            let fraction_start = i;
            while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            let fractional_digits = i - fraction_start;
            if fractional_digits == 0 || fractional_digits > 18 {
                return Err(err("invalid fractional precision"));
            }
        }
        if i != bytes.len() {
            return Err(err("invalid decimal character"));
        }
        let value = Decimal::from_str_exact(s)
            .map_err(|_| err("decimal outside supported exact representation"))?;
        Ok(Self(value))
    }
    pub fn value(&self) -> Decimal {
        self.0
    }
    pub fn as_string(&self) -> String {
        self.0.to_string()
    }
}
