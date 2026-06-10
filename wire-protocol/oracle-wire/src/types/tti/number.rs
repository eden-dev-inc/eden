//! Oracle NUMBER encoding and decoding.
//!
//! Oracle NUMBER is a variable-length numeric format that can represent
//! values from -10^125 to 10^125 with up to 38 digits of precision.
//!
//! # Wire Format
//!
//! The NUMBER format consists of:
//! - 1 byte: length (0-22)
//! - 1 byte: exponent (with sign encoding)
//! - 0-20 bytes: mantissa digits (base-100 encoded)
//!
//! # Exponent Encoding
//!
//! - For positive numbers: exponent + 193 (range 193-255 for exp -62 to 62)
//! - For negative numbers: 62 - exponent (range 1-63 for exp -62 to 62)
//! - Zero is encoded as a single byte: 0x80
//!
//! # Mantissa Encoding
//!
//! - For positive numbers: digit pairs as (value + 1), terminated with 102 if odd length
//! - For negative numbers: digit pairs as (101 - value), terminated with 102

/// Oracle NUMBER value.
///
/// This can represent any Oracle NUMBER from the wire protocol.
#[derive(Clone, Debug, PartialEq)]
pub enum OracleNumber {
    /// Zero value.
    Zero,
    /// Positive infinity.
    PositiveInfinity,
    /// Negative infinity.
    NegativeInfinity,
    /// A finite positive number.
    Positive {
        /// Base-10 exponent.
        exponent: i16,
        /// Mantissa digits (each 0-99, base-100).
        mantissa: Vec<u8>,
    },
    /// A finite negative number.
    Negative {
        /// Base-10 exponent.
        exponent: i16,
        /// Mantissa digits (each 0-99, base-100).
        mantissa: Vec<u8>,
    },
}

/// Error when parsing an Oracle NUMBER.
#[derive(Clone, Debug, thiserror::Error)]
pub enum NumberParseError {
    #[error("number data is empty")]
    Empty,
    #[error("invalid exponent byte: {0}")]
    InvalidExponent(u8),
    #[error("invalid mantissa byte: {0} at position {1}")]
    InvalidMantissa(u8, usize),
    #[error("mantissa digit {0} out of range (0-99) at position {1}")]
    MantissaDigitOutOfRange(u8, usize),
    #[error("number too long: {0} bytes")]
    TooLong(usize),
    #[error("exponent {0} out of range [-65, 62]")]
    ExponentOutOfRange(i16),
    #[error("mantissa is empty for non-zero number")]
    EmptyMantissa,
    #[error("number would overflow i64")]
    I64Overflow,
    #[error("number would overflow i128")]
    I128Overflow,
}

impl OracleNumber {
    /// The maximum wire length for an Oracle NUMBER (22 bytes).
    pub const MAX_WIRE_LENGTH: usize = 22;

    /// The maximum mantissa digits (base-100, so 20 digit pairs = 40 decimal digits).
    pub const MAX_MANTISSA_DIGITS: usize = 20;

    /// Maximum exponent value (10^62 base-100 = 10^124 decimal).
    pub const MAX_EXPONENT: i16 = 62;

    /// Minimum exponent value (10^-65 base-100 = 10^-130 decimal).
    pub const MIN_EXPONENT: i16 = -65;

    /// Parse an Oracle NUMBER from wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, NumberParseError> {
        if data.is_empty() {
            return Err(NumberParseError::Empty);
        }

        if data.len() > Self::MAX_WIRE_LENGTH {
            return Err(NumberParseError::TooLong(data.len()));
        }

        // Check for special values
        if data.len() == 1 {
            return match data[0] {
                0x80 => Ok(Self::Zero),
                _ => Err(NumberParseError::InvalidExponent(data[0])),
            };
        }

        // Check for infinity
        if data.len() == 2 && data[1] == 0x65 {
            return match data[0] {
                0xFF => Ok(Self::PositiveInfinity),
                0x00 => Ok(Self::NegativeInfinity),
                _ => Err(NumberParseError::InvalidExponent(data[0])),
            };
        }

        let exp_byte = data[0];
        let is_positive = exp_byte >= 0x80;

        // Decode exponent
        let exponent = if is_positive {
            // Positive: exp_byte = exponent + 193
            (exp_byte as i16) - 193
        } else {
            // Negative: exp_byte = 62 - exponent
            62 - (exp_byte as i16)
        };

        // Decode mantissa
        let mut mantissa = Vec::with_capacity(data.len() - 1);

        for (i, &byte) in data[1..].iter().enumerate() {
            if is_positive {
                // Positive: digit = byte - 1 (range 1-100 -> 0-99)
                // Terminator is 102
                if byte == 102 {
                    break;
                }
                if !(1..=100).contains(&byte) {
                    return Err(NumberParseError::InvalidMantissa(byte, i));
                }
                let digit = byte - 1;
                if digit > 99 {
                    return Err(NumberParseError::MantissaDigitOutOfRange(digit, i));
                }
                mantissa.push(digit);
            } else {
                // Negative: digit = 101 - byte (range 2-101 -> 0-99)
                // Terminator is 102
                if byte == 102 {
                    break;
                }
                if !(2..=101).contains(&byte) {
                    return Err(NumberParseError::InvalidMantissa(byte, i));
                }
                let digit = 101 - byte;
                if digit > 99 {
                    return Err(NumberParseError::MantissaDigitOutOfRange(digit, i));
                }
                mantissa.push(digit);
            }
        }

        if is_positive {
            Ok(Self::Positive { exponent, mantissa })
        } else {
            Ok(Self::Negative { exponent, mantissa })
        }
    }

    /// Parse from bytes with strict validation.
    ///
    /// This performs additional checks beyond basic parsing:
    /// - Validates exponent is within Oracle's documented range
    /// - Validates mantissa contains only valid digits (0-99)
    pub fn from_bytes_strict(data: &[u8]) -> Result<Self, NumberParseError> {
        let num = Self::from_bytes(data)?;

        // Validate exponent range for non-special values
        match &num {
            Self::Positive { exponent, mantissa } | Self::Negative { exponent, mantissa } => {
                if *exponent < Self::MIN_EXPONENT || *exponent > Self::MAX_EXPONENT {
                    return Err(NumberParseError::ExponentOutOfRange(*exponent));
                }
                // Validate mantissa digits
                for (i, &digit) in mantissa.iter().enumerate() {
                    if digit > 99 {
                        return Err(NumberParseError::MantissaDigitOutOfRange(digit, i));
                    }
                }
            }
            _ => {}
        }

        Ok(num)
    }

    /// Encode to Oracle NUMBER wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Zero => vec![0x80],
            Self::PositiveInfinity => vec![0xFF, 0x65],
            Self::NegativeInfinity => vec![0x00, 0x65],
            Self::Positive { exponent, mantissa } => {
                let mut bytes = Vec::with_capacity(mantissa.len() + 2);
                // Encode exponent: exp_byte = exponent + 193
                bytes.push((exponent + 193) as u8);
                // Encode mantissa: byte = digit + 1
                for &digit in mantissa {
                    bytes.push(digit + 1);
                }
                // Add terminator if needed (odd number of significant digits)
                if mantissa.len() % 2 == 1 {
                    bytes.push(102);
                }
                bytes
            }
            Self::Negative { exponent, mantissa } => {
                let mut bytes = Vec::with_capacity(mantissa.len() + 2);
                // Encode exponent: exp_byte = 62 - exponent
                bytes.push((62 - exponent) as u8);
                // Encode mantissa: byte = 101 - digit
                for &digit in mantissa {
                    bytes.push(101 - digit);
                }
                // Always add terminator for negative numbers
                bytes.push(102);
                bytes
            }
        }
    }

    /// Check if this number is zero.
    pub fn is_zero(&self) -> bool {
        matches!(self, Self::Zero)
    }

    /// Check if this number is positive (including positive infinity).
    pub fn is_positive(&self) -> bool {
        matches!(self, Self::Positive { .. } | Self::PositiveInfinity)
    }

    /// Check if this number is negative (including negative infinity).
    pub fn is_negative(&self) -> bool {
        matches!(self, Self::Negative { .. } | Self::NegativeInfinity)
    }

    /// Check if this is an infinity value.
    pub fn is_infinite(&self) -> bool {
        matches!(self, Self::PositiveInfinity | Self::NegativeInfinity)
    }

    /// Try to convert to i64.
    ///
    /// Returns None if the value is too large, infinite, or has a fractional part.
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Self::Zero => Some(0),
            Self::PositiveInfinity | Self::NegativeInfinity => None,
            Self::Positive { exponent, mantissa } | Self::Negative { exponent, mantissa } => {
                let is_negative = matches!(self, Self::Negative { .. });

                // Each mantissa digit is base-100, so 2 decimal digits
                // exponent is the power of 100 of the first mantissa digit
                let total_digits = (mantissa.len() * 2) as i16;
                let decimal_position = (*exponent + 1) * 2; // Position of decimal point from left

                // If decimal point is in the middle of mantissa, there's a fractional part
                if decimal_position < total_digits {
                    return None;
                }

                // Build the integer value
                let mut value: i64 = 0;
                for &digit in mantissa {
                    value = value.checked_mul(100)?.checked_add(digit as i64)?;
                }

                // Scale by remaining exponent
                let remaining_exp = decimal_position - total_digits;
                for _ in 0..remaining_exp {
                    value = value.checked_mul(100)?;
                }

                if is_negative { Some(-value) } else { Some(value) }
            }
        }
    }

    /// Try to convert to i128.
    ///
    /// Returns None if the value is too large, infinite, or has a fractional part.
    /// This supports larger numbers than `to_i64`.
    pub fn to_i128(&self) -> Option<i128> {
        match self {
            Self::Zero => Some(0),
            Self::PositiveInfinity | Self::NegativeInfinity => None,
            Self::Positive { exponent, mantissa } | Self::Negative { exponent, mantissa } => {
                let is_negative = matches!(self, Self::Negative { .. });

                let total_digits = (mantissa.len() * 2) as i16;
                let decimal_position = (*exponent + 1) * 2;

                // If decimal point is in the middle of mantissa, there's a fractional part
                if decimal_position < total_digits {
                    return None;
                }

                // Build the integer value
                let mut value: i128 = 0;
                for &digit in mantissa {
                    value = value.checked_mul(100)?.checked_add(digit as i128)?;
                }

                // Scale by remaining exponent
                let remaining_exp = decimal_position - total_digits;
                for _ in 0..remaining_exp {
                    value = value.checked_mul(100)?;
                }

                if is_negative { Some(-value) } else { Some(value) }
            }
        }
    }

    /// Check if this number has a fractional part.
    pub fn has_fraction(&self) -> bool {
        match self {
            Self::Zero | Self::PositiveInfinity | Self::NegativeInfinity => false,
            Self::Positive { exponent, mantissa } | Self::Negative { exponent, mantissa } => {
                let total_digits = (mantissa.len() * 2) as i16;
                let decimal_position = (*exponent + 1) * 2;
                decimal_position < total_digits
            }
        }
    }

    /// Get the number of significant decimal digits.
    pub fn precision(&self) -> usize {
        match self {
            Self::Zero => 1,
            Self::PositiveInfinity | Self::NegativeInfinity => 0,
            Self::Positive { mantissa, .. } | Self::Negative { mantissa, .. } => {
                // Each mantissa digit is 2 decimal digits, but we need to handle
                // leading zeros in first digit and trailing zeros in last digit
                if mantissa.is_empty() {
                    return 0;
                }

                let mut digits = mantissa.len() * 2;

                // First digit may have a leading zero (e.g., 05 = 5)
                if mantissa[0] < 10 {
                    digits -= 1;
                }

                digits
            }
        }
    }

    /// Try to convert to f64.
    pub fn to_f64(&self) -> f64 {
        match self {
            Self::Zero => 0.0,
            Self::PositiveInfinity => f64::INFINITY,
            Self::NegativeInfinity => f64::NEG_INFINITY,
            Self::Positive { exponent, mantissa } | Self::Negative { exponent, mantissa } => {
                let is_negative = matches!(self, Self::Negative { .. });

                // Build mantissa as f64
                let mut value = 0.0_f64;
                for &digit in mantissa {
                    value = value * 100.0 + (digit as f64);
                }

                // Apply exponent: exponent is power of 100 of first digit
                // After reading n mantissa digits, we need to divide by 100^(n-1) then multiply by 100^exp
                let exp_adjust = (*exponent as f64) - (mantissa.len() as f64 - 1.0);
                value *= 100.0_f64.powf(exp_adjust);

                if is_negative { -value } else { value }
            }
        }
    }

    /// Create from an i64 value.
    pub fn from_i64(value: i64) -> Self {
        if value == 0 {
            return Self::Zero;
        }

        let is_negative = value < 0;
        let mut abs_value = value.unsigned_abs();

        // Convert to base-100 digits
        let mut digits = Vec::new();
        while abs_value > 0 {
            digits.push((abs_value % 100) as u8);
            abs_value /= 100;
        }
        digits.reverse();

        // Exponent is position of first digit pair
        let exponent = (digits.len() as i16) - 1;

        if is_negative {
            Self::Negative { exponent, mantissa: digits }
        } else {
            Self::Positive { exponent, mantissa: digits }
        }
    }

    /// Create from an f64 value.
    ///
    /// Note: This is a simplified implementation that converts to string first.
    /// For exact precision, consider using a decimal library.
    pub fn from_f64(value: f64) -> Self {
        if value == 0.0 {
            return Self::Zero;
        }
        if value.is_infinite() {
            return if value.is_sign_positive() {
                Self::PositiveInfinity
            } else {
                Self::NegativeInfinity
            };
        }
        if value.is_nan() {
            // Oracle doesn't have NaN, treat as zero
            return Self::Zero;
        }

        // For integer values, use the integer path
        if value.fract() == 0.0 && value.abs() < i64::MAX as f64 {
            return Self::from_i64(value as i64);
        }

        // Convert through string representation for proper decimal handling
        let is_negative = value < 0.0;
        let abs_value = value.abs();

        // Get the exponent (power of 10)
        let exp10 = abs_value.log10().floor() as i16;

        // Normalize to get significand between 1 and 100
        let normalized = abs_value / 10f64.powi(exp10 as i32 - (exp10 % 2).abs() as i32);

        // Convert to base-100 mantissa (limited precision)
        let mut mantissa = Vec::new();
        let mut working = normalized;
        for _ in 0..10 {
            // Limit to 10 digit pairs (20 decimal digits)
            let digit = (working.floor() as u8).min(99);
            mantissa.push(digit);
            working = (working - digit as f64) * 100.0;
            if working < 0.01 {
                break;
            }
        }

        // Oracle exponent is in base-100 (pairs of decimal digits)
        let oracle_exp = (exp10 + 1) / 2;

        if is_negative {
            Self::Negative { exponent: oracle_exp, mantissa }
        } else {
            Self::Positive { exponent: oracle_exp, mantissa }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero() {
        let zero = OracleNumber::from_bytes(&[0x80]).unwrap();
        assert!(zero.is_zero());
        assert_eq!(zero.to_bytes(), vec![0x80]);
        assert_eq!(zero.to_i64(), Some(0));
        assert_eq!(zero.to_f64(), 0.0);
    }

    #[test]
    fn test_positive_integer() {
        // Create 123
        let num = OracleNumber::from_i64(123);
        assert!(num.is_positive());
        assert_eq!(num.to_i64(), Some(123));

        // Create 10000
        let num = OracleNumber::from_i64(10000);
        assert_eq!(num.to_i64(), Some(10000));
    }

    #[test]
    fn test_negative_integer() {
        let num = OracleNumber::from_i64(-456);
        assert!(num.is_negative());
        assert_eq!(num.to_i64(), Some(-456));
    }

    #[test]
    fn test_roundtrip() {
        for value in [0, 1, -1, 100, -100, 12345, -12345, 1000000, -1000000] {
            let num = OracleNumber::from_i64(value);
            let bytes = num.to_bytes();
            let parsed = OracleNumber::from_bytes(&bytes).unwrap();
            assert_eq!(parsed.to_i64(), Some(value), "Failed for {}", value);
        }
    }

    #[test]
    fn test_infinity() {
        let pos_inf = OracleNumber::from_bytes(&[0xFF, 0x65]).unwrap();
        assert!(pos_inf.is_infinite());
        assert!(pos_inf.is_positive());
        assert_eq!(pos_inf.to_f64(), f64::INFINITY);

        let neg_inf = OracleNumber::from_bytes(&[0x00, 0x65]).unwrap();
        assert!(neg_inf.is_infinite());
        assert!(neg_inf.is_negative());
        assert_eq!(neg_inf.to_f64(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_to_i128() {
        // i64::MAX + 1 should work with i128 but not i64
        let large = OracleNumber::from_i64(i64::MAX);
        assert_eq!(large.to_i64(), Some(i64::MAX));
        assert_eq!(large.to_i128(), Some(i64::MAX as i128));

        // Zero
        let zero = OracleNumber::Zero;
        assert_eq!(zero.to_i128(), Some(0));

        // Infinity returns None
        assert!(OracleNumber::PositiveInfinity.to_i128().is_none());
        assert!(OracleNumber::NegativeInfinity.to_i128().is_none());
    }

    #[test]
    fn test_has_fraction() {
        let whole = OracleNumber::from_i64(123);
        assert!(!whole.has_fraction());

        let frac = OracleNumber::from_f64(123.456);
        assert!(frac.has_fraction());

        assert!(!OracleNumber::Zero.has_fraction());
        assert!(!OracleNumber::PositiveInfinity.has_fraction());
    }

    #[test]
    fn test_precision() {
        let small = OracleNumber::from_i64(5);
        assert_eq!(small.precision(), 1);

        let medium = OracleNumber::from_i64(12345);
        assert_eq!(medium.precision(), 5);

        assert_eq!(OracleNumber::Zero.precision(), 1);
        assert_eq!(OracleNumber::PositiveInfinity.precision(), 0);
    }

    #[test]
    fn test_error_cases() {
        // Empty data
        assert!(matches!(OracleNumber::from_bytes(&[]), Err(NumberParseError::Empty)));

        // Too long
        let long_data = vec![0xC1; 25];
        assert!(matches!(OracleNumber::from_bytes(&long_data), Err(NumberParseError::TooLong(25))));

        // Invalid single byte
        assert!(matches!(OracleNumber::from_bytes(&[0x70]), Err(NumberParseError::InvalidExponent(_))));

        // Invalid infinity format
        assert!(matches!(OracleNumber::from_bytes(&[0x50, 0x65]), Err(NumberParseError::InvalidExponent(_))));
    }

    #[test]
    fn test_boundary_values() {
        // Test with exact powers of 100
        for exp in 0..10 {
            let value = 100i64.pow(exp);
            let num = OracleNumber::from_i64(value);
            assert_eq!(num.to_i64(), Some(value), "Failed for 100^{}", exp);
        }
    }

    #[test]
    fn test_small_integers() {
        // Test single digits
        for i in 0..=99 {
            let num = OracleNumber::from_i64(i);
            assert_eq!(num.to_i64(), Some(i), "Failed for {}", i);
        }

        // Test negative single digits
        for i in 1..=99 {
            let num = OracleNumber::from_i64(-i);
            assert_eq!(num.to_i64(), Some(-i), "Failed for -{}", i);
        }
    }

    #[test]
    fn test_from_f64_edge_cases() {
        // NaN should become zero
        let nan = OracleNumber::from_f64(f64::NAN);
        assert!(nan.is_zero());

        // Very small number
        let tiny = OracleNumber::from_f64(0.0001);
        assert!(tiny.has_fraction());

        // Very large number (but not infinite)
        let large = OracleNumber::from_f64(1e38);
        assert!(large.is_positive());
    }

    #[test]
    fn test_strict_parsing() {
        // Valid number should pass strict parsing
        let valid_bytes = OracleNumber::from_i64(12345).to_bytes();
        assert!(OracleNumber::from_bytes_strict(&valid_bytes).is_ok());

        // Zero should pass
        assert!(OracleNumber::from_bytes_strict(&[0x80]).is_ok());

        // Infinity should pass
        assert!(OracleNumber::from_bytes_strict(&[0xFF, 0x65]).is_ok());
    }
}
