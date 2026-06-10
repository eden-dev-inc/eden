//! Tests for Oracle NUMBER encoding/decoding.

use crate::types::tti::number::OracleNumber;

#[test]
fn test_zero_encoding() {
    let zero = OracleNumber::Zero;
    assert_eq!(zero.to_bytes(), vec![0x80]);
    assert!(zero.is_zero());
    assert!(!zero.is_positive());
    assert!(!zero.is_negative());
}

#[test]
fn test_zero_decoding() {
    let parsed = OracleNumber::from_bytes(&[0x80]).unwrap();
    assert!(parsed.is_zero());
}

#[test]
fn test_positive_integers() {
    let test_cases = [
        (1i64, 1.0f64),
        (10, 10.0),
        (99, 99.0),
        (100, 100.0),
        (123, 123.0),
        (1000, 1000.0),
        (12345, 12345.0),
        (1000000, 1000000.0),
    ];

    for (int_val, float_val) in test_cases {
        let num = OracleNumber::from_i64(int_val);
        assert!(num.is_positive(), "Expected positive for {}", int_val);
        assert_eq!(num.to_i64(), Some(int_val), "i64 roundtrip failed for {}", int_val);
        assert!((num.to_f64() - float_val).abs() < 0.001, "f64 conversion failed for {}", int_val);
    }
}

#[test]
fn test_negative_integers() {
    let test_cases = [
        (-1i64, -1.0f64),
        (-10, -10.0),
        (-99, -99.0),
        (-100, -100.0),
        (-123, -123.0),
        (-1000, -1000.0),
        (-12345, -12345.0),
    ];

    for (int_val, float_val) in test_cases {
        let num = OracleNumber::from_i64(int_val);
        assert!(num.is_negative(), "Expected negative for {}", int_val);
        assert_eq!(num.to_i64(), Some(int_val), "i64 roundtrip failed for {}", int_val);
        assert!((num.to_f64() - float_val).abs() < 0.001, "f64 conversion failed for {}", int_val);
    }
}

#[test]
fn test_byte_roundtrip() {
    let test_values = [0, 1, -1, 50, -50, 99, -99, 100, -100, 1234, -1234, 10000, -10000];

    for value in test_values {
        let num = OracleNumber::from_i64(value);
        let bytes = num.to_bytes();
        let parsed = OracleNumber::from_bytes(&bytes).unwrap_or_else(|_| panic!("Parse failed for {}", value));
        let roundtrip = parsed.to_i64();
        assert_eq!(roundtrip, Some(value), "Roundtrip failed for {}", value);
    }
}

#[test]
fn test_infinity() {
    // Positive infinity
    let pos_inf = OracleNumber::from_bytes(&[0xFF, 0x65]).unwrap();
    assert!(pos_inf.is_infinite());
    assert!(pos_inf.is_positive());
    assert_eq!(pos_inf.to_f64(), f64::INFINITY);
    assert_eq!(pos_inf.to_i64(), None);

    // Negative infinity
    let neg_inf = OracleNumber::from_bytes(&[0x00, 0x65]).unwrap();
    assert!(neg_inf.is_infinite());
    assert!(neg_inf.is_negative());
    assert_eq!(neg_inf.to_f64(), f64::NEG_INFINITY);
    assert_eq!(neg_inf.to_i64(), None);

    // Roundtrip
    assert_eq!(OracleNumber::PositiveInfinity.to_bytes(), vec![0xFF, 0x65]);
    assert_eq!(OracleNumber::NegativeInfinity.to_bytes(), vec![0x00, 0x65]);
}

#[test]
fn test_large_numbers() {
    // Test values near i64 limits
    let large_values = [1_000_000_000i64, 10_000_000_000, 100_000_000_000, 1_000_000_000_000];

    for value in large_values {
        let num = OracleNumber::from_i64(value);
        let bytes = num.to_bytes();
        let parsed = OracleNumber::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.to_i64(), Some(value), "Large value roundtrip failed for {}", value);
    }
}

#[test]
fn test_parse_errors() {
    // Empty data
    assert!(OracleNumber::from_bytes(&[]).is_err());

    // Too long
    let too_long = vec![0x80; 30];
    assert!(OracleNumber::from_bytes(&too_long).is_err());
}
