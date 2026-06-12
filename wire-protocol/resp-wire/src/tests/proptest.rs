//! Property-based tests for RESP parsing.

use crate::types::bulk_string::{BulkString, BulkStringValue};
use crate::types::double::Double;
use crate::types::integer::{Integer, parse_signed_integer};
use crate::{RespParseSync, write_bulk_string, write_double, write_integer};
use proptest::prelude::*;
use wire_stream::SliceStream;

// =============================================================================
// Integer Parsing Properties
// =============================================================================

proptest! {
    /// Roundtrip: write_integer -> parse -> same value
    /// Note: write_integer takes i64, so this tests the i64 subset of i128.
    #[test]
    fn integer_roundtrip(value in any::<i64>()) {
        let mut buf = Vec::new();
        write_integer(&mut buf, value).expect("write failed");

        let stream = SliceStream::new(&buf);
        let parsed = Integer::parse_sync(&stream).expect("parse failed");

        prop_assert_eq!(parsed, value as i128);
    }

    /// Test parse_signed_integer directly with i128 values.
    /// This catches edge cases that write_integer (i64) cannot produce.
    #[test]
    fn parse_signed_integer_valid(value in any::<i128>()) {
        let s = value.to_string();
        let result = parse_signed_integer(s.as_bytes());

        prop_assert_eq!(result.expect("parse failed"), value);
    }

    /// Specific edge cases for integer boundaries.
    #[test]
    fn integer_boundaries(value in prop_oneof![
        Just(0i128),
        Just(1i128),
        Just(-1i128),
        Just(i64::MAX as i128),
        Just(i64::MIN as i128),
        Just(i128::MAX),
        Just(i128::MIN),
        Just(i128::MAX - 1),
        Just(i128::MIN + 1),
    ]) {
        let s = value.to_string();
        let result = parse_signed_integer(s.as_bytes());

        prop_assert_eq!(result.expect("parse failed"), value);
    }

    /// Test that invalid integer formats are rejected.
    #[test]
    fn integer_rejects_invalid(input in "[^0-9+-].*|\\+|\\-|") {
        let result = parse_signed_integer(input.as_bytes());
        prop_assert!(result.is_err());
    }
}

// =============================================================================
// Double Parsing Properties
// =============================================================================

proptest! {
    /// Roundtrip: write_double -> parse -> same value (for finite, non-NaN values)
    #[test]
    fn double_roundtrip_finite(value in any::<f64>().prop_filter("finite", |v| v.is_finite())) {
        let mut buf = Vec::new();
        write_double(&mut buf, value).expect("write failed");

        let stream = SliceStream::new(&buf);
        let parsed = Double::parse_sync(&stream).expect("parse failed");

        // For finite values, exact equality should hold after roundtrip
        prop_assert_eq!(parsed, value);
    }

    /// Test infinity roundtrip.
    #[test]
    fn double_infinity(value in prop_oneof![Just(f64::INFINITY), Just(f64::NEG_INFINITY)]) {
        let mut buf = Vec::new();
        write_double(&mut buf, value).expect("write failed");

        let stream = SliceStream::new(&buf);
        let parsed = Double::parse_sync(&stream).expect("parse failed");

        prop_assert_eq!(parsed, value);
    }

    /// Test NaN roundtrip (NaN != NaN, so check with is_nan).
    #[test]
    fn double_nan(_dummy in Just(())) {
        let mut buf = Vec::new();
        write_double(&mut buf, f64::NAN).expect("write failed");

        let stream = SliceStream::new(&buf);
        let parsed = Double::parse_sync(&stream).expect("parse failed");

        prop_assert!(parsed.is_nan());
    }

    /// Test negative zero.
    #[test]
    fn double_negative_zero(_dummy in Just(())) {
        let mut buf = Vec::new();
        write_double(&mut buf, -0.0_f64).expect("write failed");

        let stream = SliceStream::new(&buf);
        let parsed = Double::parse_sync(&stream).expect("parse failed");

        // -0.0 == 0.0 in Rust, but we can check sign bit
        prop_assert!(parsed == 0.0);
        // Note: -0.0 may or may not preserve sign through Display formatting
    }
}

// =============================================================================
// Explicit edge case tests (non-proptest, for documentation)
// =============================================================================

#[test]
fn integer_i128_max() {
    let s = i128::MAX.to_string();
    let result = parse_signed_integer(s.as_bytes());
    assert_eq!(result.expect("parse failed"), i128::MAX);
}

#[test]
fn integer_i128_min() {
    let s = i128::MIN.to_string();
    let result = parse_signed_integer(s.as_bytes());
    assert_eq!(result.expect("parse failed"), i128::MIN);
}

#[test]
fn double_special_strings() {
    // Test that the parser accepts the exact strings it should
    for (input, expected) in [(",inf\r\n", f64::INFINITY), (",-inf\r\n", f64::NEG_INFINITY)] {
        let stream = SliceStream::new(input.as_bytes());
        let parsed = Double::parse_sync(&stream).expect("parse failed");
        assert_eq!(parsed, expected);
    }

    // NaN
    let stream = SliceStream::new(b",nan\r\n");
    let parsed = Double::parse_sync(&stream).expect("parse failed");
    assert!(parsed.is_nan());
}

// =============================================================================
// BulkString Parsing Properties
// =============================================================================

proptest! {
    /// Roundtrip: write_bulk_string -> parse -> collect -> same bytes
    #[test]
    fn bulk_string_roundtrip(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        let mut buf = Vec::new();
        write_bulk_string(&mut buf, &data).expect("write failed");

        let stream = SliceStream::new(&buf);
        let value = BulkString::parse_sync(&stream).expect("parse failed");

        match value {
            BulkStringValue::Null => {
                prop_assert!(false, "Expected string, got null");
            }
            BulkStringValue::String(mut reader) => {
                let mut collected = Vec::new();
                while let Some(chunk) = reader.next_sync().expect("read failed") {
                    collected.extend_from_slice(&chunk);
                }
                prop_assert!(reader.is_finished(), "Reader should be finished");
                prop_assert_eq!(collected, data);
            }
        }
    }

    /// Length declared in header matches actual content length
    #[test]
    fn bulk_string_length_consistency(data in prop::collection::vec(any::<u8>(), 0..500)) {
        let mut buf = Vec::new();
        write_bulk_string(&mut buf, &data).expect("write failed");

        let stream = SliceStream::new(&buf);
        let value = BulkString::parse_sync(&stream).expect("parse failed");

        if let BulkStringValue::String(reader) = value {
            prop_assert_eq!(reader.remaining(), data.len());
        }
    }

    /// Empty bulk string ($0\r\n\r\n) parses correctly
    #[test]
    fn bulk_string_empty(_dummy in Just(())) {
        let buf = b"$0\r\n\r\n";
        let stream = SliceStream::new(buf.as_slice());
        let value = BulkString::parse_sync(&stream).expect("parse failed");

        match value {
            BulkStringValue::Null => prop_assert!(false, "Expected empty string, got null"),
            BulkStringValue::String(mut reader) => {
                prop_assert_eq!(reader.remaining(), 0);
                let chunk = reader.next_sync().expect("read failed");
                prop_assert!(chunk.is_none());
                prop_assert!(reader.is_finished());
            }
        }
    }
}

// =============================================================================
// BulkString edge cases
// =============================================================================

#[test]
fn bulk_string_null() {
    let buf = b"$-1\r\n";
    let stream = SliceStream::new(buf.as_slice());
    let value = BulkString::parse_sync(&stream).expect("parse failed");

    assert!(matches!(value, BulkStringValue::Null));
}

#[test]
fn bulk_string_with_crlf_in_content() {
    // Bulk strings can contain CRLF in their content
    let data = b"hello\r\nworld";
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, data).expect("write failed");

    let stream = SliceStream::new(&buf);
    let value = BulkString::parse_sync(&stream).expect("parse failed");

    match value {
        BulkStringValue::Null => panic!("Expected string, got null"),
        BulkStringValue::String(mut reader) => {
            let mut collected = Vec::new();
            while let Some(chunk) = reader.next_sync().expect("read failed") {
                collected.extend_from_slice(&chunk);
            }
            assert_eq!(collected, data);
        }
    }
}

#[test]
fn bulk_string_binary_data() {
    // Bulk strings can contain any binary data including null bytes
    let data: Vec<u8> = (0u8..=255).collect();
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, &data).expect("write failed");

    let stream = SliceStream::new(&buf);
    let value = BulkString::parse_sync(&stream).expect("parse failed");

    match value {
        BulkStringValue::Null => panic!("Expected string, got null"),
        BulkStringValue::String(mut reader) => {
            let mut collected = Vec::new();
            while let Some(chunk) = reader.next_sync().expect("read failed") {
                collected.extend_from_slice(&chunk);
            }
            assert_eq!(collected, data);
        }
    }
}
