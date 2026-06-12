//! Tests for MongoDB wire protocol read helpers.

use super::stream::TestStream;
use crate::read::MongoReadSyncExt;
use crate::write::bson_type;

// =============================================================================
// BSON Document Read Tests
// =============================================================================

#[test]
fn read_bson_document_minimal() {
    // Minimal BSON document: {}
    let data = [
        0x05, 0x00, 0x00, 0x00, // length = 5
        0x00, // terminator
    ];

    let stream = TestStream::new(&data);
    let doc = stream.read_bson_document_sync().expect("");

    assert_eq!(doc, data);
}

#[test]
fn read_bson_document_with_element() {
    // { "a": 1 }
    let data = [
        0x0C, 0x00, 0x00, 0x00, // length = 12
        0x10, // int32 type
        b'a', 0x00, // key "a"
        0x01, 0x00, 0x00, 0x00, // value = 1
        0x00, // terminator
    ];

    let stream = TestStream::new(&data);
    let doc = stream.read_bson_document_sync().expect("");

    assert_eq!(doc, data);
    assert_eq!(stream.position(), 12);
}

// =============================================================================
// BSON String Read Tests
// =============================================================================

#[test]
fn read_bson_string_basic() {
    // BSON string "hello"
    let data = [
        0x06, 0x00, 0x00, 0x00, // length = 6 (includes null)
        b'h', b'e', b'l', b'l', b'o', 0x00,
    ];

    let stream = TestStream::new(&data);
    let s = stream.read_bson_string_sync().expect("");

    assert_eq!(s, b"hello");
}

#[test]
fn read_bson_string_empty() {
    let data = [
        0x01, 0x00, 0x00, 0x00, // length = 1
        0x00, // just null terminator
    ];

    let stream = TestStream::new(&data);
    let s = stream.read_bson_string_sync().expect("");

    assert_eq!(s, b"");
}

#[test]
fn read_bson_string_unicode() {
    // "日本" in UTF-8
    let data = [
        0x07, 0x00, 0x00, 0x00, // length = 7
        0xE6, 0x97, 0xA5, 0xE6, 0x9C, 0xAC, 0x00,
    ];

    let stream = TestStream::new(&data);
    let s = stream.read_bson_string_sync().expect("");

    assert_eq!(std::str::from_utf8(&s).expect(""), "日本");
}

// =============================================================================
// BSON Binary Read Tests
// =============================================================================

#[test]
fn read_bson_binary_generic() {
    let data = [
        0x04, 0x00, 0x00, 0x00, // length = 4
        0x00, // subtype = generic
        0x01, 0x02, 0x03, 0x04, // data
    ];

    let stream = TestStream::new(&data);
    let (subtype, binary) = stream.read_bson_binary_sync().expect("");

    assert_eq!(subtype, 0x00);
    assert_eq!(binary, [0x01, 0x02, 0x03, 0x04]);
}

#[test]
fn read_bson_binary_uuid() {
    let uuid_bytes = [
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
    ];
    let mut data = vec![
        0x10, 0x00, 0x00, 0x00, // length = 16
        0x04, // subtype = UUID
    ];
    data.extend_from_slice(&uuid_bytes);

    let stream = TestStream::new(&data);
    let (subtype, binary) = stream.read_bson_binary_sync().expect("");

    assert_eq!(subtype, 0x04);
    assert_eq!(binary, uuid_bytes);
}

// =============================================================================
// BSON ObjectId Read Tests
// =============================================================================

#[test]
fn read_bson_object_id_basic() {
    let oid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let stream = TestStream::new(&oid);

    let result = stream.read_bson_object_id_sync().expect("");
    assert_eq!(result, oid);
    assert_eq!(stream.position(), 12);
}

#[test]
fn read_bson_object_id_all_zeros() {
    let oid = [0u8; 12];
    let stream = TestStream::new(&oid);

    let result = stream.read_bson_object_id_sync().expect("");
    assert_eq!(result, oid);
}

#[test]
fn read_bson_object_id_all_ones() {
    let oid = [0xFFu8; 12];
    let stream = TestStream::new(&oid);

    let result = stream.read_bson_object_id_sync().expect("");
    assert_eq!(result, oid);
}

// =============================================================================
// BSON Boolean Read Tests
// =============================================================================

#[test]
fn read_bson_boolean_true() {
    let stream = TestStream::new(&[0x01]);
    assert!(stream.read_bson_boolean_sync().expect(""));
}

#[test]
fn read_bson_boolean_false() {
    let stream = TestStream::new(&[0x00]);
    assert!(!stream.read_bson_boolean_sync().expect(""));
}

#[test]
fn read_bson_boolean_nonzero_is_true() {
    // Any non-zero value should be true
    let stream = TestStream::new(&[0xFF]);
    assert!(stream.read_bson_boolean_sync().expect(""));
}

// =============================================================================
// BSON DateTime Read Tests
// =============================================================================

#[test]
fn read_bson_datetime_positive() {
    let timestamp: i64 = 1234567890123;
    let bytes = timestamp.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_datetime_sync().expect("");
    assert_eq!(result, timestamp);
}

#[test]
fn read_bson_datetime_negative() {
    // Dates before epoch
    let timestamp: i64 = -1000000000;
    let bytes = timestamp.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_datetime_sync().expect("");
    assert_eq!(result, timestamp);
}

#[test]
fn read_bson_datetime_zero() {
    let bytes = 0i64.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_datetime_sync().expect("");
    assert_eq!(result, 0);
}

// =============================================================================
// BSON Timestamp Read Tests
// =============================================================================

#[test]
fn read_bson_timestamp_basic() {
    // Timestamp with increment=100, seconds=1234567890
    let seconds: u32 = 1234567890;
    let increment: u32 = 100;
    let value: u64 = ((increment as u64) << 32) | (seconds as u64);
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_timestamp_sync().expect("");
    assert_eq!(result, value);
}

// =============================================================================
// BSON Decimal128 Read Tests
// =============================================================================

#[test]
fn read_bson_decimal128_basic() {
    let decimal = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x22,
    ]; // represents 0.0
    let stream = TestStream::new(&decimal);

    let result = stream.read_bson_decimal128_sync().expect("");
    assert_eq!(result, decimal);
}

// =============================================================================
// BSON Double Read Tests
// =============================================================================

#[test]
fn read_bson_double_positive() {
    let value: f64 = 3.125;
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_double_sync().expect("");
    assert!((result - value).abs() < f64::EPSILON);
}

#[test]
fn read_bson_double_negative() {
    let value: f64 = -2.521;
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_double_sync().expect("");
    assert!((result - value).abs() < f64::EPSILON);
}

#[test]
fn read_bson_double_zero() {
    let value: f64 = 0.0;
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_double_sync().expect("");
    assert_eq!(result, 0.0);
}

#[test]
fn read_bson_double_infinity() {
    let value: f64 = f64::INFINITY;
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_double_sync().expect("");
    assert!(result.is_infinite() && result.is_sign_positive());
}

#[test]
fn read_bson_double_nan() {
    let value: f64 = f64::NAN;
    let bytes = value.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let result = stream.read_bson_double_sync().expect("");
    assert!(result.is_nan());
}

// =============================================================================
// Skip Element Tests
// =============================================================================

#[test]
fn skip_bson_element_double() {
    let bytes = 3.12f64.to_le_bytes();
    let stream = TestStream::new(&bytes);

    let skipped = stream.skip_bson_element_sync(bson_type::DOUBLE).expect("");
    assert_eq!(skipped, 8);
    assert_eq!(stream.position(), 8);
}

#[test]
fn skip_bson_element_string() {
    let data = [
        0x06, 0x00, 0x00, 0x00, // length = 6
        b'h', b'e', b'l', b'l', b'o', 0x00,
    ];
    let stream = TestStream::new(&data);

    let skipped = stream.skip_bson_element_sync(bson_type::STRING).expect("");
    assert_eq!(skipped, 10); // 4 (length) + 6 (content)
}

#[test]
fn skip_bson_element_document() {
    let data = [
        0x05, 0x00, 0x00, 0x00, // doc length = 5
        0x00, // terminator
    ];
    let stream = TestStream::new(&data);

    let skipped = stream.skip_bson_element_sync(bson_type::DOCUMENT).expect("");
    assert_eq!(skipped, 5);
}

#[test]
fn skip_bson_element_binary() {
    let data = [
        0x04, 0x00, 0x00, 0x00, // length = 4
        0x00, // subtype
        0x01, 0x02, 0x03, 0x04, // data
    ];
    let stream = TestStream::new(&data);

    let skipped = stream.skip_bson_element_sync(bson_type::BINARY).expect("");
    assert_eq!(skipped, 9); // 4 (length) + 1 (subtype) + 4 (data)
}

#[test]
fn skip_bson_element_object_id() {
    let oid = [0u8; 12];
    let stream = TestStream::new(&oid);

    let skipped = stream.skip_bson_element_sync(bson_type::OBJECT_ID).expect("");
    assert_eq!(skipped, 12);
}

#[test]
fn skip_bson_element_boolean() {
    let stream = TestStream::new(&[0x01]);

    let skipped = stream.skip_bson_element_sync(bson_type::BOOLEAN).expect("");
    assert_eq!(skipped, 1);
}

#[test]
fn skip_bson_element_null() {
    let stream = TestStream::new(&[]);

    let skipped = stream.skip_bson_element_sync(bson_type::NULL).expect("");
    assert_eq!(skipped, 0);
}

#[test]
fn skip_bson_element_int32() {
    let stream = TestStream::new(42i32.to_le_bytes());

    let skipped = stream.skip_bson_element_sync(bson_type::INT32).expect("");
    assert_eq!(skipped, 4);
}

#[test]
fn skip_bson_element_int64() {
    let stream = TestStream::new(42i64.to_le_bytes());

    let skipped = stream.skip_bson_element_sync(bson_type::INT64).expect("");
    assert_eq!(skipped, 8);
}

#[test]
fn skip_bson_element_decimal128() {
    let stream = TestStream::new(&[0u8; 16]);

    let skipped = stream.skip_bson_element_sync(bson_type::DECIMAL128).expect("");
    assert_eq!(skipped, 16);
}

// =============================================================================
// Sequential Read Tests
// =============================================================================

#[test]
fn sequential_reads() {
    let mut data = Vec::new();

    // First: ObjectId (12 bytes)
    data.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

    // Second: Boolean (1 byte)
    data.push(0x01);

    // Third: Double (8 bytes)
    data.extend_from_slice(&3.15f64.to_le_bytes());

    let stream = TestStream::new(&data);

    // Read ObjectId
    let oid = stream.read_bson_object_id_sync().expect("");
    assert_eq!(oid, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

    // Read Boolean
    let b = stream.read_bson_boolean_sync().expect("");
    assert!(b);

    // Read Double
    let d = stream.read_bson_double_sync().expect("");
    assert!((d - 3.15).abs() < 0.001);

    assert_eq!(stream.position(), 21);
}
