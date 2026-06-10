//! RESP3 protocol tests.

use super::stream::TestStream;
use crate::pipeline::PipelineExt;

// =============================================================================
// RESP3 Null Tests
// =============================================================================

#[test]
fn pipeline_resp3_null() {
    let stream = TestStream::new(b"_\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 3);
}

// =============================================================================
// RESP3 Boolean Tests
// =============================================================================

#[test]
fn pipeline_resp3_boolean_true() {
    let stream = TestStream::new(b"#t\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

#[test]
fn pipeline_resp3_boolean_false() {
    let stream = TestStream::new(b"#f\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

// =============================================================================
// RESP3 Double Tests
// =============================================================================

#[test]
fn pipeline_resp3_double_positive() {
    let stream = TestStream::new(b",3.14159\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 10);
}

#[test]
fn pipeline_resp3_double_negative() {
    let stream = TestStream::new(b",-2.718\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 9);
}

#[test]
fn pipeline_resp3_double_infinity() {
    let stream = TestStream::new(b",inf\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 6);
}

#[test]
fn pipeline_resp3_double_neg_infinity() {
    let stream = TestStream::new(b",-inf\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 7);
}

#[test]
fn pipeline_resp3_double_nan() {
    let stream = TestStream::new(b",nan\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 6);
}

// =============================================================================
// RESP3 Big Number Tests
// =============================================================================

#[test]
fn pipeline_resp3_big_number() {
    let stream = TestStream::new(b"(3492890328409238509324850943850943825024385\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

#[test]
fn pipeline_resp3_big_number_negative() {
    let stream = TestStream::new(b"(-3492890328409238509324850943850943825024385\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// RESP3 Blob Error Tests
// =============================================================================

#[test]
fn pipeline_resp3_blob_error() {
    let stream = TestStream::new(b"!21\r\nSYNTAX invalid syntax\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    // !21\r\n (5) + 21 bytes content + \r\n (2) = 28
    assert_eq!(stream.position(), 28);
}

// =============================================================================
// RESP3 Verbatim String Tests
// =============================================================================

#[test]
fn pipeline_resp3_verbatim_string_txt() {
    let stream = TestStream::new(b"=15\r\ntxt:Some string\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 22);
}

#[test]
fn pipeline_resp3_verbatim_string_mkd() {
    let stream = TestStream::new(b"=15\r\nmkd:# Heading 1\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// RESP3 Map Tests
// =============================================================================

#[test]
fn pipeline_resp3_map_empty() {
    let stream = TestStream::new(b"%0\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

#[test]
fn pipeline_resp3_map_simple() {
    // Map with 2 key-value pairs
    let stream = TestStream::new(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    // %2\r\n(4) + +first\r\n(8) + :1\r\n(4) + +second\r\n(9) + :2\r\n(4) = 29
    assert_eq!(stream.position(), 29);
}

#[test]
fn pipeline_resp3_map_nested() {
    // Map containing another map
    let stream = TestStream::new(b"%1\r\n+key\r\n%1\r\n+inner\r\n:42\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// RESP3 Set Tests
// =============================================================================

#[test]
fn pipeline_resp3_set_empty() {
    let stream = TestStream::new(b"~0\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

#[test]
fn pipeline_resp3_set_simple() {
    let stream = TestStream::new(b"~3\r\n:1\r\n:2\r\n:3\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 16);
}

// =============================================================================
// RESP3 Push Tests
// =============================================================================

#[test]
fn pipeline_resp3_push() {
    let stream = TestStream::new(b">2\r\n+pubsub\r\n+message\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

#[test]
fn pipeline_resp3_push_with_data() {
    // Push message for a subscription
    let stream = TestStream::new(b">3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$4\r\ndata\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// RESP3 Attribute Tests (if supported)
// =============================================================================

#[test]
fn pipeline_resp3_attribute() {
    // Attribute followed by actual data
    let stream = TestStream::new(b"|1\r\n+key\r\n+value\r\n+OK\r\n");
    let mut pipeline = stream.pipeline();

    // Skip the attribute
    assert!(pipeline.skip().expect("skip 1"));
    // Skip the actual response
    assert!(pipeline.skip().expect("skip 2"));
}

// =============================================================================
// Mixed RESP2/RESP3 Tests
// =============================================================================

#[test]
fn pipeline_mixed_types() {
    let stream = TestStream::new(b"+OK\r\n:42\r\n#t\r\n,3.14\r\n$5\r\nhello\r\n_\r\n");
    let mut pipeline = stream.pipeline();

    // Simple string
    assert!(pipeline.skip().expect("skip 1"));
    // Integer
    assert!(pipeline.skip().expect("skip 2"));
    // Boolean
    assert!(pipeline.skip().expect("skip 3"));
    // Double
    assert!(pipeline.skip().expect("skip 4"));
    // Bulk string
    assert!(pipeline.skip().expect("skip 5"));
    // Null
    assert!(pipeline.skip().expect("skip 6"));
    // No more
    assert!(!pipeline.skip().expect("skip 7"));
}

#[test]
fn pipeline_resp3_array_with_map() {
    // Array containing a map
    let stream = TestStream::new(b"*1\r\n%1\r\n+key\r\n:1\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// Raw Frame Extraction Tests
// =============================================================================

#[test]
fn pipeline_next_raw_simple() {
    let stream = TestStream::new(b"+OK\r\n:42\r\n");
    let mut pipeline = stream.pipeline();

    let frame1 = pipeline.next_raw().expect("error").expect("no frame");
    assert_eq!(frame1, b"+OK\r\n");

    let frame2 = pipeline.next_raw().expect("error").expect("no frame");
    assert_eq!(frame2, b":42\r\n");
}

#[test]
fn pipeline_next_raw_bulk_string() {
    let stream = TestStream::new(b"$11\r\nhello world\r\n");
    let mut pipeline = stream.pipeline();

    let frame = pipeline.next_raw().expect("error").expect("no frame");
    assert_eq!(frame, b"$11\r\nhello world\r\n");
}

#[test]
fn pipeline_next_raw_array() {
    let stream = TestStream::new(b"*2\r\n+a\r\n+b\r\n");
    let mut pipeline = stream.pipeline();

    let frame = pipeline.next_raw().expect("error").expect("no frame");
    assert_eq!(frame, b"*2\r\n+a\r\n+b\r\n");
}
