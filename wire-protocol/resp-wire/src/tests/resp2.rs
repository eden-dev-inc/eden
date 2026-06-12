//! RESP2 protocol tests.

use super::stream::TestStream;
use crate::InvalidLength;
use crate::RespParse;
use crate::RespParseSync;
use crate::pipeline::PipelineExt;
use crate::types::bulk_string::BulkStringParseError;
use crate::types::dynamic::{Dynamic, DynamicParseError};
use crate::types::simple_string::SimpleString;

// =============================================================================
// Simple String Tests
// =============================================================================

#[test]
fn simple_string_basic() {
    let stream = TestStream::new(b"+OK\r\n");
    let mut reader = SimpleString::parse_sync(&stream).expect("parse failed");
    let content = reader.next_sync().expect("read error").expect("no content");
    assert_eq!(&*content, b"OK");
}

#[test]
fn simple_string_empty() {
    let stream = TestStream::new(b"+\r\n");
    let mut reader = SimpleString::parse_sync(&stream).expect("parse failed");
    let content = reader.next_sync().expect("read error").expect("no content");
    assert_eq!(&*content, b"");
}

#[test]
fn simple_string_with_spaces() {
    let stream = TestStream::new(b"+Hello World\r\n");
    let mut reader = SimpleString::parse_sync(&stream).expect("parse failed");
    let content = reader.next_sync().expect("read error").expect("no content");
    assert_eq!(&*content, b"Hello World");
}

// =============================================================================
// Pipeline Tests
// =============================================================================

#[test]
fn pipeline_single_frame() {
    let stream = TestStream::new(b"+OK\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 5);
}

#[test]
fn pipeline_multiple_frames() {
    let stream = TestStream::new(b"+OK\r\n+PONG\r\n:42\r\n");
    let mut pipeline = stream.pipeline();

    // First frame
    assert!(pipeline.skip().expect("skip 1"));

    // Second frame
    assert!(pipeline.skip().expect("skip 2"));

    // Third frame
    assert!(pipeline.skip().expect("skip 3"));

    // No more frames
    assert!(!pipeline.skip().expect("skip 4"));
}

#[test]
fn pipeline_bulk_string() {
    let stream = TestStream::new(b"$5\r\nhello\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 11);
}

#[test]
fn pipeline_array() {
    let stream = TestStream::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 22);
}

#[test]
fn pipeline_nested_array() {
    let stream = TestStream::new(b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 28);
}

#[test]
fn pipeline_null_bulk_string() {
    let stream = TestStream::new(b"$-1\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 5);
}

#[test]
fn pipeline_null_array() {
    let stream = TestStream::new(b"*-1\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 5);
}

// =============================================================================
// Integer Tests
// =============================================================================

#[test]
fn pipeline_integer_positive() {
    let stream = TestStream::new(b":1000\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 7);
}

#[test]
fn pipeline_integer_negative() {
    let stream = TestStream::new(b":-42\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 6);
}

#[test]
fn pipeline_integer_zero() {
    let stream = TestStream::new(b":0\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

// =============================================================================
// Error Tests
// =============================================================================

#[test]
fn pipeline_simple_error() {
    let stream = TestStream::new(b"-ERR unknown command\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 22);
}

// =============================================================================
// Complex Pipeline Tests
// =============================================================================

#[test]
fn pipeline_redis_command_response() {
    // Simulating: SET key value -> OK, GET key -> "value"
    let stream = TestStream::new(b"+OK\r\n$5\r\nvalue\r\n");
    let mut pipeline = stream.pipeline();

    // SET response
    let frame1 = pipeline.next_raw().expect("frame 1 error").expect("no frame 1");
    assert_eq!(frame1, b"+OK\r\n");

    // GET response
    let frame2 = pipeline.next_raw().expect("frame 2 error").expect("no frame 2");
    assert_eq!(frame2, b"$5\r\nvalue\r\n");

    // No more
    assert!(pipeline.next_raw().expect("frame 3 error").is_none());
}

#[test]
fn pipeline_empty_array() {
    let stream = TestStream::new(b"*0\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
    assert_eq!(stream.position(), 4);
}

#[test]
fn pipeline_array_with_null_element() {
    // Array with null bulk string element
    let stream = TestStream::new(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
    let mut pipeline = stream.pipeline();

    assert!(pipeline.skip().expect("skip failed"));
}

// =============================================================================
// Incomplete Data Tests
// =============================================================================

#[test]
fn pipeline_incomplete_simple_string() {
    let stream = TestStream::new(b"+OK"); // missing \r\n
    let mut pipeline = stream.pipeline();

    // Should return error (incomplete data causes parse failure)
    assert!(pipeline.skip().is_err());
}

#[test]
fn pipeline_incomplete_bulk_string() {
    let stream = TestStream::new(b"$5\r\nhel"); // missing "lo\r\n"
    let mut pipeline = stream.pipeline();

    // Should return error (not enough data)
    assert!(pipeline.skip().is_err());
}

// =============================================================================
// Dynamic Bulk String Limit Tests
// =============================================================================

#[test]
fn dynamic_bulk_string_sync_rejects_over_max_string_bytes() {
    let input = format!("${}\r\n", crate::limits::MAX_STRING_BYTES + 1);
    let stream = TestStream::new(input.as_bytes());

    let err = Dynamic::parse_sync(&stream).expect_err("expected oversized bulk string to fail");
    assert!(matches!(
        err,
        crate::RespParseError::Parse(DynamicParseError::BulkString(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))
    ));
}

#[test]
fn dynamic_bulk_string_async_rejects_over_max_string_bytes() {
    let input = format!("${}\r\n", crate::limits::MAX_STRING_BYTES + 1);
    let stream = TestStream::new(input.as_bytes());

    let err = pollster::block_on(Dynamic::parse(&stream)).expect_err("expected oversized bulk string to fail");
    assert!(matches!(
        err,
        crate::RespParseError::Parse(DynamicParseError::BulkString(BulkStringParseError::InvalidLength(InvalidLength::TooLarge)))
    ));
}

#[test]
fn dynamic_bulk_string_sync_accepts_max_string_bytes_boundary() {
    let len = crate::limits::MAX_STRING_BYTES;
    let mut payload = format!("${}\r\n", len).into_bytes();
    payload.extend(vec![b'a'; len]);
    payload.extend_from_slice(b"\r\n");

    let stream = TestStream::new(payload.as_slice());
    let parsed = Dynamic::parse_sync(&stream).expect("expected max-size bulk string to parse");

    match parsed {
        Dynamic::BulkString { value } => {
            assert_eq!(value.len(), len);
            assert!(value.iter().all(|&b| b == b'a'));
        }
        other => panic!("expected bulk string, got {other:?}"),
    }
}
