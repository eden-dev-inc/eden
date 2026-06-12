//! Tests for RESP write helpers.

use crate::write::*;

// =============================================================================
// Bulk String Tests
// =============================================================================

#[test]
fn write_bulk_string_basic() {
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, b"hello").expect("");
    assert_eq!(buf, b"$5\r\nhello\r\n");
}

#[test]
fn write_bulk_string_empty() {
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, b"").expect("");
    assert_eq!(buf, b"$0\r\n\r\n");
}

#[test]
fn write_bulk_string_binary() {
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, &[0x00, 0x01, 0x02]).expect("");
    assert_eq!(buf, b"$3\r\n\x00\x01\x02\r\n");
}

#[test]
fn write_bulk_string_with_crlf() {
    let mut buf = Vec::new();
    write_bulk_string(&mut buf, b"line1\r\nline2").expect("");
    assert_eq!(buf, b"$12\r\nline1\r\nline2\r\n");
}

// =============================================================================
// Array Header Tests
// =============================================================================

#[test]
fn write_array_header_zero() {
    let mut buf = Vec::new();
    write_array_header(&mut buf, 0).expect("");
    assert_eq!(buf, b"*0\r\n");
}

#[test]
fn write_array_header_positive() {
    let mut buf = Vec::new();
    write_array_header(&mut buf, 5).expect("");
    assert_eq!(buf, b"*5\r\n");
}

#[test]
fn write_array_header_large() {
    let mut buf = Vec::new();
    write_array_header(&mut buf, 1000000).expect("");
    assert_eq!(buf, b"*1000000\r\n");
}

// =============================================================================
// Integer Tests
// =============================================================================

#[test]
fn write_integer_positive() {
    let mut buf = Vec::new();
    write_integer(&mut buf, 42).expect("");
    assert_eq!(buf, b":42\r\n");
}

#[test]
fn write_integer_negative() {
    let mut buf = Vec::new();
    write_integer(&mut buf, -123).expect("");
    assert_eq!(buf, b":-123\r\n");
}

#[test]
fn write_integer_zero() {
    let mut buf = Vec::new();
    write_integer(&mut buf, 0).expect("");
    assert_eq!(buf, b":0\r\n");
}

#[test]
fn write_integer_max() {
    let mut buf = Vec::new();
    write_integer(&mut buf, i64::MAX).expect("");
    assert_eq!(buf, format!(":{}\\r\\n", i64::MAX).replace("\\r\\n", "\r\n").as_bytes());
}

#[test]
fn write_integer_min() {
    let mut buf = Vec::new();
    write_integer(&mut buf, i64::MIN).expect("");
    assert_eq!(buf, format!(":{}\\r\\n", i64::MIN).replace("\\r\\n", "\r\n").as_bytes());
}

// =============================================================================
// Simple String Tests
// =============================================================================

#[test]
fn write_simple_string_ok() {
    let mut buf = Vec::new();
    write_simple_string(&mut buf, b"OK").expect("");
    assert_eq!(buf, b"+OK\r\n");
}

#[test]
fn write_simple_string_empty() {
    let mut buf = Vec::new();
    write_simple_string(&mut buf, b"").expect("");
    assert_eq!(buf, b"+\r\n");
}

#[test]
fn write_simple_string_pong() {
    let mut buf = Vec::new();
    write_simple_string(&mut buf, b"PONG").expect("");
    assert_eq!(buf, b"+PONG\r\n");
}

// =============================================================================
// Simple Error Tests
// =============================================================================

#[test]
fn write_simple_error_basic() {
    let mut buf = Vec::new();
    write_simple_error(&mut buf, b"ERR unknown command").expect("");
    assert_eq!(buf, b"-ERR unknown command\r\n");
}

#[test]
fn write_simple_error_wrongtype() {
    let mut buf = Vec::new();
    write_simple_error(&mut buf, b"WRONGTYPE Operation against a key holding the wrong kind of value").expect("");
    assert_eq!(buf, b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
}

// =============================================================================
// Null Tests
// =============================================================================

#[test]
fn write_null_bulk_test() {
    let mut buf = Vec::new();
    write_null_bulk(&mut buf).expect("");
    assert_eq!(buf, b"$-1\r\n");
}

#[test]
fn write_null_array_test() {
    let mut buf = Vec::new();
    write_null_array(&mut buf).expect("");
    assert_eq!(buf, b"*-1\r\n");
}

#[test]
fn write_null_resp3() {
    let mut buf = Vec::new();
    write_null(&mut buf).expect("");
    assert_eq!(buf, b"_\r\n");
}

// =============================================================================
// RESP3 Boolean Tests
// =============================================================================

#[test]
fn write_boolean_true() {
    let mut buf = Vec::new();
    write_boolean(&mut buf, true).expect("");
    assert_eq!(buf, b"#t\r\n");
}

#[test]
fn write_boolean_false() {
    let mut buf = Vec::new();
    write_boolean(&mut buf, false).expect("");
    assert_eq!(buf, b"#f\r\n");
}

// =============================================================================
// RESP3 Double Tests
// =============================================================================

#[test]
fn write_double_positive() {
    let mut buf = Vec::new();
    write_double(&mut buf, 3.125).expect("");
    assert!(buf.starts_with(b",3.125"));
    assert!(buf.ends_with(b"\r\n"));
}

#[test]
fn write_double_negative() {
    let mut buf = Vec::new();
    write_double(&mut buf, -2.5).expect("");
    assert!(buf.starts_with(b",-2.5"));
}

#[test]
fn write_double_zero() {
    let mut buf = Vec::new();
    write_double(&mut buf, 0.0).expect("");
    assert_eq!(buf, b",0\r\n");
}

#[test]
fn write_double_infinity() {
    let mut buf = Vec::new();
    write_double(&mut buf, f64::INFINITY).expect("");
    assert_eq!(buf, b",inf\r\n");
}

#[test]
fn write_double_neg_infinity() {
    let mut buf = Vec::new();
    write_double(&mut buf, f64::NEG_INFINITY).expect("");
    assert_eq!(buf, b",-inf\r\n");
}

#[test]
fn write_double_nan() {
    let mut buf = Vec::new();
    write_double(&mut buf, f64::NAN).expect("");
    assert_eq!(buf, b",nan\r\n");
}

// =============================================================================
// RESP3 Map Header Tests
// =============================================================================

#[test]
fn write_map_header_empty() {
    let mut buf = Vec::new();
    write_map_header(&mut buf, 0).expect("");
    assert_eq!(buf, b"%0\r\n");
}

#[test]
fn write_map_header_with_entries() {
    let mut buf = Vec::new();
    write_map_header(&mut buf, 3).expect("");
    assert_eq!(buf, b"%3\r\n");
}

// =============================================================================
// RESP3 Set Header Tests
// =============================================================================

#[test]
fn write_set_header_empty() {
    let mut buf = Vec::new();
    write_set_header(&mut buf, 0).expect("");
    assert_eq!(buf, b"~0\r\n");
}

#[test]
fn write_set_header_with_members() {
    let mut buf = Vec::new();
    write_set_header(&mut buf, 5).expect("");
    assert_eq!(buf, b"~5\r\n");
}

// =============================================================================
// RESP3 Push Header Tests
// =============================================================================

#[test]
fn write_push_header_test() {
    let mut buf = Vec::new();
    write_push_header(&mut buf, 3).expect("");
    assert_eq!(buf, b">3\r\n");
}

// =============================================================================
// RESP3 Attribute Header Tests
// =============================================================================

#[test]
fn write_attribute_header_test() {
    let mut buf = Vec::new();
    write_attribute_header(&mut buf, 1).expect("");
    assert_eq!(buf, b"|1\r\n");
}

// =============================================================================
// RESP3 Blob Error Tests
// =============================================================================

#[test]
fn write_blob_error_test() {
    let mut buf = Vec::new();
    write_blob_error(&mut buf, b"SYNTAX invalid syntax").expect("");
    assert_eq!(buf, b"!21\r\nSYNTAX invalid syntax\r\n");
}

// =============================================================================
// RESP3 Verbatim String Tests
// =============================================================================

#[test]
fn write_verbatim_string_txt() {
    let mut buf = Vec::new();
    write_verbatim_string(&mut buf, b"txt", b"hello").expect("");
    assert_eq!(buf, b"=9\r\ntxt:hello\r\n");
}

#[test]
fn write_verbatim_string_mkd() {
    let mut buf = Vec::new();
    write_verbatim_string(&mut buf, b"mkd", b"# Title").expect("");
    assert_eq!(buf, b"=11\r\nmkd:# Title\r\n");
}

// =============================================================================
// RESP3 Big Number Tests
// =============================================================================

#[test]
fn write_big_number_positive() {
    let mut buf = Vec::new();
    write_big_number(&mut buf, b"12345678901234567890").expect("");
    assert_eq!(buf, b"(12345678901234567890\r\n");
}

#[test]
fn write_big_number_negative() {
    let mut buf = Vec::new();
    write_big_number(&mut buf, b"-98765432109876543210").expect("");
    assert_eq!(buf, b"(-98765432109876543210\r\n");
}

// =============================================================================
// Command Builder Tests
// =============================================================================

#[test]
fn command_builder_ping() {
    let cmd = CommandBuilder::new("PING").build();
    assert_eq!(cmd.to_vec(), b"*1\r\n$4\r\nPING\r\n");
}

#[test]
fn command_builder_get() {
    let cmd = CommandBuilder::new("GET").arg("mykey").build();
    assert_eq!(cmd.to_vec(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
}

#[test]
fn command_builder_set() {
    let cmd = CommandBuilder::new("SET").arg("mykey").arg("myvalue").build();
    assert_eq!(cmd.to_vec(), b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
}

#[test]
fn command_builder_set_with_options() {
    let cmd = CommandBuilder::new("SET").arg("key").arg("value").arg("EX").arg_int(3600).build();
    // arg_int uses RESP integer format (:)
    assert_eq!(cmd, b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n:3600\r\n");
}

#[test]
fn command_builder_mget() {
    let cmd = CommandBuilder::new("MGET").arg("key1").arg("key2").arg("key3").build();
    assert_eq!(cmd, b"*4\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
}

#[test]
fn command_builder_zadd() {
    let cmd = CommandBuilder::new("ZADD").arg("myzset").arg_int(1).arg("one").arg_int(2).arg("two").build();
    // arg_int uses RESP integer format (:)
    assert_eq!(cmd, b"*6\r\n$4\r\nZADD\r\n$6\r\nmyzset\r\n:1\r\n$3\r\none\r\n:2\r\n$3\r\ntwo\r\n");
}

#[test]
fn command_builder_binary_data() {
    let cmd = CommandBuilder::new("SET").arg("key").arg_bulk(&[0x00, 0x01, 0x02]).build();
    assert_eq!(cmd, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n\x00\x01\x02\r\n");
}

// =============================================================================
// Integration: Building Complete Responses
// =============================================================================

#[test]
fn build_complete_array_response() {
    let mut buf = Vec::new();

    // *3\r\n:1\r\n:2\r\n:3\r\n
    write_array_header(&mut buf, 3).expect("");
    write_integer(&mut buf, 1).expect("");
    write_integer(&mut buf, 2).expect("");
    write_integer(&mut buf, 3).expect("");

    assert_eq!(buf, b"*3\r\n:1\r\n:2\r\n:3\r\n");
}

#[test]
fn build_complete_map_response() {
    let mut buf = Vec::new();

    // %2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n
    write_map_header(&mut buf, 2).expect("");
    write_simple_string(&mut buf, b"key1").expect("");
    write_integer(&mut buf, 1).expect("");
    write_simple_string(&mut buf, b"key2").expect("");
    write_integer(&mut buf, 2).expect("");

    assert_eq!(buf, b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n");
}

#[test]
fn build_nested_array_response() {
    let mut buf = Vec::new();

    // *2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n
    write_array_header(&mut buf, 2).expect("");
    write_array_header(&mut buf, 2).expect("");
    write_integer(&mut buf, 1).expect("");
    write_integer(&mut buf, 2).expect("");
    write_array_header(&mut buf, 2).expect("");
    write_integer(&mut buf, 3).expect("");
    write_integer(&mut buf, 4).expect("");

    assert_eq!(buf, b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
}
