//! Tests for MongoDB message header parsing.

use super::stream::TestStream;
use crate::header::{MessageHeader, OpCode};

#[test]
fn parse_header_op_msg() {
    let data = [
        0x15, 0x00, 0x00, 0x00, // messageLength = 21
        0x01, 0x00, 0x00, 0x00, // requestID = 1
        0x00, 0x00, 0x00, 0x00, // responseTo = 0
        0xDD, 0x07, 0x00, 0x00, // opCode = 2013 (OP_MSG)
    ];
    let stream = TestStream::new(&data);

    let header = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

    assert_eq!(header.message_length, 21);
    assert_eq!(header.request_id, 1);
    assert_eq!(header.response_to, 0);
    assert_eq!(header.op_code(), Some(OpCode::Msg));
}

#[test]
fn parse_header_op_query() {
    let data = [
        0x64, 0x00, 0x00, 0x00, // messageLength = 100
        0x2A, 0x00, 0x00, 0x00, // requestID = 42
        0x00, 0x00, 0x00, 0x00, // responseTo = 0
        0xD4, 0x07, 0x00, 0x00, // opCode = 2004 (OP_QUERY)
    ];
    let stream = TestStream::new(&data);

    let header = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

    assert_eq!(header.message_length, 100);
    assert_eq!(header.request_id, 42);
    assert_eq!(header.response_to, 0);
    assert_eq!(header.op_code(), Some(OpCode::Query));
}

#[test]
fn parse_header_op_reply() {
    let data = [
        0xC8, 0x00, 0x00, 0x00, // messageLength = 200
        0x01, 0x00, 0x00, 0x00, // requestID = 1
        0x2A, 0x00, 0x00, 0x00, // responseTo = 42
        0x01, 0x00, 0x00, 0x00, // opCode = 1 (OP_REPLY)
    ];
    let stream = TestStream::new(&data);

    let header = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

    assert_eq!(header.message_length, 200);
    assert_eq!(header.request_id, 1);
    assert_eq!(header.response_to, 42);
    assert_eq!(header.op_code(), Some(OpCode::Reply));
}

#[test]
fn header_roundtrip() {
    let original = MessageHeader {
        message_length: 100,
        request_id: 42,
        response_to: 0,
        op_code: OpCode::Msg as i32,
    };

    let encoded = original.encode();
    let stream = TestStream::new(&encoded);
    let decoded = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

    assert_eq!(original.message_length, decoded.message_length);
    assert_eq!(original.request_id, decoded.request_id);
    assert_eq!(original.response_to, decoded.response_to);
    assert_eq!(original.op_code, decoded.op_code);
}

#[test]
fn header_body_length() {
    let header = MessageHeader {
        message_length: 100,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Msg as i32,
    };

    assert_eq!(header.body_length().expect(""), 84); // 100 - 16
}

#[test]
fn header_body_length_invalid() {
    let header = MessageHeader {
        message_length: 10, // Less than header size (16)
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Msg as i32,
    };

    assert!(header.body_length().is_err());
}

#[test]
fn opcode_from_i32() {
    assert_eq!(OpCode::from_i32(1), Some(OpCode::Reply));
    assert_eq!(OpCode::from_i32(2001), Some(OpCode::Update));
    assert_eq!(OpCode::from_i32(2002), Some(OpCode::Insert));
    assert_eq!(OpCode::from_i32(2004), Some(OpCode::Query));
    assert_eq!(OpCode::from_i32(2005), Some(OpCode::GetMore));
    assert_eq!(OpCode::from_i32(2006), Some(OpCode::Delete));
    assert_eq!(OpCode::from_i32(2007), Some(OpCode::KillCursors));
    assert_eq!(OpCode::from_i32(2012), Some(OpCode::Compressed));
    assert_eq!(OpCode::from_i32(2013), Some(OpCode::Msg));
    assert_eq!(OpCode::from_i32(9999), None);
}

#[test]
fn opcode_is_deprecated() {
    assert!(OpCode::Reply.is_deprecated());
    assert!(OpCode::Update.is_deprecated());
    assert!(OpCode::Insert.is_deprecated());
    assert!(OpCode::Query.is_deprecated());
    assert!(OpCode::GetMore.is_deprecated());
    assert!(OpCode::Delete.is_deprecated());
    assert!(OpCode::KillCursors.is_deprecated());
    assert!(!OpCode::Compressed.is_deprecated());
    assert!(!OpCode::Msg.is_deprecated());
}

#[test]
fn header_encode_size() {
    let header = MessageHeader {
        message_length: 100,
        request_id: 42,
        response_to: 0,
        op_code: OpCode::Msg as i32,
    };

    let encoded = header.encode();
    assert_eq!(encoded.len(), 16);
}

#[test]
fn header_negative_request_id() {
    // Request IDs can be any i32
    let data = [
        0x15, 0x00, 0x00, 0x00, // messageLength = 21
        0xFF, 0xFF, 0xFF, 0xFF, // requestID = -1
        0x00, 0x00, 0x00, 0x00, // responseTo = 0
        0xDD, 0x07, 0x00, 0x00, // opCode = 2013
    ];
    let stream = TestStream::new(&data);

    let header = MessageHeader::parse_sync(&stream).expect("Header parsing failed");

    assert_eq!(header.request_id, -1);
}
