//! Tests for MongoDB OP_MSG parsing.

use super::stream::TestStream;
use crate::op_msg::{OpMsg, OpMsgSection, flags};

#[test]
fn parse_simple_op_msg() {
    // Minimal OP_MSG with empty body document
    // flags (4) + kind (1) + doc_len (4) + doc content (1 null byte) = 10
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    assert_eq!(msg.flags, 0);
    assert_eq!(msg.sections.len(), 1);
    assert!(msg.checksum.is_none());

    match &msg.sections[0] {
        OpMsgSection::Body { document } => {
            assert_eq!(document.len(), 5);
        }
        _ => panic!("expected body section"),
    }
}

#[test]
fn parse_op_msg_with_ping() {
    // OP_MSG with { ping: 1 }
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        // BSON document: { ping: 1 }
        0x0F, 0x00, 0x00, 0x00, // doc length = 15
        0x10, // type = int32
        b'p', b'i', b'n', b'g', 0x00, // key = "ping"
        0x01, 0x00, 0x00, 0x00, // value = 1
        0x00, // doc terminator
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    assert_eq!(msg.sections.len(), 1);

    let body = msg.body().expect("no body");
    assert_eq!(body.len(), 15);
}

#[test]
fn parse_op_msg_more_to_come() {
    let data = [
        0x02, 0x00, 0x00, 0x00, // flags = MORE_TO_COME (bit 1)
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    assert!(msg.more_to_come());
    assert_eq!(msg.flags & flags::MORE_TO_COME, flags::MORE_TO_COME);
}

#[test]
fn parse_op_msg_with_checksum() {
    let data = [
        0x01, 0x00, 0x00, 0x00, // flags = CHECKSUM_PRESENT (bit 0)
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
        0xDE, 0xAD, 0xBE, 0xEF, // checksum
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    assert!(msg.checksum.is_some());
    assert_eq!(msg.checksum.expect(""), 0xEFBEADDE); // little-endian
}

#[test]
fn parse_op_msg_document_sequence() {
    // OP_MSG with document sequence section
    // Section size = size(4) + identifier "docs\0"(5) + doc1(5) + doc2(5) = 19
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
        0x01, // section kind = 1 (document sequence)
        0x13, 0x00, 0x00, 0x00, // section size = 19
        b'd', b'o', b'c', b's', 0x00, // identifier = "docs"
        0x05, 0x00, 0x00, 0x00, // doc 1 length = 5
        0x00, // doc 1 terminator
        0x05, 0x00, 0x00, 0x00, // doc 2 length = 5
        0x00, // doc 2 terminator
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    assert_eq!(msg.sections.len(), 2);

    match &msg.sections[0] {
        OpMsgSection::Body { .. } => {}
        _ => panic!("expected body section first"),
    }

    match &msg.sections[1] {
        OpMsgSection::DocumentSequence { identifier, documents } => {
            assert_eq!(identifier, "docs");
            assert_eq!(documents.len(), 2);
        }
        _ => panic!("expected document sequence section"),
    }
}

#[test]
fn op_msg_body_helper() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("Parse failed");

    let body = msg.body();
    assert!(body.is_some());
    assert_eq!(body.expect("").len(), 5);
}

#[test]
fn parse_op_msg_too_short() {
    let data = [0x00, 0x00, 0x00, 0x00]; // Only flags, no sections

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());

    assert!(result.is_err());
}

#[test]
fn flag_constants() {
    assert_eq!(flags::CHECKSUM_PRESENT, 0x01);
    assert_eq!(flags::MORE_TO_COME, 0x02);
    assert_eq!(flags::EXHAUST_ALLOWED, 0x10000);
}
