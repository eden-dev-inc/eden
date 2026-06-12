//! Tests for MongoDB wire protocol write helpers.

use crate::header::OpCode;
use crate::write::*;

// =============================================================================
// Header Write Tests
// =============================================================================

#[test]
fn write_header_op_msg() {
    let mut buf = Vec::new();
    write_header(&mut buf, 100, 1, 0, OpCode::Msg).expect("");

    assert_eq!(buf.len(), 16);
    assert_eq!(&buf[0..4], &100i32.to_le_bytes());
    assert_eq!(&buf[4..8], &1i32.to_le_bytes());
    assert_eq!(&buf[8..12], &0i32.to_le_bytes());
    assert_eq!(&buf[12..16], &2013i32.to_le_bytes());
}

#[test]
fn write_header_op_query() {
    let mut buf = Vec::new();
    write_header(&mut buf, 200, 42, 0, OpCode::Query).expect("");

    assert_eq!(&buf[12..16], &2004i32.to_le_bytes());
}

#[test]
fn write_header_with_response_to() {
    let mut buf = Vec::new();
    write_header(&mut buf, 100, 2, 1, OpCode::Reply).expect("");

    assert_eq!(&buf[8..12], &1i32.to_le_bytes()); // response_to = 1
}

// =============================================================================
// Integer Write Tests
// =============================================================================

#[test]
fn write_i32_le_positive() {
    let mut buf = Vec::new();
    write_i32_le(&mut buf, 0x12345678).expect("");
    assert_eq!(buf, [0x78, 0x56, 0x34, 0x12]);
}

#[test]
fn write_i32_le_negative() {
    let mut buf = Vec::new();
    write_i32_le(&mut buf, -1).expect("");
    assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
}

#[test]
fn write_u32_le_test() {
    let mut buf = Vec::new();
    write_u32_le(&mut buf, 0xDEADBEEF).expect("");
    assert_eq!(buf, [0xEF, 0xBE, 0xAD, 0xDE]);
}

#[test]
fn write_i64_le_test() {
    let mut buf = Vec::new();
    write_i64_le(&mut buf, 0x123456789ABCDEF0i64).expect("");
    assert_eq!(buf, [0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]);
}

#[test]
fn write_u64_le_test() {
    let mut buf = Vec::new();
    write_u64_le(&mut buf, 0xFEDCBA9876543210u64).expect("");
    assert_eq!(buf, [0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE]);
}

#[test]
fn write_f64_le_test() {
    let mut buf = Vec::new();
    write_f64_le(&mut buf, 1.5).expect("");
    assert_eq!(buf, 1.5f64.to_le_bytes());
}

// =============================================================================
// String Write Tests
// =============================================================================

#[test]
fn write_cstring_basic() {
    let mut buf = Vec::new();
    write_cstring(&mut buf, b"test").expect("");
    assert_eq!(buf, b"test\0");
}

#[test]
fn write_cstring_empty() {
    let mut buf = Vec::new();
    write_cstring(&mut buf, b"").expect("");
    assert_eq!(buf, b"\0");
}

#[test]
fn write_cstring_with_dots() {
    let mut buf = Vec::new();
    write_cstring(&mut buf, b"mydb.mycollection").expect("");
    assert_eq!(buf, b"mydb.mycollection\0");
}

// =============================================================================
// BSON Type Write Tests
// =============================================================================

#[test]
fn write_bson_string_basic() {
    let mut buf = Vec::new();
    write_bson_string(&mut buf, b"hello").expect("");

    // length (4 bytes) + content + null terminator
    assert_eq!(&buf[0..4], &6i32.to_le_bytes()); // "hello" + \0 = 6
    assert_eq!(&buf[4..9], b"hello");
    assert_eq!(buf[9], 0);
}

#[test]
fn write_bson_string_empty() {
    let mut buf = Vec::new();
    write_bson_string(&mut buf, b"").expect("");

    assert_eq!(&buf[0..4], &1i32.to_le_bytes()); // just \0 = 1
    assert_eq!(buf[4], 0);
}

#[test]
fn write_bson_binary_test() {
    let mut buf = Vec::new();
    write_bson_binary(&mut buf, 0x00, &[1, 2, 3, 4]).expect("");

    assert_eq!(&buf[0..4], &4i32.to_le_bytes()); // length
    assert_eq!(buf[4], 0x00); // subtype
    assert_eq!(&buf[5..9], &[1, 2, 3, 4]); // data
}

#[test]
fn write_bson_boolean_true() {
    let mut buf = Vec::new();
    write_bson_boolean(&mut buf, true).expect("");
    assert_eq!(buf, [0x01]);
}

#[test]
fn write_bson_boolean_false() {
    let mut buf = Vec::new();
    write_bson_boolean(&mut buf, false).expect("");
    assert_eq!(buf, [0x00]);
}

#[test]
fn write_bson_object_id_test() {
    let oid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let mut buf = Vec::new();
    write_bson_object_id(&mut buf, &oid).expect("");
    assert_eq!(buf, oid);
}

#[test]
fn write_bson_document_end_test() {
    let mut buf = Vec::new();
    write_bson_document_end(&mut buf).expect("");
    assert_eq!(buf, [0x00]);
}

#[test]
fn write_bson_element_header_int32() {
    let mut buf = Vec::new();
    write_bson_element_header(&mut buf, bson_type::INT32, b"count").expect("");

    assert_eq!(buf[0], bson_type::INT32);
    assert_eq!(&buf[1..6], b"count");
    assert_eq!(buf[6], 0); // null terminator
}

#[test]
fn write_bson_element_header_string() {
    let mut buf = Vec::new();
    write_bson_element_header(&mut buf, bson_type::STRING, b"name").expect("");

    assert_eq!(buf[0], bson_type::STRING);
    assert_eq!(&buf[1..5], b"name");
    assert_eq!(buf[5], 0);
}

// =============================================================================
// BSON Type Constants Tests
// =============================================================================

#[test]
fn bson_type_constants() {
    assert_eq!(bson_type::DOUBLE, 0x01);
    assert_eq!(bson_type::STRING, 0x02);
    assert_eq!(bson_type::DOCUMENT, 0x03);
    assert_eq!(bson_type::ARRAY, 0x04);
    assert_eq!(bson_type::BINARY, 0x05);
    assert_eq!(bson_type::OBJECT_ID, 0x07);
    assert_eq!(bson_type::BOOLEAN, 0x08);
    assert_eq!(bson_type::UTC_DATETIME, 0x09);
    assert_eq!(bson_type::NULL, 0x0A);
    assert_eq!(bson_type::INT32, 0x10);
    assert_eq!(bson_type::TIMESTAMP, 0x11);
    assert_eq!(bson_type::INT64, 0x12);
    assert_eq!(bson_type::DECIMAL128, 0x13);
}

// =============================================================================
// OP_MSG Flag Constants Tests
// =============================================================================

#[test]
fn msg_flags_constants() {
    assert_eq!(msg_flags::CHECKSUM_PRESENT, 1);
    assert_eq!(msg_flags::MORE_TO_COME, 2);
    assert_eq!(msg_flags::EXHAUST_ALLOWED, 0x10000);
}

// =============================================================================
// OP_QUERY Flag Constants Tests
// =============================================================================

#[test]
fn query_flags_constants() {
    assert_eq!(query_flags::TAILABLE_CURSOR, 2);
    assert_eq!(query_flags::SLAVE_OK, 4);
    assert_eq!(query_flags::OPLOG_REPLAY, 8);
    assert_eq!(query_flags::NO_CURSOR_TIMEOUT, 16);
    assert_eq!(query_flags::AWAIT_DATA, 32);
    assert_eq!(query_flags::EXHAUST, 64);
    assert_eq!(query_flags::PARTIAL, 128);
}

// =============================================================================
// OP_MSG Builder Tests
// =============================================================================

#[test]
fn op_msg_builder_simple() {
    // Simple BSON document: { "ping": 1 }
    let doc = [
        16, 0, 0, 0,    // document length
        0x10, // int32 type
        b'p', b'i', b'n', b'g', 0, // key "ping"
        1, 0, 0, 0, // value 1
        0, // document end
    ];

    let msg = OpMsgBuilder::new(42).body(&doc).build();

    // Check header
    let len = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
    assert_eq!(len, msg.len() as i32);

    let request_id = i32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]);
    assert_eq!(request_id, 42);

    let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
    assert_eq!(opcode, 2013);

    // Check flags (should be 0)
    let flags = u32::from_le_bytes([msg[16], msg[17], msg[18], msg[19]]);
    assert_eq!(flags, 0);

    // Check section kind
    assert_eq!(msg[20], 0); // body section
}

#[test]
fn op_msg_builder_with_document_sequence() {
    let body = [5, 0, 0, 0, 0]; // minimal empty doc
    let doc1 = [5, 0, 0, 0, 0];
    let doc2 = [5, 0, 0, 0, 0];

    let msg = OpMsgBuilder::new(1).body(&body).document_sequence("documents", &[&doc1, &doc2]).build();

    // Verify it builds without panic
    assert!(msg.len() > 16 + 4 + 1 + 5); // header + flags + kind + body
}

#[test]
fn op_msg_builder_request_id() {
    let doc = [5, 0, 0, 0, 0];

    let msg1 = OpMsgBuilder::new(1).body(&doc).build();
    let msg2 = OpMsgBuilder::new(999).body(&doc).build();

    let id1 = i32::from_le_bytes([msg1[4], msg1[5], msg1[6], msg1[7]]);
    let id2 = i32::from_le_bytes([msg2[4], msg2[5], msg2[6], msg2[7]]);

    assert_eq!(id1, 1);
    assert_eq!(id2, 999);
}

// =============================================================================
// OP_QUERY Write Tests
// =============================================================================

#[test]
fn write_query_basic() {
    let mut buf = Vec::new();
    let query_doc = [5, 0, 0, 0, 0]; // minimal empty doc

    write_query(
        &mut buf,
        0,                  // flags
        b"test.collection", // collection
        0,                  // skip
        10,                 // limit
        &query_doc,
    )
    .expect("");

    // flags (4) + collection + \0 + skip (4) + limit (4) + doc
    assert_eq!(&buf[0..4], &0u32.to_le_bytes()); // flags
    assert_eq!(&buf[4..19], b"test.collection");
    assert_eq!(buf[19], 0); // null terminator
    assert_eq!(&buf[20..24], &0i32.to_le_bytes()); // skip
    assert_eq!(&buf[24..28], &10i32.to_le_bytes()); // limit
}

#[test]
fn write_query_with_flags() {
    let mut buf = Vec::new();
    let query_doc = [5, 0, 0, 0, 0];
    let flags = query_flags::SLAVE_OK | query_flags::NO_CURSOR_TIMEOUT;

    write_query(&mut buf, flags, b"db.coll", 0, 100, &query_doc).expect("");

    let written_flags = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(written_flags, flags);
}

// =============================================================================
// Section Kind Tests
// =============================================================================

#[test]
fn section_kind_values() {
    assert_eq!(SectionKind::Body as u8, 0);
    assert_eq!(SectionKind::DocumentSequence as u8, 1);
}

#[test]
fn write_section_kind_body() {
    let mut buf = Vec::new();
    write_section_kind(&mut buf, SectionKind::Body).expect("");
    assert_eq!(buf, [0]);
}

#[test]
fn write_section_kind_document_sequence() {
    let mut buf = Vec::new();
    write_section_kind(&mut buf, SectionKind::DocumentSequence).expect("");
    assert_eq!(buf, [1]);
}

// =============================================================================
// Integration Tests
// =============================================================================

#[test]
fn build_complete_bson_document() {
    // Build: { "name": "test", "count": 42 }
    let mut doc = Vec::new();

    // Reserve space for length
    doc.extend_from_slice(&[0, 0, 0, 0]);

    // String element: "name" = "test"
    write_bson_element_header(&mut doc, bson_type::STRING, b"name").expect("");
    write_bson_string(&mut doc, b"test").expect("");

    // Int32 element: "count" = 42
    write_bson_element_header(&mut doc, bson_type::INT32, b"count").expect("");
    write_i32_le(&mut doc, 42).expect("");

    // Document terminator
    write_bson_document_end(&mut doc).expect("");

    // Fix length
    let len = doc.len() as i32;
    doc[0..4].copy_from_slice(&len.to_le_bytes());

    // Verify structure
    assert_eq!(&doc[0..4], &len.to_le_bytes());
    assert_eq!(*doc.last().expect(""), 0); // terminator
}

#[test]
fn build_complete_op_msg() {
    // Build a complete OP_MSG with ping command
    let mut doc = Vec::new();
    doc.extend_from_slice(&[0, 0, 0, 0]); // length placeholder
    write_bson_element_header(&mut doc, bson_type::INT32, b"ping").expect("");
    write_i32_le(&mut doc, 1).expect("");
    write_bson_document_end(&mut doc).expect("");
    let len = doc.len() as i32;
    doc[0..4].copy_from_slice(&len.to_le_bytes());

    let msg = OpMsgBuilder::new(1).body(&doc).build();

    // Should be valid message
    let msg_len = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
    assert_eq!(msg_len as usize, msg.len());
}
