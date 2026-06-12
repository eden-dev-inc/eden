//! Integration tests for wire protocol parsing.

use super::stream::TestStream;
use crate::header::{MessageHeader, OpCode};
use crate::op_compressed::{CompressorId, OpCompressed};
use crate::op_msg::{OpMsg, OpMsgSection, flags};
use crate::{MAX_DOCUMENTS_PER_MESSAGE, MongoWireError};

#[allow(dead_code)]
fn minimal_bson_doc() -> Vec<u8> {
    vec![0x05, 0x00, 0x00, 0x00, 0x00]
}

#[allow(dead_code)]
fn build_op_msg_with_checksum(body_doc: &[u8]) -> Vec<u8> {
    let mut message = Vec::new();

    // Body: flags (4) + kind (1) + doc + checksum (4)
    let body_length = 4 + 1 + body_doc.len() + 4;
    let message_length = MessageHeader::SIZE + body_length;

    // Header
    message.extend_from_slice(&(message_length as i32).to_le_bytes());
    message.extend_from_slice(&1i32.to_le_bytes()); // requestId
    message.extend_from_slice(&0i32.to_le_bytes()); // responseTo
    message.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());

    // Body
    message.extend_from_slice(&(flags::CHECKSUM_PRESENT).to_le_bytes());
    message.push(0x00); // section kind = body
    message.extend_from_slice(body_doc);

    // Compute and append CRC-32C
    let checksum = crc32c::crc32c(&message);
    message.extend_from_slice(&checksum.to_le_bytes());

    message
}

// CRC-32C checksum tests

#[test]
fn checksum_valid_ping_command() {
    // { ping: 1 } - BSON document
    // length (4) + type (1) + key "ping\0" (5) + value (4) + terminator (1) = 15
    let doc = [
        0x0F, 0x00, 0x00, 0x00, // length = 15
        0x10, // int32 type
        b'p', b'i', b'n', b'g', 0x00, // key "ping"
        0x01, 0x00, 0x00, 0x00, // value = 1
        0x00, // terminator
    ];

    let message = build_op_msg_with_checksum(&doc);
    let msg = OpMsg::parse_with_checksum(&message).expect("valid checksum");

    assert!(msg.checksum.is_some());
    assert_eq!(msg.sections.len(), 1);
}

#[test]
fn checksum_tampered_flags_rejected() {
    let doc = minimal_bson_doc();
    let mut message = build_op_msg_with_checksum(&doc);

    // Tamper with the flags (byte 16 after header)
    // Add MORE_TO_COME flag which doesn't break parsing
    message[16] |= 0x02;

    let result = OpMsg::parse_with_checksum(&message);
    assert!(matches!(result, Err(MongoWireError::ChecksumMismatch { .. })));
}

#[test]
fn checksum_tampered_request_id_rejected() {
    let doc = minimal_bson_doc();
    let mut message = build_op_msg_with_checksum(&doc);

    // Tamper with request ID
    message[4] = 0xFF;

    let result = OpMsg::parse_with_checksum(&message);
    assert!(matches!(result, Err(MongoWireError::ChecksumMismatch { .. })));
}

#[test]
fn checksum_wrong_value_rejected() {
    let doc = minimal_bson_doc();
    let mut message = build_op_msg_with_checksum(&doc);

    // Corrupt the checksum bytes at the end
    let len = message.len();
    message[len - 4..].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

    let result = OpMsg::parse_with_checksum(&message);
    assert!(matches!(result, Err(MongoWireError::ChecksumMismatch { .. })));
}

#[test]
fn checksum_empty_doc() {
    let doc = minimal_bson_doc();
    let message = build_op_msg_with_checksum(&doc);

    let msg = OpMsg::parse_with_checksum(&message).expect("valid");
    assert!(msg.checksum.is_some());
}

#[test]
fn validate_checksum_helper() {
    let data = b"test payload for crc";
    let checksum = crc32c::crc32c(data);

    assert!(OpMsg::validate_checksum(data, checksum).is_ok());
    assert!(OpMsg::validate_checksum(data, checksum ^ 1).is_err());
}

// Document limit tests

#[allow(dead_code)]
fn build_document_sequence(identifier: &str, doc_count: usize) -> Vec<u8> {
    let doc = minimal_bson_doc();
    let id_bytes = identifier.as_bytes();

    // section_size = size_field(4) + identifier + null + docs
    let section_size = 4 + id_bytes.len() + 1 + (doc.len() * doc_count);

    let mut data = Vec::new();
    // flags
    data.extend_from_slice(&0u32.to_le_bytes());
    // body section (required)
    data.push(0x00);
    data.extend_from_slice(&doc);
    // document sequence section
    data.push(0x01);
    data.extend_from_slice(&(section_size as i32).to_le_bytes());
    data.extend_from_slice(id_bytes);
    data.push(0x00);
    for _ in 0..doc_count {
        data.extend_from_slice(&doc);
    }

    data
}

#[test]
fn document_limit_small_batch_accepted() {
    let data = build_document_sequence("docs", 10);
    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("small batch");

    match &msg.sections[1] {
        OpMsgSection::DocumentSequence { documents, .. } => {
            assert_eq!(documents.len(), 10);
        }
        _ => panic!("expected document sequence"),
    }
}

#[test]
fn document_limit_at_boundary_accepted() {
    // This test verifies parsing logic but uses a smaller count
    // Full 100k doc test would be slow
    let data = build_document_sequence("docs", 1000);
    let stream = TestStream::new(&data);
    let msg = OpMsg::parse_sync(&stream, data.len()).expect("boundary batch");

    match &msg.sections[1] {
        OpMsgSection::DocumentSequence { documents, .. } => {
            assert_eq!(documents.len(), 1000);
        }
        _ => panic!("expected document sequence"),
    }
}

#[test]
fn document_limit_exceeded_rejected() {
    // Build a message claiming to have more than MAX_DOCUMENTS_PER_MESSAGE
    // We'll construct a sequence header that would require too many docs
    let doc = minimal_bson_doc();
    let doc_count = MAX_DOCUMENTS_PER_MESSAGE + 1;
    let id = "docs";

    let section_size = 4 + id.len() + 1 + (doc.len() * doc_count);

    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body section
    data.extend_from_slice(&doc);
    data.push(0x01); // document sequence
    data.extend_from_slice(&(section_size as i32).to_le_bytes());
    data.extend_from_slice(id.as_bytes());
    data.push(0x00);

    // Add docs up to the limit
    for _ in 0..doc_count {
        data.extend_from_slice(&doc);
    }

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());

    assert!(matches!(result, Err(MongoWireError::TooManyDocuments { .. })));
}

// Compression round-trip tests

#[test]
fn zlib_roundtrip_real_data() {
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::Write;

    // Original OP_MSG body
    let original = b"This is test data that will be compressed and decompressed";

    // Compress with zlib
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(original).expect("compress");
    let compressed = encoder.finish().expect("finish");

    // Build OP_COMPRESSED message
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes()); // original opcode
    data.extend_from_slice(&(original.len() as i32).to_le_bytes()); // uncompressed size
    data.push(CompressorId::Zlib as u8);
    data.extend_from_slice(&compressed);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("parse compressed");

    assert_eq!(msg.original_opcode, OpCode::Msg as i32);
    assert_eq!(msg.uncompressed_size as usize, original.len());
    assert_eq!(msg.compressor_id, CompressorId::Zlib);
    assert!(msg.is_vulnerable_compression());
}

#[test]
#[cfg(feature = "decompression")]
fn zlib_decompress_real_data() {
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::Write;

    let original = b"MongoDB wire protocol test payload for compression";

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(original).expect("compress");
    let compressed = encoder.finish().expect("finish");

    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&(original.len() as i32).to_le_bytes());
    data.push(CompressorId::Zlib as u8);
    data.extend_from_slice(&compressed);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("parse");

    let decompressed = msg.decompress().expect("decompress");
    assert_eq!(decompressed, original);
}

// Malformed message tests

#[test]
fn malformed_truncated_header() {
    let data = [0x00, 0x00, 0x00]; // Only 3 bytes
    let result = OpMsg::parse_with_checksum(&data);
    assert!(matches!(result, Err(MongoWireError::MessageTooShort { .. })));
}

#[test]
fn malformed_invalid_section_type() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0xFF); // invalid section kind
    data.extend_from_slice(&minimal_bson_doc());

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(matches!(result, Err(MongoWireError::UnsupportedSectionType(0xFF))));
}

#[test]
fn malformed_negative_doc_length() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body section
    data.extend_from_slice(&(-1i32).to_le_bytes()); // negative length
    data.push(0x00);

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn malformed_doc_too_short() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body section
    data.extend_from_slice(&3i32.to_le_bytes()); // length < 5
    data.push(0x00);

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn malformed_missing_doc_terminator() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body section
    data.extend_from_slice(&5i32.to_le_bytes()); // length = 5
    data.push(0x01); // non-zero terminator

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(matches!(result, Err(MongoWireError::MissingNullTerminator)));
}

#[test]
fn malformed_doc_length_exceeds_message() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body section
    data.extend_from_slice(&1000i32.to_le_bytes()); // claims 1000 bytes
    data.push(0x00); // only 1 byte

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(result.is_err());
}

#[test]
fn malformed_sequence_negative_length() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u32.to_le_bytes()); // flags
    data.push(0x00); // body
    data.extend_from_slice(&minimal_bson_doc());
    data.push(0x01); // document sequence
    data.extend_from_slice(&(-1i32).to_le_bytes()); // negative length

    let stream = TestStream::new(&data);
    let result = OpMsg::parse_sync(&stream, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn compressed_invalid_compressor_id() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&100i32.to_le_bytes());
    data.push(99); // invalid compressor
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());
    assert!(result.is_err());
}

#[test]
fn compressed_body_too_short() {
    let data = [0x00, 0x00, 0x00]; // too short for compressed header

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());
    assert!(matches!(result, Err(MongoWireError::MessageTooShort { .. })));
}
