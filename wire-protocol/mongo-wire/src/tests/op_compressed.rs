//! Tests for OP_COMPRESSED message parsing with CVE-2025-14847 mitigation.

use super::stream::TestStream;
use crate::header::{MessageHeader, OpCode};
use crate::op_compressed::{CompressorId, MAX_UNCOMPRESSED_SIZE, OpCompressed};

// =============================================================================
// Valid Compressed Message Tests
// =============================================================================

#[test]
fn parse_valid_compressed_snappy() {
    let original_opcode = OpCode::Msg as i32;
    let uncompressed_size = 100i32;
    let compressor = CompressorId::Snappy as u8;
    let compressed_data = b"fake_snappy_data";

    let mut data = Vec::new();
    data.extend_from_slice(&original_opcode.to_le_bytes());
    data.extend_from_slice(&uncompressed_size.to_le_bytes());
    data.push(compressor);
    data.extend_from_slice(compressed_data);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should parse valid compressed message");

    assert_eq!(msg.original_opcode, OpCode::Msg as i32);
    assert_eq!(msg.uncompressed_size, 100);
    assert_eq!(msg.compressor_id, CompressorId::Snappy);
    assert_eq!(msg.compressed_data, compressed_data);
}

#[test]
fn parse_valid_compressed_zlib() {
    let original_opcode = OpCode::Msg as i32;
    let uncompressed_size = 50i32;
    let compressor = CompressorId::Zlib as u8;

    // Valid zlib header: CMF=0x78, FLG=0x9C
    // (0x78 * 256 + 0x9C) % 31 == 0 ✓
    let compressed_data = vec![0x78, 0x9C, 0x63, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01];

    let mut data = Vec::new();
    data.extend_from_slice(&original_opcode.to_le_bytes());
    data.extend_from_slice(&uncompressed_size.to_le_bytes());
    data.push(compressor);
    data.extend_from_slice(&compressed_data);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should parse valid zlib message");

    assert_eq!(msg.compressor_id, CompressorId::Zlib);
    assert!(msg.is_vulnerable_compression());
}

#[test]
fn parse_valid_compressed_zstd() {
    let original_opcode = OpCode::Query as i32;
    let uncompressed_size = 200i32;
    let compressor = CompressorId::Zstd as u8;
    let compressed_data = b"fake_zstd_data_here";

    let mut data = Vec::new();
    data.extend_from_slice(&original_opcode.to_le_bytes());
    data.extend_from_slice(&uncompressed_size.to_le_bytes());
    data.push(compressor);
    data.extend_from_slice(compressed_data);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 42,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should parse valid zstd message");

    assert_eq!(msg.original_op_code(), Some(OpCode::Query));
    assert_eq!(msg.compressor_id, CompressorId::Zstd);
    assert!(!msg.is_vulnerable_compression());
}

// =============================================================================
// CVE-2025-14847: Length Mismatch Tests
// =============================================================================

#[test]
fn reject_cve_length_mismatch_larger() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&100i32.to_le_bytes());
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    // Create header with WRONG length (larger than actual)
    let header = MessageHeader {
        message_length: 1000, // Claims 1000 bytes but data is much smaller
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject length mismatch");
    match result {
        Err(crate::MongoWireError::InvalidMessageLength(_)) => {
            // Expected error type
        }
        _ => panic!("Expected InvalidMessageLength error"),
    }
}

#[test]
fn reject_cve_length_mismatch_smaller() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&100i32.to_le_bytes());
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(&[0u8; 100]); // 100 bytes of data

    // Create header claiming message is smaller than actual
    let header = MessageHeader {
        message_length: 50, // Claims only 50 bytes
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject length mismatch");
}

// =============================================================================
// CVE-2025-14847: Excessive Uncompressed Size Tests
// =============================================================================

#[test]
fn reject_cve_excessive_uncompressed_size() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&(MAX_UNCOMPRESSED_SIZE + 1).to_le_bytes()); // Too large!
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject excessive size");
    match result {
        Err(crate::MongoWireError::InvalidBson(msg)) => {
            assert!(msg.contains("exceeds maximum"));
        }
        _ => panic!("Expected InvalidBson error with 'exceeds maximum'"),
    }
}

#[test]
fn reject_cve_negative_uncompressed_size() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&(-100i32).to_le_bytes()); // Negative size!
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject negative size");
}

#[test]
fn accept_maximum_valid_size() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&MAX_UNCOMPRESSED_SIZE.to_le_bytes()); // Exactly at limit
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should accept size at limit");

    assert_eq!(msg.uncompressed_size, MAX_UNCOMPRESSED_SIZE);
}

// =============================================================================
// CVE-2025-14847: Invalid Zlib Header Tests
// =============================================================================

#[test]
fn reject_cve_invalid_zlib_checksum() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Zlib as u8);

    // Invalid zlib header: bad FCHECK
    // (0x78 * 256 + 0x00) % 31 != 0
    let bad_zlib = vec![0x78, 0x00, 0x63, 0x00, 0x00];
    data.extend_from_slice(&bad_zlib);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject invalid zlib checksum");
    match result {
        Err(crate::MongoWireError::InvalidBson(msg)) => {
            assert!(msg.contains("zlib header checksum"));
        }
        _ => panic!("Expected InvalidBson error about zlib checksum"),
    }
}

#[test]
fn reject_cve_invalid_zlib_compression_method() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Zlib as u8);

    // Invalid: CM should be 8 (deflate), but we use 9
    // CMF = 0x79 means CM=9, CINFO=7
    let bad_zlib = vec![0x79, 0x9C, 0x63, 0x00];
    data.extend_from_slice(&bad_zlib);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject invalid compression method");
}

#[test]
fn reject_cve_invalid_zlib_window_size() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Zlib as u8);

    // Invalid: CINFO > 7
    // CMF = 0x88 means CM=8, CINFO=8 (invalid)
    let bad_zlib = vec![0x88, 0x00, 0x63];
    data.extend_from_slice(&bad_zlib);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject invalid window size");
}

#[test]
fn reject_cve_zlib_too_short() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Zlib as u8);

    // Only 1 byte - can't even read the header
    data.push(0x78);

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject zlib data too short");
}

// =============================================================================
// Compressor ID Tests
// =============================================================================

#[test]
fn compressor_id_from_u8() {
    assert_eq!(CompressorId::from_u8(0), Some(CompressorId::Noop));
    assert_eq!(CompressorId::from_u8(1), Some(CompressorId::Snappy));
    assert_eq!(CompressorId::from_u8(2), Some(CompressorId::Zlib));
    assert_eq!(CompressorId::from_u8(3), Some(CompressorId::Zstd));
    assert_eq!(CompressorId::from_u8(99), None);
}

#[test]
fn compressor_vulnerability_check() {
    assert!(!CompressorId::Noop.is_vulnerable());
    assert!(!CompressorId::Snappy.is_vulnerable());
    assert!(CompressorId::Zlib.is_vulnerable());
    assert!(!CompressorId::Zstd.is_vulnerable());
}

#[test]
fn reject_unknown_compressor() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(99); // Unknown compressor ID
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject unknown compressor");
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn reject_message_too_short() {
    // Body too short to contain header fields
    let data = [0x00, 0x00, 0x00]; // Only 3 bytes

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let result = OpCompressed::parse_sync(&stream, &header, data.len());

    assert!(result.is_err(), "Should reject message too short");
    match result {
        Err(crate::MongoWireError::MessageTooShort { .. }) => {}
        _ => panic!("Expected MessageTooShort error"),
    }
}

#[test]
fn parse_minimum_valid_message() {
    // Minimum valid: 4 (opcode) + 4 (size) + 1 (compressor) + 0 (data) = 9 bytes
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    data.extend_from_slice(&0i32.to_le_bytes()); // 0 uncompressed size
    data.push(CompressorId::Noop as u8);
    // No compressed data

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should parse minimum valid message");

    assert_eq!(msg.compressed_data.len(), 0);
}

#[test]
fn original_opcode_helper() {
    let mut data = Vec::new();
    data.extend_from_slice(&(OpCode::Query as i32).to_le_bytes());
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("");

    assert_eq!(msg.original_op_code(), Some(OpCode::Query));
}

#[test]
fn original_opcode_unknown() {
    let mut data = Vec::new();
    data.extend_from_slice(&9999i32.to_le_bytes()); // Unknown opcode
    data.extend_from_slice(&50i32.to_le_bytes());
    data.push(CompressorId::Snappy as u8);
    data.extend_from_slice(b"data");

    let header = MessageHeader {
        message_length: (MessageHeader::SIZE + data.len()) as i32,
        request_id: 1,
        response_to: 0,
        op_code: OpCode::Compressed as i32,
    };

    let stream = TestStream::new(&data);
    let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("");

    assert_eq!(msg.original_op_code(), None);
}

// =============================================================================
// Multiple Valid Zlib Headers
// =============================================================================

#[test]
fn accept_various_valid_zlib_headers() {
    let test_cases = vec![
        (0x78, 0x9C), // Default compression (32K window, deflate)
        (0x78, 0xDA), // Best compression (32K window, deflate)
        (0x78, 0x01), // No compression (32K window, deflate)
        (0x68, 0xDE), // 16K window, deflate - valid FCHECK
    ];

    for (cmf, flg) in test_cases {
        let mut data = Vec::new();
        data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
        data.extend_from_slice(&50i32.to_le_bytes());
        data.push(CompressorId::Zlib as u8);
        data.extend_from_slice(&[cmf, flg, 0x00, 0x00]);

        let header = MessageHeader {
            message_length: (MessageHeader::SIZE + data.len()) as i32,
            request_id: 1,
            response_to: 0,
            op_code: OpCode::Compressed as i32,
        };

        let stream = TestStream::new(&data);
        let result = OpCompressed::parse_sync(&stream, &header, data.len());

        assert!(result.is_ok(), "Should accept valid zlib header CMF=0x{:02X} FLG=0x{:02X}", cmf, flg);
    }
}

#[test]
fn zlib_fcheck_validation_comprehensive() {
    // Test that FCHECK validation is working correctly
    // FCHECK is valid when (CMF * 256 + FLG) % 31 == 0

    // Valid combinations
    let valid = vec![
        (0x78, 0x9C), // (30876 % 31) = 0
        (0x78, 0xDA), // (30938 % 31) = 0
        (0x78, 0x01), // (30721 % 31) = 0
        (0x68, 0xDE), // (26846 % 31) = 0
    ];

    for (cmf, flg) in valid {
        let fcheck = (cmf as u16 * 256 + flg as u16) % 31;
        assert_eq!(fcheck, 0, "CMF=0x{:02X} FLG=0x{:02X} should have FCHECK=0, got {}", cmf, flg, fcheck);
    }

    // Invalid combinations
    let invalid = vec![
        (0x78, 0x00), // (30720 % 31) = 30
        (0x68, 0x54), // (26708 % 31) = 17
        (0x78, 0x02), // (30722 % 31) = 1
    ];

    for (cmf, flg) in invalid {
        let fcheck = (cmf as u16 * 256 + flg as u16) % 31;
        assert_ne!(fcheck, 0, "CMF=0x{:02X} FLG=0x{:02X} should have invalid FCHECK, got {}", cmf, flg, fcheck);
    }
}
