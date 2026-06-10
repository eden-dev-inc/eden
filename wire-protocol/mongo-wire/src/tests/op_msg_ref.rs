//! Tests for zero-copy OP_MSG parsing.

use crate::error::MongoWireError;
use crate::op_msg::{OpMsg, OpMsgSection};
use crate::op_msg_ref::{DocumentSequence, OpMsgRef, OpMsgSectionRef};

// ============================================================================
// DocumentIterator tests (via DocumentSequence::iter())
// ============================================================================

#[test]
fn document_iterator_empty_data() {
    let seq = DocumentSequence::new("test", &[]);
    let mut iter = seq.iter();
    assert!(iter.next().is_none());
}

#[test]
fn document_iterator_single_minimal_doc() {
    // Minimal BSON document: length (4 bytes) + null terminator (1 byte) = 5 bytes
    let data = [0x05, 0x00, 0x00, 0x00, 0x00];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let doc = iter.next().unwrap().unwrap();
    assert_eq!(doc, &data[..]);
    assert!(iter.next().is_none());
}

#[test]
fn document_iterator_multiple_docs() {
    // Two minimal documents
    let data = [
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 2
    ];

    let seq = DocumentSequence::new("test", &data);
    let docs: Vec<_> = seq.iter().collect();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].as_ref().unwrap().len(), 5);
    assert_eq!(docs[1].as_ref().unwrap().len(), 5);
}

#[test]
fn document_iterator_varying_sizes() {
    // Two documents of different sizes
    let data = [
        // Doc 1: 5 bytes
        0x05, 0x00, 0x00, 0x00, 0x00, // Doc 2: 6 bytes (length 4 + content 1 + terminator 1)
        0x06, 0x00, 0x00, 0x00, 0x01, 0x00,
    ];

    let seq = DocumentSequence::new("test", &data);
    let docs: Vec<_> = seq.iter().collect();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].as_ref().unwrap().len(), 5);
    assert_eq!(docs[1].as_ref().unwrap().len(), 6);
}

#[test]
fn document_iterator_insufficient_length_bytes() {
    // Only 3 bytes, need 4 for length
    let data = [0x05, 0x00, 0x00];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let result = iter.next().unwrap();
    assert!(result.is_err());
}

#[test]
fn document_iterator_negative_length() {
    // -1 as i32 in little endian
    let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x00];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let result = iter.next().unwrap();
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn document_iterator_doc_too_short() {
    // Length = 4, but minimum is 5
    let data = [0x04, 0x00, 0x00, 0x00, 0x00];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let result = iter.next().unwrap();
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn document_iterator_length_exceeds_data() {
    // Claims 100 bytes but only 5 available
    let data = [0x64, 0x00, 0x00, 0x00, 0x00];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let result = iter.next().unwrap();
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn document_iterator_missing_null_terminator() {
    // Valid length but last byte is not null
    let data = [0x05, 0x00, 0x00, 0x00, 0x01];
    let seq = DocumentSequence::new("test", &data);
    let mut iter = seq.iter();

    let result = iter.next().unwrap();
    assert!(matches!(result, Err(MongoWireError::MissingNullTerminator)));
}

// ============================================================================
// DocumentSequence tests
// ============================================================================

#[test]
fn document_sequence_empty() {
    let seq = DocumentSequence::new("documents", &[]);
    assert_eq!(seq.identifier, "documents");
    assert_eq!(seq.count(), 0);
    assert!(seq.iter().next().is_none());
}

#[test]
fn document_sequence_single_doc() {
    let data = [0x05, 0x00, 0x00, 0x00, 0x00];
    let seq = DocumentSequence::new("inserts", &data);

    assert_eq!(seq.identifier, "inserts");
    assert_eq!(seq.count(), 1);
    assert_eq!(seq.raw_data(), &data[..]);
}

#[test]
fn document_sequence_multiple_docs() {
    let data = [
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 2
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 3
    ];
    let seq = DocumentSequence::new("updates", &data);

    assert_eq!(seq.count(), 3);

    let mut count = 0;
    for doc in seq.iter() {
        assert!(doc.is_ok());
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn document_sequence_iterator_is_lazy() {
    // Create a sequence with invalid second document
    let data = [
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1: valid
        0xFF, 0xFF, 0xFF, 0xFF, 0x00, // doc 2: invalid (negative length)
    ];
    let seq = DocumentSequence::new("docs", &data);

    let mut iter = seq.iter();

    // First document should succeed
    let first = iter.next().unwrap();
    assert!(first.is_ok());

    // Second document should fail
    let second = iter.next().unwrap();
    assert!(second.is_err());
}

// ============================================================================
// OpMsgRef parsing tests
// ============================================================================

#[test]
fn parse_simple_body_only() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

    assert_eq!(msg.flags, 0);
    assert_eq!(msg.sections.len(), 1);
    assert!(msg.checksum.is_none());
    assert!(!msg.more_to_come());

    match &msg.sections[0] {
        OpMsgSectionRef::Body { document } => {
            assert_eq!(document.len(), 5);
            // Verify it's a slice into the original buffer
            assert_eq!(document.as_ptr(), data[5..].as_ptr());
        }
        _ => panic!("expected body section"),
    }
}

#[test]
fn parse_body_with_ping_command() {
    // { ping: 1 } BSON document
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

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    let body = msg.body().unwrap();
    assert_eq!(body.len(), 15);
}

#[test]
fn parse_with_checksum() {
    let data = [
        0x01, 0x00, 0x00, 0x00, // flags = CHECKSUM_PRESENT
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
        0xDE, 0xAD, 0xBE, 0xEF, // checksum (little endian)
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

    assert_eq!(msg.checksum, Some(0xEFBEADDE));
}

#[test]
fn parse_more_to_come_flag() {
    let data = [
        0x02, 0x00, 0x00, 0x00, // flags = MORE_TO_COME
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    assert!(msg.more_to_come());
}

#[test]
fn parse_with_document_sequence() {
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

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

    assert_eq!(msg.sections.len(), 2);

    match &msg.sections[0] {
        OpMsgSectionRef::Body { document } => {
            assert_eq!(document.len(), 5);
        }
        _ => panic!("expected body section first"),
    }

    match &msg.sections[1] {
        OpMsgSectionRef::DocumentSequence(seq) => {
            assert_eq!(seq.identifier, "docs");
            assert_eq!(seq.count(), 2);

            let docs: Vec<_> = seq.iter().filter_map(|r| r.ok()).collect();
            assert_eq!(docs.len(), 2);
        }
        _ => panic!("expected document sequence section"),
    }
}

#[test]
fn parse_multiple_document_sequences() {
    // Body + two document sequences
    // Section size = 4 (size itself) + 2 ("a\0") + 5 (doc) = 11
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, 0x00, // body doc (5 bytes)
        0x01, // section kind = 1 (first document sequence)
        0x0B, 0x00, 0x00, 0x00, // section size = 11 (4 + 2 + 5)
        b'a', 0x00, // identifier = "a" (2 bytes)
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1 (5 bytes)
        0x01, // section kind = 1 (second document sequence)
        0x0B, 0x00, 0x00, 0x00, // section size = 11
        b'b', 0x00, // identifier = "b" (2 bytes)
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1 (5 bytes)
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    assert_eq!(msg.sections.len(), 3);

    match &msg.sections[1] {
        OpMsgSectionRef::DocumentSequence(seq) => assert_eq!(seq.identifier, "a"),
        _ => panic!("expected document sequence"),
    }

    match &msg.sections[2] {
        OpMsgSectionRef::DocumentSequence(seq) => assert_eq!(seq.identifier, "b"),
        _ => panic!("expected document sequence"),
    }
}

// ============================================================================
// Conversion tests
// ============================================================================

#[test]
fn convert_body_only_to_owned() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags = 0
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
        0x00, // BSON terminator
    ];

    let msg_ref = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    let msg_owned: OpMsg = msg_ref.into();

    assert_eq!(msg_owned.flags, 0);
    assert_eq!(msg_owned.sections.len(), 1);
    assert!(msg_owned.checksum.is_none());

    match &msg_owned.sections[0] {
        OpMsgSection::Body { document } => {
            assert_eq!(document.len(), 5);
            assert_eq!(document, &[0x05, 0x00, 0x00, 0x00, 0x00]);
        }
        _ => panic!("expected body section"),
    }
}

#[test]
fn convert_with_document_sequence_to_owned() {
    let data = [
        0x02, 0x00, 0x00, 0x00, // flags = MORE_TO_COME
        0x00, // section kind = 0 (body)
        0x05, 0x00, 0x00, 0x00, 0x00, // body doc
        0x01, // section kind = 1
        0x13, 0x00, 0x00, 0x00, // section size = 19
        b'd', b'o', b'c', b's', 0x00, // identifier = "docs"
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 1
        0x05, 0x00, 0x00, 0x00, 0x00, // doc 2
    ];

    let msg_ref = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    let msg_owned: OpMsg = msg_ref.into();

    assert_eq!(msg_owned.flags, 0x02);
    assert_eq!(msg_owned.sections.len(), 2);

    match &msg_owned.sections[1] {
        OpMsgSection::DocumentSequence { identifier, documents } => {
            assert_eq!(identifier, "docs");
            assert_eq!(documents.len(), 2);
        }
        _ => panic!("expected document sequence"),
    }
}

#[test]
fn convert_preserves_checksum() {
    let data = [
        0x01, 0x00, 0x00, 0x00, // flags = CHECKSUM_PRESENT
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, 0x00, // body doc
        0x12, 0x34, 0x56, 0x78, // checksum
    ];

    let msg_ref = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
    let msg_owned: OpMsg = msg_ref.into();

    assert_eq!(msg_owned.checksum, Some(0x78563412));
}

// ============================================================================
// Error case tests
// ============================================================================

#[test]
fn error_body_length_too_short() {
    let data = [0x00, 0x00, 0x00]; // Less than 4 bytes

    let result = OpMsgRef::parse_borrowed(&data, 3);
    assert!(matches!(result, Err(MongoWireError::MessageTooShort { .. })));
}

#[test]
fn error_buffer_shorter_than_body_length() {
    let data = [0x00, 0x00, 0x00, 0x00, 0x00];

    let result = OpMsgRef::parse_borrowed(&data, 100);
    assert!(matches!(result, Err(MongoWireError::IncompleteMessage { .. })));
}

#[test]
fn error_no_sections() {
    let data = [0x00, 0x00, 0x00, 0x00]; // Just flags, no sections

    let result = OpMsgRef::parse_borrowed(&data, 4);
    assert!(result.is_err());
}

#[test]
fn error_invalid_section_type() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x02, // invalid section type
        0x05, 0x00, 0x00, 0x00, 0x00,
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(matches!(result, Err(MongoWireError::UnsupportedSectionType(2))));
}

#[test]
fn error_negative_document_length() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0xFF, 0xFF, 0xFF, 0xFF, // -1 as i32
        0x00,
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn error_document_too_short() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0x03, 0x00, 0x00, 0x00, // length = 3 (too short)
        0x00, 0x00, 0x00,
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn error_document_exceeds_section() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0x64, 0x00, 0x00, 0x00, // length = 100 (exceeds available)
        0x00,
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(matches!(result, Err(MongoWireError::InvalidBson(_))));
}

#[test]
fn error_missing_null_terminator_body() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0x05, 0x00, 0x00, 0x00, // length = 5
        0x01, // non-null terminator
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(matches!(result, Err(MongoWireError::MissingNullTerminator)));
}

#[test]
fn error_sequence_missing_identifier_terminator() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0x05, 0x00, 0x00, 0x00, 0x00, // body
        0x01, // document sequence
        0x08, 0x00, 0x00, 0x00, // size = 8
        b'a', b'b', b'c', b'd', // no null terminator in sight
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(result.is_err());
}

#[test]
fn error_truncated_checksum() {
    let data = [
        0x01, 0x00, 0x00, 0x00, // flags = CHECKSUM_PRESENT
        0x00, // section kind = 0
        0x05, 0x00, 0x00, 0x00, 0x00, // body doc
        0xDE, 0xAD, // only 2 bytes of checksum
    ];

    let result = OpMsgRef::parse_borrowed(&data, data.len());
    assert!(result.is_err());
}

// ============================================================================
// Zero-copy verification tests
// ============================================================================

#[test]
fn verify_zero_copy_body() {
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x00, // body section
        0x05, 0x00, 0x00, 0x00, 0x00,
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

    match &msg.sections[0] {
        OpMsgSectionRef::Body { document } => {
            // Verify the pointer is into the original buffer
            let doc_start = document.as_ptr() as usize;
            let buf_start = data.as_ptr() as usize;
            let buf_end = buf_start + data.len();

            assert!(doc_start >= buf_start);
            assert!(doc_start < buf_end);
        }
        _ => panic!("expected body"),
    }
}

#[test]
fn verify_zero_copy_document_sequence() {
    // Section size = 4 (size) + 2 ("d\0") + 5 (doc) = 11
    let data = [
        0x00, 0x00, 0x00, 0x00, // flags
        0x01, // document sequence
        0x0B, 0x00, 0x00, 0x00, // size = 11 (4 + 2 + 5)
        b'd', 0x00, // identifier (2 bytes)
        0x05, 0x00, 0x00, 0x00, 0x00, // doc (5 bytes)
    ];

    let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

    match &msg.sections[0] {
        OpMsgSectionRef::DocumentSequence(seq) => {
            // Verify raw_data pointer is into original buffer
            let raw_start = seq.raw_data().as_ptr() as usize;
            let buf_start = data.as_ptr() as usize;
            let buf_end = buf_start + data.len();

            assert!(raw_start >= buf_start);
            assert!(raw_start < buf_end);

            // Verify iterated documents are also zero-copy
            for doc_result in seq.iter() {
                let doc = doc_result.unwrap();
                let doc_start = doc.as_ptr() as usize;
                assert!(doc_start >= buf_start);
                assert!(doc_start < buf_end);
            }
        }
        _ => panic!("expected document sequence"),
    }
}
