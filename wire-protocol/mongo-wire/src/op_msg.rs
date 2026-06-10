//! OP_MSG message parsing.
//!
//! OP_MSG is the modern MongoDB wire protocol message format (MongoDB 3.6+).
//! It supports:
//! - Bidirectional messages (requests and responses use the same format)
//! - Multiple document sections
//! - Checksum validation (CRC-32C)
//! - Compression

use crate::error::MongoWireError;
use crate::header::MessageHeader;
use crate::{MAX_BSON_DOCUMENT_SIZE, MAX_DOCUMENTS_PER_MESSAGE};
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// OP_MSG flag bits.
pub mod flags {
    /// The message ends with 4 bytes containing a CRC-32C checksum.
    pub const CHECKSUM_PRESENT: u32 = 1 << 0;
    /// Another message will follow this one without further action from the receiver.
    pub const MORE_TO_COME: u32 = 1 << 1;
    /// The client is not prepared for multiple replies.
    pub const EXHAUST_ALLOWED: u32 = 1 << 16;
    /// Mask of all valid flag bits. Reserved bits must be zero.
    pub const VALID_FLAGS_MASK: u32 = CHECKSUM_PRESENT | MORE_TO_COME | EXHAUST_ALLOWED;
}

/// Section types in OP_MSG.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum SectionKind {
    /// Body section (kind = 0): Single BSON document.
    Body = 0,
    /// Document sequence (kind = 1): Multiple BSON documents with identifier.
    DocumentSequence = 1,
}

/// A section within an OP_MSG.
#[derive(Clone, Debug)]
pub enum OpMsgSection {
    /// Body section containing a single BSON document.
    Body {
        /// The raw BSON document bytes.
        document: Vec<u8>,
    },
    /// Document sequence containing multiple documents.
    DocumentSequence {
        /// Sequence identifier (e.g., "documents", "deletes").
        identifier: String,
        /// Raw BSON documents in this sequence.
        documents: Vec<Vec<u8>>,
    },
}

/// Parsed OP_MSG message.
#[derive(Clone, Debug)]
pub struct OpMsg {
    /// Flag bits.
    pub flags: u32,
    /// Message sections.
    pub sections: Vec<OpMsgSection>,
    /// Optional CRC-32C checksum.
    pub checksum: Option<u32>,
}

impl OpMsg {
    /// Parse an OP_MSG from a stream (after header has been read).
    #[inline]
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 5 {
            return Err(MongoWireError::message_too_short(5, body_length));
        }

        let body = stream.peek(Some(body_length)).map_err(Into::into)?;
        if body.len() < body_length {
            return Err(MongoWireError::incomplete(body_length, body.len()));
        }

        let flags = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        let has_checksum = (flags & flags::CHECKSUM_PRESENT) != 0;

        let min_body = if has_checksum { 8 } else { 4 };
        if body_length < min_body + 1 {
            return Err(MongoWireError::message_too_short(min_body + 1, body_length));
        }
        let section_bytes = body_length - min_body;

        let mut sections = Vec::with_capacity(2);
        let mut offset = 4;
        let section_end = 4 + section_bytes;
        let mut documents_seen = 0usize;

        while offset < section_end {
            let section_kind = body[offset];
            offset += 1;

            match section_kind {
                0 => {
                    if offset + 4 > body.len() {
                        return Err(MongoWireError::InvalidBson("insufficient bytes for document length".into()));
                    }
                    let doc_len_i32 = i32::from_le_bytes([body[offset], body[offset + 1], body[offset + 2], body[offset + 3]]);
                    if doc_len_i32 < 0 {
                        return Err(MongoWireError::InvalidBson("negative document length".into()));
                    }
                    if doc_len_i32 < 5 {
                        return Err(MongoWireError::InvalidBson("document too short".into()));
                    }
                    let doc_len = doc_len_i32 as usize;
                    if doc_len > MAX_BSON_DOCUMENT_SIZE {
                        return Err(MongoWireError::DocumentTooLarge { length: doc_len, max: MAX_BSON_DOCUMENT_SIZE });
                    }
                    let next_documents_seen = documents_seen
                        .checked_add(1)
                        .ok_or(MongoWireError::TooManyDocuments { count: usize::MAX, max: MAX_DOCUMENTS_PER_MESSAGE })?;
                    if next_documents_seen > MAX_DOCUMENTS_PER_MESSAGE {
                        return Err(MongoWireError::TooManyDocuments { count: next_documents_seen, max: MAX_DOCUMENTS_PER_MESSAGE });
                    }
                    if offset + doc_len > section_end {
                        return Err(MongoWireError::InvalidBson("document length exceeds remaining section bytes".into()));
                    }
                    if offset + doc_len > body.len() {
                        return Err(MongoWireError::InvalidBson("document extends beyond buffer".into()));
                    }

                    if body[offset + doc_len - 1] != 0 {
                        return Err(MongoWireError::MissingNullTerminator);
                    }

                    let document = body[offset..offset + doc_len].to_vec();

                    sections.push(OpMsgSection::Body { document });
                    offset += doc_len;
                    documents_seen = next_documents_seen;
                }
                1 => {
                    if offset + 4 > body.len() {
                        return Err(MongoWireError::InvalidBson("insufficient bytes for sequence length".into()));
                    }
                    let seq_len_i32 = i32::from_le_bytes([body[offset], body[offset + 1], body[offset + 2], body[offset + 3]]);
                    if seq_len_i32 < 0 {
                        return Err(MongoWireError::InvalidBson("negative sequence length".into()));
                    }
                    let seq_len = seq_len_i32 as usize;
                    if seq_len < 5 {
                        return Err(MongoWireError::InvalidBson("sequence too short".into()));
                    }
                    if offset + seq_len > section_end {
                        return Err(MongoWireError::InvalidBson("sequence length exceeds remaining section bytes".into()));
                    }
                    offset += 4;

                    let id_start = offset;
                    let seq_data_end = id_start + seq_len - 4;
                    let mut id_end = id_start;
                    while id_end < body.len() && id_end < seq_data_end && body[id_end] != 0 {
                        id_end += 1;
                    }
                    if id_end >= body.len() || body[id_end] != 0 {
                        return Err(MongoWireError::MissingNullTerminator);
                    }

                    let identifier = std::str::from_utf8(&body[id_start..id_end])?.to_string();
                    let id_len = id_end - id_start;
                    offset = id_end + 1;

                    let seq_data_len = seq_len
                        .checked_sub(4 + id_len + 1)
                        .ok_or_else(|| MongoWireError::InvalidBson("sequence too short for identifier".into()))?;
                    let docs_end = offset + seq_data_len;

                    let mut documents = Vec::new();
                    while offset < docs_end {
                        if offset + 4 > body.len() {
                            return Err(MongoWireError::InvalidBson("insufficient bytes for document length".into()));
                        }
                        let doc_len_i32 = i32::from_le_bytes([body[offset], body[offset + 1], body[offset + 2], body[offset + 3]]);
                        if doc_len_i32 < 0 {
                            return Err(MongoWireError::InvalidBson("negative document length".into()));
                        }
                        if doc_len_i32 < 5 {
                            return Err(MongoWireError::InvalidBson("document too short".into()));
                        }
                        let doc_len = doc_len_i32 as usize;
                        if doc_len > MAX_BSON_DOCUMENT_SIZE {
                            return Err(MongoWireError::DocumentTooLarge { length: doc_len, max: MAX_BSON_DOCUMENT_SIZE });
                        }
                        let next_documents_seen = documents_seen
                            .checked_add(1)
                            .ok_or(MongoWireError::TooManyDocuments { count: usize::MAX, max: MAX_DOCUMENTS_PER_MESSAGE })?;
                        if next_documents_seen > MAX_DOCUMENTS_PER_MESSAGE {
                            return Err(MongoWireError::TooManyDocuments { count: next_documents_seen, max: MAX_DOCUMENTS_PER_MESSAGE });
                        }
                        if offset + doc_len > docs_end {
                            return Err(MongoWireError::InvalidBson("document length exceeds sequence bytes".into()));
                        }
                        if offset + doc_len > body.len() {
                            return Err(MongoWireError::InvalidBson("document extends beyond buffer".into()));
                        }

                        if body[offset + doc_len - 1] != 0 {
                            return Err(MongoWireError::MissingNullTerminator);
                        }

                        documents.push(body[offset..offset + doc_len].to_vec());
                        offset += doc_len;
                        documents_seen = next_documents_seen;
                    }

                    sections.push(OpMsgSection::DocumentSequence { identifier, documents });
                }
                _ => {
                    return Err(MongoWireError::UnsupportedSectionType(section_kind));
                }
            }
        }

        let checksum = if has_checksum {
            if offset + 4 > body.len() {
                return Err(MongoWireError::InvalidBson("insufficient bytes for checksum".into()));
            }
            Some(u32::from_le_bytes([body[offset], body[offset + 1], body[offset + 2], body[offset + 3]]))
        } else {
            None
        };

        stream.advance_by(body_length).map_err(Into::into)?;

        Ok(Self { flags, sections, checksum })
    }

    /// Parse an OP_MSG from a stream asynchronously (after header has been read).
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 4 {
            return Err(MongoWireError::message_too_short(4, body_length));
        }

        // Read flags
        let flags = stream.read_u32_le().await.map_err(Into::into)?;

        // Validate reserved bits are zero (per MongoDB wire protocol spec)
        let reserved_bits = flags & !flags::VALID_FLAGS_MASK;
        if reserved_bits != 0 {
            return Err(MongoWireError::InvalidFlags { flags, reserved: reserved_bits });
        }

        let has_checksum = (flags & flags::CHECKSUM_PRESENT) != 0;

        let min_body = if has_checksum { 8 } else { 4 };
        if body_length < min_body + 1 {
            return Err(MongoWireError::message_too_short(min_body + 1, body_length));
        }
        let section_bytes = body_length - min_body;

        let mut sections = Vec::new();
        let mut bytes_read = 0;
        let mut documents_seen = 0usize;

        while bytes_read < section_bytes {
            let section_kind = {
                let kind_byte = stream.peek_read_exactly::<1>().await.map_err(Into::into)?;
                stream.accept_exactly(&kind_byte).map_err(Into::into)?;
                kind_byte[0]
            };
            bytes_read += 1;

            match section_kind {
                0 => {
                    // Body section: single BSON document
                    let doc_len_i32 = stream.read_i32_le().await.map_err(Into::into)?;
                    if doc_len_i32 < 0 {
                        return Err(MongoWireError::InvalidBson("negative document length".into()));
                    }
                    if doc_len_i32 < 5 {
                        return Err(MongoWireError::InvalidBson("document too short".into()));
                    }
                    let doc_len = doc_len_i32 as usize;
                    if doc_len > MAX_BSON_DOCUMENT_SIZE {
                        return Err(MongoWireError::DocumentTooLarge { length: doc_len, max: MAX_BSON_DOCUMENT_SIZE });
                    }
                    let next_documents_seen = documents_seen
                        .checked_add(1)
                        .ok_or(MongoWireError::TooManyDocuments { count: usize::MAX, max: MAX_DOCUMENTS_PER_MESSAGE })?;
                    if next_documents_seen > MAX_DOCUMENTS_PER_MESSAGE {
                        return Err(MongoWireError::TooManyDocuments { count: next_documents_seen, max: MAX_DOCUMENTS_PER_MESSAGE });
                    }
                    let remaining_section = section_bytes
                        .checked_sub(bytes_read)
                        .ok_or_else(|| MongoWireError::InvalidBson("section length underflow".into()))?;
                    if doc_len > remaining_section {
                        return Err(MongoWireError::InvalidBson("document length exceeds remaining section bytes".into()));
                    }

                    // Read remaining document bytes (we already read the length)
                    let initial_capacity = doc_len.min(64 * 1024);
                    let mut document = Vec::with_capacity(initial_capacity);
                    document.extend_from_slice(&doc_len_i32.to_le_bytes());

                    let remaining = stream.peek_read(Some(doc_len - 4)).await.map_err(Into::into)?;
                    if remaining.last() != Some(&0) {
                        return Err(MongoWireError::MissingNullTerminator);
                    }
                    document.extend_from_slice(&remaining);
                    stream.accept(&remaining, None).map_err(Into::into)?;

                    sections.push(OpMsgSection::Body { document });
                    bytes_read += doc_len;
                    documents_seen = next_documents_seen;
                }
                1 => {
                    // Document sequence
                    let seq_len_i32 = stream.read_i32_le().await.map_err(Into::into)?;
                    if seq_len_i32 < 0 {
                        return Err(MongoWireError::InvalidBson("negative sequence length".into()));
                    }
                    let seq_len = seq_len_i32 as usize;
                    if seq_len < 5 {
                        return Err(MongoWireError::InvalidBson("sequence too short".into()));
                    }
                    let remaining_section = section_bytes
                        .checked_sub(bytes_read)
                        .ok_or_else(|| MongoWireError::InvalidBson("section length underflow".into()))?;
                    if seq_len > remaining_section {
                        return Err(MongoWireError::InvalidBson("sequence length exceeds remaining section bytes".into()));
                    }
                    bytes_read += 4;

                    // Read identifier (C-string) - use sync method since WireRead extends WireReadSync
                    let id_result = stream.read_cstring_sync().map_err(Into::into)?;
                    let identifier = match id_result {
                        Ok(id_bytes) => {
                            let id = std::str::from_utf8(&id_bytes)?;
                            bytes_read += id_bytes.len() + 1;
                            id.to_string()
                        }
                        Err(_) => return Err(MongoWireError::MissingNullTerminator),
                    };

                    // Read documents until we've consumed seq_len bytes
                    let mut documents = Vec::new();
                    let seq_data_len = seq_len
                        .checked_sub(4 + identifier.len() + 1)
                        .ok_or_else(|| MongoWireError::InvalidBson("sequence too short for identifier".into()))?;
                    let mut seq_bytes_read = 0;

                    while seq_bytes_read < seq_data_len {
                        let doc_len_i32 = stream.read_i32_le().await.map_err(Into::into)?;
                        if doc_len_i32 < 0 {
                            return Err(MongoWireError::InvalidBson("negative document length".into()));
                        }
                        if doc_len_i32 < 5 {
                            return Err(MongoWireError::InvalidBson("document too short".into()));
                        }
                        let doc_len = doc_len_i32 as usize;
                        if doc_len > MAX_BSON_DOCUMENT_SIZE {
                            return Err(MongoWireError::DocumentTooLarge { length: doc_len, max: MAX_BSON_DOCUMENT_SIZE });
                        }
                        let next_documents_seen = documents_seen
                            .checked_add(1)
                            .ok_or(MongoWireError::TooManyDocuments { count: usize::MAX, max: MAX_DOCUMENTS_PER_MESSAGE })?;
                        if next_documents_seen > MAX_DOCUMENTS_PER_MESSAGE {
                            return Err(MongoWireError::TooManyDocuments { count: next_documents_seen, max: MAX_DOCUMENTS_PER_MESSAGE });
                        }
                        let remaining_seq = seq_data_len
                            .checked_sub(seq_bytes_read)
                            .ok_or_else(|| MongoWireError::InvalidBson("sequence length underflow".into()))?;
                        if doc_len > remaining_seq {
                            return Err(MongoWireError::InvalidBson("document length exceeds sequence bytes".into()));
                        }

                        let initial_capacity = doc_len.min(64 * 1024);
                        let mut document = Vec::with_capacity(initial_capacity);
                        document.extend_from_slice(&doc_len_i32.to_le_bytes());

                        let remaining = stream.peek_read(Some(doc_len - 4)).await.map_err(Into::into)?;
                        if remaining.last() != Some(&0) {
                            return Err(MongoWireError::MissingNullTerminator);
                        }
                        document.extend_from_slice(&remaining);
                        stream.accept(&remaining, None).map_err(Into::into)?;

                        documents.push(document);
                        seq_bytes_read += doc_len;
                        documents_seen = next_documents_seen;
                    }

                    bytes_read += seq_data_len;
                    sections.push(OpMsgSection::DocumentSequence { identifier, documents });
                }
                _ => {
                    return Err(MongoWireError::UnsupportedSectionType(section_kind));
                }
            }
        }

        // Read optional checksum
        let checksum = if has_checksum {
            Some(stream.read_u32_le().await.map_err(Into::into)?)
        } else {
            None
        };

        Ok(Self { flags, sections, checksum })
    }

    /// Get the body document (first section of kind 0).
    pub fn body(&self) -> Option<&[u8]> {
        self.sections.iter().find_map(|s| match s {
            OpMsgSection::Body { document } => Some(document.as_slice()),
            _ => None,
        })
    }

    /// Check if more messages are coming.
    pub fn more_to_come(&self) -> bool {
        (self.flags & flags::MORE_TO_COME) != 0
    }

    /// Parse an OP_MSG from raw message bytes with checksum validation.
    ///
    /// This function validates the CRC-32C checksum if the CHECKSUM_PRESENT flag is set.
    /// The `message_bytes` should include the full message from the start of the header.
    ///
    /// # Arguments
    /// * `message_bytes` - The complete message bytes including header
    ///
    /// # Returns
    /// The parsed OpMsg if valid, or an error if parsing or checksum validation fails.
    #[inline]
    pub fn parse_with_checksum(message_bytes: &[u8]) -> Result<Self, MongoWireError> {
        use wire_stream::SliceStream;

        if message_bytes.len() < MessageHeader::SIZE {
            return Err(MongoWireError::message_too_short(MessageHeader::SIZE, message_bytes.len()));
        }

        let stream = SliceStream::new(message_bytes);
        let header = MessageHeader::parse_sync(&stream)?;
        let body_length = header.body_length()?;
        let message_length = MessageHeader::SIZE + body_length;
        if message_bytes.len() < message_length {
            return Err(MongoWireError::incomplete(message_length, message_bytes.len()));
        }

        // Parse the message first
        let msg = Self::parse_sync(&stream, body_length)?;

        // Validate checksum if present
        if let Some(expected_checksum) = msg.checksum {
            // CRC-32C is computed over the message from start through end of sections
            // (excluding the 4-byte checksum itself)
            if message_length < 4 {
                return Err(MongoWireError::InvalidBson("message too short for checksum validation".into()));
            }
            let checksum_range = message_length - 4;
            let actual_checksum = crc32c::crc32c(&message_bytes[..checksum_range]);

            if actual_checksum != expected_checksum {
                return Err(MongoWireError::ChecksumMismatch { expected: expected_checksum, actual: actual_checksum });
            }
        }

        Ok(msg)
    }

    /// Validate a checksum against message bytes.
    ///
    /// # Arguments
    /// * `message_bytes` - The complete message bytes including header (excluding checksum)
    /// * `expected_checksum` - The checksum value to validate against
    ///
    /// # Returns
    /// Ok(()) if checksum matches, Err(ChecksumMismatch) otherwise.
    pub fn validate_checksum(message_bytes: &[u8], expected_checksum: u32) -> Result<(), MongoWireError> {
        let actual_checksum = crc32c::crc32c(message_bytes);
        if actual_checksum != expected_checksum {
            return Err(MongoWireError::ChecksumMismatch { expected: expected_checksum, actual: actual_checksum });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parse_simple_op_msg() {
        // Minimal OP_MSG with empty body document
        // flags (4) + kind (1) + doc_len (4) + doc content (1 null byte) = 10
        let data = [
            0x00, 0x00, 0x00, 0x00, // flags = 0
            0x00, // section kind = 0 (body)
            0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
            0x00, // BSON terminator
        ];

        let stream = SliceStream::new(&data);
        let msg = OpMsg::parse_sync(&stream, data.len()).expect("Should be able to parse message");

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
    fn test_parse_with_valid_checksum() {
        // Build a complete message with header and valid checksum
        let mut message = Vec::new();

        // Body: flags (4) + kind (1) + doc_len (4) + doc content (1) + checksum (4) = 14
        let body_length = 14u32;
        let message_length = (MessageHeader::SIZE as u32) + body_length;

        // Header
        message.extend_from_slice(&(message_length as i32).to_le_bytes()); // messageLength
        message.extend_from_slice(&1i32.to_le_bytes()); // requestId
        message.extend_from_slice(&0i32.to_le_bytes()); // responseTo
        message.extend_from_slice(&2013i32.to_le_bytes()); // opCode (OP_MSG)

        // Body
        message.extend_from_slice(&1u32.to_le_bytes()); // flags = CHECKSUM_PRESENT
        message.push(0x00); // section kind = 0 (body)
        message.extend_from_slice(&5i32.to_le_bytes()); // BSON doc length = 5
        message.push(0x00); // BSON terminator

        // Compute checksum over everything so far
        let checksum = crc32c::crc32c(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        let msg = OpMsg::parse_with_checksum(&message).expect("Should parse with valid checksum");
        assert_eq!(msg.checksum, Some(checksum));
    }

    #[test]
    fn test_parse_with_invalid_checksum() {
        // Build a message with invalid checksum
        let mut message = Vec::new();

        let body_length = 14u32;
        let message_length = (MessageHeader::SIZE as u32) + body_length;

        // Header
        message.extend_from_slice(&(message_length as i32).to_le_bytes());
        message.extend_from_slice(&1i32.to_le_bytes());
        message.extend_from_slice(&0i32.to_le_bytes());
        message.extend_from_slice(&2013i32.to_le_bytes());

        // Body
        message.extend_from_slice(&1u32.to_le_bytes()); // flags = CHECKSUM_PRESENT
        message.push(0x00);
        message.extend_from_slice(&5i32.to_le_bytes());
        message.push(0x00);

        // Wrong checksum
        message.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());

        let result = OpMsg::parse_with_checksum(&message);
        assert!(matches!(result, Err(MongoWireError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_validate_checksum() {
        let data = b"hello world";
        let checksum = crc32c::crc32c(data);

        assert!(OpMsg::validate_checksum(data, checksum).is_ok());
        assert!(OpMsg::validate_checksum(data, checksum + 1).is_err());
    }
}
