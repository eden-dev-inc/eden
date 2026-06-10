//! Zero-copy OP_MSG parsing.
//!
//! Borrows document bytes from the input buffer instead of copying them.
//!
//! ```rust,ignore
//! use mongo_wire::{OpMsgRef, OpMsgSectionRef, DocumentSequence};
//!
//! let data: &[u8] = /* wire message bytes */;
//! let msg = OpMsgRef::parse_borrowed(data, body_length)?;
//!
//! for section in &msg.sections {
//!     match section {
//!         OpMsgSectionRef::Body { document } => {
//!             // document is a &[u8] borrowing from `data`
//!         }
//!         OpMsgSectionRef::DocumentSequence(seq) => {
//!             for doc in seq.iter() {
//!                 let doc = doc?;
//!             }
//!         }
//!     }
//! }
//! ```

use crate::error::MongoWireError;
use crate::op_msg::{OpMsg, OpMsgSection, flags};
use crate::{MAX_BSON_DOCUMENT_SIZE, MAX_DOCUMENTS_PER_MESSAGE};

/// Zero-copy OP_MSG that borrows document data from the input buffer.
#[derive(Clone, Debug)]
pub struct OpMsgRef<'a> {
    /// Flag bits.
    pub flags: u32,
    /// Message sections.
    pub sections: Vec<OpMsgSectionRef<'a>>,
    /// Optional CRC-32C checksum.
    pub checksum: Option<u32>,
}

/// A section within an [`OpMsgRef`].
#[derive(Clone, Debug)]
pub enum OpMsgSectionRef<'a> {
    /// Single BSON document (section kind 0).
    Body {
        /// Raw BSON document bytes.
        document: &'a [u8],
    },
    /// Document sequence (section kind 1).
    DocumentSequence(DocumentSequence<'a>),
}

/// Lazy document sequence. Documents are validated on iteration, not upfront.
#[derive(Clone, Debug)]
pub struct DocumentSequence<'a> {
    /// Sequence identifier (e.g. "documents", "deletes").
    pub identifier: &'a str,
    /// Raw document bytes.
    raw_data: &'a [u8],
}

impl<'a> DocumentSequence<'a> {
    /// Create a new document sequence.
    #[inline]
    pub fn new(identifier: &'a str, raw_data: &'a [u8]) -> Self {
        Self { identifier, raw_data }
    }

    /// Iterate over documents. Each document is validated as it is yielded.
    #[inline]
    pub fn iter(&self) -> DocumentIterator<'a> {
        DocumentIterator { data: self.raw_data, offset: 0 }
    }

    /// Count documents. Iterates the full sequence, so prefer `iter()` when possible.
    pub fn count(&self) -> usize {
        self.iter().filter(|r| r.is_ok()).count()
    }

    /// Raw bytes of the document sequence.
    #[inline]
    pub fn raw_data(&self) -> &'a [u8] {
        self.raw_data
    }
}

/// Iterator over BSON documents in a [`DocumentSequence`].
#[derive(Clone, Debug)]
pub struct DocumentIterator<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for DocumentIterator<'a> {
    type Item = Result<&'a [u8], MongoWireError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }

        let remaining = &self.data[self.offset..];

        if remaining.len() < 4 {
            return Some(Err(MongoWireError::InvalidBson("insufficient bytes for document length".into())));
        }

        let doc_len_i32 = i32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]);

        if doc_len_i32 < 0 {
            return Some(Err(MongoWireError::InvalidBson("negative document length".into())));
        }

        let doc_len = doc_len_i32 as usize;

        if doc_len < 5 {
            return Some(Err(MongoWireError::InvalidBson("document too short".into())));
        }

        if doc_len > MAX_BSON_DOCUMENT_SIZE {
            return Some(Err(MongoWireError::DocumentTooLarge { length: doc_len, max: MAX_BSON_DOCUMENT_SIZE }));
        }

        if remaining.len() < doc_len {
            return Some(Err(MongoWireError::InvalidBson("document length exceeds available data".into())));
        }

        let document = &remaining[..doc_len];

        if document.last() != Some(&0) {
            return Some(Err(MongoWireError::MissingNullTerminator));
        }

        self.offset += doc_len;
        Some(Ok(document))
    }
}

impl<'a> OpMsgRef<'a> {
    /// Parse an OP_MSG body, borrowing document bytes from `data`.
    #[inline]
    pub fn parse_borrowed(data: &'a [u8], body_length: usize) -> Result<Self, MongoWireError> {
        if body_length < 4 {
            return Err(MongoWireError::message_too_short(4, body_length));
        }

        if data.len() < body_length {
            return Err(MongoWireError::incomplete(body_length, data.len()));
        }

        let flags = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
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
            if offset >= data.len() {
                return Err(MongoWireError::InvalidBson("unexpected end of section data".into()));
            }

            let section_kind = data[offset];
            offset += 1;

            match section_kind {
                0 => {
                    if offset + 4 > data.len() {
                        return Err(MongoWireError::InvalidBson("insufficient bytes for document length".into()));
                    }

                    let doc_len_i32 = i32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);

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

                    let remaining_section = section_end.saturating_sub(offset);
                    if doc_len > remaining_section {
                        return Err(MongoWireError::InvalidBson("document length exceeds remaining section bytes".into()));
                    }

                    if offset + doc_len > data.len() {
                        return Err(MongoWireError::InvalidBson("document extends beyond buffer".into()));
                    }

                    let document = &data[offset..offset + doc_len];

                    if document.last() != Some(&0) {
                        return Err(MongoWireError::MissingNullTerminator);
                    }

                    sections.push(OpMsgSectionRef::Body { document });
                    offset += doc_len;
                    documents_seen = next_documents_seen;
                }
                1 => {
                    if offset + 4 > data.len() {
                        return Err(MongoWireError::InvalidBson("insufficient bytes for sequence length".into()));
                    }

                    let seq_len_i32 = i32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);

                    if seq_len_i32 < 0 {
                        return Err(MongoWireError::InvalidBson("negative sequence length".into()));
                    }

                    let seq_len = seq_len_i32 as usize;
                    if seq_len < 5 {
                        return Err(MongoWireError::InvalidBson("sequence too short".into()));
                    }

                    let remaining_section = section_end.saturating_sub(offset);
                    if seq_len > remaining_section {
                        return Err(MongoWireError::InvalidBson("sequence length exceeds remaining section bytes".into()));
                    }

                    offset += 4;

                    let id_start = offset;
                    let seq_data_start = id_start + seq_len - 4;

                    let mut id_end = id_start;
                    while id_end < data.len() && id_end < seq_data_start && data[id_end] != 0 {
                        id_end += 1;
                    }

                    if id_end >= data.len() || data[id_end] != 0 {
                        return Err(MongoWireError::MissingNullTerminator);
                    }

                    let identifier = std::str::from_utf8(&data[id_start..id_end])?;
                    let id_len = id_end - id_start;

                    let docs_start = id_end + 1;
                    let docs_len = seq_len
                        .checked_sub(4 + id_len + 1)
                        .ok_or_else(|| MongoWireError::InvalidBson("sequence too short for identifier".into()))?;

                    if docs_start + docs_len > data.len() {
                        return Err(MongoWireError::InvalidBson("document sequence extends beyond buffer".into()));
                    }

                    let raw_data = &data[docs_start..docs_start + docs_len];

                    let doc_sequence = DocumentSequence::new(identifier, raw_data);
                    for doc_result in doc_sequence.iter() {
                        doc_result?;
                        let next_documents_seen = documents_seen
                            .checked_add(1)
                            .ok_or(MongoWireError::TooManyDocuments { count: usize::MAX, max: MAX_DOCUMENTS_PER_MESSAGE })?;
                        if next_documents_seen > MAX_DOCUMENTS_PER_MESSAGE {
                            return Err(MongoWireError::TooManyDocuments { count: next_documents_seen, max: MAX_DOCUMENTS_PER_MESSAGE });
                        }
                        documents_seen = next_documents_seen;
                    }

                    sections.push(OpMsgSectionRef::DocumentSequence(doc_sequence));
                    offset = docs_start + docs_len;
                }
                _ => {
                    return Err(MongoWireError::UnsupportedSectionType(section_kind));
                }
            }
        }

        let checksum = if has_checksum {
            if offset + 4 > data.len() {
                return Err(MongoWireError::InvalidBson("insufficient bytes for checksum".into()));
            }
            Some(u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]))
        } else {
            None
        };

        Ok(Self { flags, sections, checksum })
    }

    /// First section-kind-0 document, if any.
    pub fn body(&self) -> Option<&'a [u8]> {
        self.sections.iter().find_map(|s| match s {
            OpMsgSectionRef::Body { document } => Some(*document),
            _ => None,
        })
    }

    /// Whether the `MORE_TO_COME` flag is set.
    pub fn more_to_come(&self) -> bool {
        (self.flags & flags::MORE_TO_COME) != 0
    }
}

impl<'a> From<OpMsgRef<'a>> for OpMsg {
    /// Copy all document data into an owned [`OpMsg`].
    fn from(msg_ref: OpMsgRef<'a>) -> Self {
        let sections = msg_ref
            .sections
            .into_iter()
            .map(|section| match section {
                OpMsgSectionRef::Body { document } => OpMsgSection::Body { document: document.to_vec() },
                OpMsgSectionRef::DocumentSequence(seq) => OpMsgSection::DocumentSequence {
                    identifier: seq.identifier.to_string(),
                    documents: seq.iter().filter_map(|r| r.ok()).map(|doc| doc.to_vec()).collect(),
                },
            })
            .collect();

        OpMsg { flags: msg_ref.flags, sections, checksum: msg_ref.checksum }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_iterator_single_doc() {
        // A minimal BSON document: length (4) + null terminator (1) = 5 bytes
        let data = [0x05, 0x00, 0x00, 0x00, 0x00];

        let mut iter = DocumentIterator { data: &data, offset: 0 };

        let doc = iter.next().unwrap().unwrap();
        assert_eq!(doc.len(), 5);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_document_iterator_multiple_docs() {
        // Two minimal BSON documents
        let data = [
            0x05, 0x00, 0x00, 0x00, 0x00, // doc 1
            0x05, 0x00, 0x00, 0x00, 0x00, // doc 2
        ];

        let iter = DocumentIterator { data: &data, offset: 0 };

        let docs: Vec<_> = iter.collect();
        assert_eq!(docs.len(), 2);
        assert!(docs[0].is_ok());
        assert!(docs[1].is_ok());
    }

    #[test]
    fn test_document_iterator_invalid_length() {
        // Document claims to be 100 bytes but only 5 available
        let data = [0x64, 0x00, 0x00, 0x00, 0x00];

        let mut iter = DocumentIterator { data: &data, offset: 0 };

        let result = iter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_document_iterator_negative_length() {
        // Negative length
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x00];

        let mut iter = DocumentIterator { data: &data, offset: 0 };

        let result = iter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_document_iterator_missing_terminator() {
        // Document without null terminator
        let data = [0x05, 0x00, 0x00, 0x00, 0x01];

        let mut iter = DocumentIterator { data: &data, offset: 0 };

        let result = iter.next().unwrap();
        assert!(matches!(result, Err(MongoWireError::MissingNullTerminator)));
    }

    #[test]
    fn test_document_sequence_iter() {
        let data = [
            0x05, 0x00, 0x00, 0x00, 0x00, // doc 1
            0x05, 0x00, 0x00, 0x00, 0x00, // doc 2
        ];

        let seq = DocumentSequence::new("documents", &data);
        assert_eq!(seq.identifier, "documents");
        assert_eq!(seq.count(), 2);

        for doc in seq.iter() {
            assert!(doc.is_ok());
            assert_eq!(doc.unwrap().len(), 5);
        }
    }

    #[test]
    fn test_parse_simple_body_only() {
        // OP_MSG with single body section
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

        match &msg.sections[0] {
            OpMsgSectionRef::Body { document } => {
                assert_eq!(document.len(), 5);
            }
            _ => panic!("expected body section"),
        }
    }

    #[test]
    fn test_parse_with_checksum() {
        let data = [
            0x01, 0x00, 0x00, 0x00, // flags = CHECKSUM_PRESENT
            0x00, // section kind = 0
            0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
            0x00, // BSON terminator
            0xDE, 0xAD, 0xBE, 0xEF, // checksum
        ];

        let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();

        assert!(msg.checksum.is_some());
        assert_eq!(msg.checksum.unwrap(), 0xEFBEADDE);
    }

    #[test]
    fn test_parse_with_document_sequence() {
        // OP_MSG with body + document sequence
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
            OpMsgSectionRef::Body { .. } => {}
            _ => panic!("expected body section first"),
        }

        match &msg.sections[1] {
            OpMsgSectionRef::DocumentSequence(seq) => {
                assert_eq!(seq.identifier, "docs");
                assert_eq!(seq.count(), 2);
            }
            _ => panic!("expected document sequence section"),
        }
    }

    #[test]
    fn test_conversion_to_owned() {
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

        match &msg_owned.sections[0] {
            OpMsgSection::Body { document } => {
                assert_eq!(document.len(), 5);
            }
            _ => panic!("expected body section"),
        }
    }

    #[test]
    fn test_body_helper() {
        let data = [
            0x00, 0x00, 0x00, 0x00, // flags = 0
            0x00, // section kind = 0
            0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
            0x00, // BSON terminator
        ];

        let msg = OpMsgRef::parse_borrowed(&data, data.len()).unwrap();
        let body = msg.body().unwrap();
        assert_eq!(body.len(), 5);
    }

    #[test]
    fn test_more_to_come() {
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
    fn test_parse_too_short() {
        let data = [0x00, 0x00, 0x00, 0x00]; // Only flags, no sections

        let result = OpMsgRef::parse_borrowed(&data, data.len());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_section_type() {
        let data = [
            0x00, 0x00, 0x00, 0x00, // flags = 0
            0x02, // section kind = 2 (invalid)
            0x05, 0x00, 0x00, 0x00, 0x00,
        ];

        let result = OpMsgRef::parse_borrowed(&data, data.len());
        assert!(matches!(result, Err(MongoWireError::UnsupportedSectionType(2))));
    }

    #[test]
    fn test_parse_truncated_data() {
        // Document claims 5 bytes but buffer is shorter
        let data = [
            0x00, 0x00, 0x00, 0x00, // flags = 0
            0x00, // section kind = 0
            0x05, 0x00, 0x00, 0x00, // BSON doc length = 5
                  // Missing the 5th byte (null terminator)
        ];

        let result = OpMsgRef::parse_borrowed(&data, data.len());
        assert!(result.is_err());
    }
}
