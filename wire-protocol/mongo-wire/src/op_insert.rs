//! OP_INSERT message parsing (deprecated).
//!
//! OP_INSERT inserts one or more documents into a collection.
//! Deprecated in MongoDB 3.6+ in favor of the insert command via OP_MSG.

use crate::error::MongoWireError;
use crate::{MAX_BSON_DOCUMENT_SIZE, MAX_DOCUMENTS_PER_MESSAGE};
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// OP_INSERT flag bits.
pub mod flags {
    /// If set, continue inserting remaining documents after a failure.
    pub const CONTINUE_ON_ERROR: u32 = 1 << 0;
}

/// Parsed OP_INSERT message.
#[derive(Clone, Debug)]
#[deprecated(since = "0.1.0", note = "OP_INSERT is deprecated; use insert command via OP_MSG")]
pub struct OpInsert {
    /// Insert flags.
    pub flags: u32,
    /// Full collection name (e.g., "db.collection").
    pub full_collection_name: String,
    /// Documents to insert (raw BSON bytes).
    pub documents: Vec<Vec<u8>>,
}

#[allow(deprecated)]
impl OpInsert {
    /// Parse an OP_INSERT from a stream (after header has been read).
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        // Minimum: flags (4) + collection (1) + one doc (5) = 10 bytes
        if body_length < 10 {
            return Err(MongoWireError::message_too_short(10, body_length));
        }

        let flags = stream.read_u32_le_sync().map_err(Into::into)?;

        // Read collection name (null-terminated)
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let (full_collection_name, coll_len) = match coll_result {
            Ok(bytes) => {
                let len = bytes.len() + 1; // include null terminator
                (std::str::from_utf8(&bytes)?.to_string(), len)
            }
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        // Calculate remaining bytes for documents
        let header_size = 4 + coll_len;
        let docs_size = body_length.saturating_sub(header_size);

        let mut documents = Vec::new();
        let mut bytes_read = 0;

        while bytes_read < docs_size {
            let doc_len_i32 = stream.read_i32_le_sync().map_err(Into::into)?;
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
            if documents.len() >= MAX_DOCUMENTS_PER_MESSAGE {
                return Err(MongoWireError::TooManyDocuments { count: documents.len() + 1, max: MAX_DOCUMENTS_PER_MESSAGE });
            }

            let initial_capacity = doc_len.min(64 * 1024);
            let mut document = Vec::with_capacity(initial_capacity);
            document.extend_from_slice(&doc_len_i32.to_le_bytes());

            let remaining = stream.read_bytes_sync(doc_len - 4).map_err(Into::into)?;
            if remaining.last() != Some(&0) {
                return Err(MongoWireError::MissingNullTerminator);
            }
            document.extend_from_slice(&remaining);

            documents.push(document);
            bytes_read += doc_len;
        }

        Ok(Self { flags, full_collection_name, documents })
    }

    /// Parse an OP_INSERT from a stream asynchronously.
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 10 {
            return Err(MongoWireError::message_too_short(10, body_length));
        }

        let flags = stream.read_u32_le().await.map_err(Into::into)?;

        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let (full_collection_name, coll_len) = match coll_result {
            Ok(bytes) => {
                let len = bytes.len() + 1;
                (std::str::from_utf8(&bytes)?.to_string(), len)
            }
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let header_size = 4 + coll_len;
        let docs_size = body_length.saturating_sub(header_size);

        let mut documents = Vec::new();
        let mut bytes_read = 0;

        while bytes_read < docs_size {
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
            if documents.len() >= MAX_DOCUMENTS_PER_MESSAGE {
                return Err(MongoWireError::TooManyDocuments { count: documents.len() + 1, max: MAX_DOCUMENTS_PER_MESSAGE });
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
            bytes_read += doc_len;
        }

        Ok(Self { flags, full_collection_name, documents })
    }

    /// Check if continue-on-error is set.
    pub fn continue_on_error(&self) -> bool {
        (self.flags & flags::CONTINUE_ON_ERROR) != 0
    }

    /// Split the full collection name into (database, collection).
    pub fn split_collection_name(&self) -> Option<(&str, &str)> {
        self.full_collection_name.split_once('.')
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    #[allow(deprecated)]
    fn test_parse_insert() {
        let mut data = Vec::new();
        data.extend_from_slice(&0u32.to_le_bytes()); // flags
        data.extend_from_slice(b"test.collection\0"); // collection name
        // One minimal document
        data.extend_from_slice(&5i32.to_le_bytes()); // doc length
        data.push(0); // doc terminator

        let stream = SliceStream::new(&data);
        let msg = OpInsert::parse_sync(&stream, data.len()).expect("parse failed");

        assert_eq!(msg.flags, 0);
        assert_eq!(msg.full_collection_name, "test.collection");
        assert_eq!(msg.documents.len(), 1);
        assert_eq!(msg.documents[0].len(), 5);
    }
}
