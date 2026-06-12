//! OP_DELETE message parsing (deprecated).
//!
//! OP_DELETE removes documents from a collection.
//! Deprecated in MongoDB 3.6+ in favor of the delete command via OP_MSG.

use crate::MAX_BSON_DOCUMENT_SIZE;
use crate::error::MongoWireError;
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// OP_DELETE flag bits.
pub mod flags {
    /// If set, delete only the first matching document (like deleteOne).
    pub const SINGLE_REMOVE: u32 = 1 << 0;
}

/// Parsed OP_DELETE message.
#[derive(Clone, Debug)]
#[deprecated(since = "0.1.0", note = "OP_DELETE is deprecated; use delete command via OP_MSG")]
pub struct OpDelete {
    /// Reserved field (must be 0).
    pub zero: i32,
    /// Full collection name (e.g., "db.collection").
    pub full_collection_name: String,
    /// Delete flags.
    pub flags: u32,
    /// Query selector document (raw BSON bytes).
    pub selector: Vec<u8>,
}

#[allow(deprecated)]
impl OpDelete {
    /// Parse an OP_DELETE from a stream (after header has been read).
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        // Minimum: zero (4) + collection (1) + flags (4) + selector (5) = 14 bytes
        if body_length < 14 {
            return Err(MongoWireError::message_too_short(14, body_length));
        }

        let zero = stream.read_i32_le_sync().map_err(Into::into)?;

        // Read collection name (null-terminated)
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let flags = stream.read_u32_le_sync().map_err(Into::into)?;

        // Read selector document
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

        let initial_capacity = doc_len.min(64 * 1024);
        let mut selector = Vec::with_capacity(initial_capacity);
        selector.extend_from_slice(&doc_len_i32.to_le_bytes());

        let remaining = stream.read_bytes_sync(doc_len - 4).map_err(Into::into)?;
        if remaining.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }
        selector.extend_from_slice(&remaining);

        Ok(Self { zero, full_collection_name, flags, selector })
    }

    /// Parse an OP_DELETE from a stream asynchronously.
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 14 {
            return Err(MongoWireError::message_too_short(14, body_length));
        }

        let zero = stream.read_i32_le().await.map_err(Into::into)?;

        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let flags = stream.read_u32_le().await.map_err(Into::into)?;

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

        let initial_capacity = doc_len.min(64 * 1024);
        let mut selector = Vec::with_capacity(initial_capacity);
        selector.extend_from_slice(&doc_len_i32.to_le_bytes());

        let remaining = stream.peek_read(Some(doc_len - 4)).await.map_err(Into::into)?;
        if remaining.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }
        selector.extend_from_slice(&remaining);
        stream.accept(&remaining, None).map_err(Into::into)?;

        Ok(Self { zero, full_collection_name, flags, selector })
    }

    /// Check if single-remove is set (deleteOne behavior).
    pub fn is_single_remove(&self) -> bool {
        (self.flags & flags::SINGLE_REMOVE) != 0
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
    fn test_parse_delete() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_le_bytes()); // zero
        data.extend_from_slice(b"test.collection\0"); // collection name
        data.extend_from_slice(&flags::SINGLE_REMOVE.to_le_bytes()); // flags
        // Selector document (minimal)
        data.extend_from_slice(&5i32.to_le_bytes());
        data.push(0);

        let stream = SliceStream::new(&data);
        let msg = OpDelete::parse_sync(&stream, data.len()).expect("parse failed");

        assert_eq!(msg.zero, 0);
        assert_eq!(msg.full_collection_name, "test.collection");
        assert!(msg.is_single_remove());
        assert_eq!(msg.selector.len(), 5);
    }
}
