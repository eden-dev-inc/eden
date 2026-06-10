//! OP_GET_MORE message parsing (deprecated).
//!
//! OP_GET_MORE retrieves more documents from an existing cursor.
//! Deprecated in MongoDB 3.6+ in favor of the getMore command via OP_MSG.

use crate::error::MongoWireError;
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// Parsed OP_GET_MORE message.
#[derive(Clone, Debug)]
#[deprecated(since = "0.1.0", note = "OP_GET_MORE is deprecated; use getMore command via OP_MSG")]
pub struct OpGetMore {
    /// Reserved field (must be 0).
    pub zero: i32,
    /// Full collection name (e.g., "db.collection").
    pub full_collection_name: String,
    /// Number of documents to return.
    pub number_to_return: i32,
    /// Cursor ID from the original query.
    pub cursor_id: i64,
}

#[allow(deprecated)]
impl OpGetMore {
    /// Parse an OP_GET_MORE from a stream (after header has been read).
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        // Minimum: zero (4) + collection (1) + numberToReturn (4) + cursorId (8) = 17 bytes
        if body_length < 17 {
            return Err(MongoWireError::message_too_short(17, body_length));
        }

        let zero = stream.read_i32_le_sync().map_err(Into::into)?;

        // Read collection name (null-terminated)
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let number_to_return = stream.read_i32_le_sync().map_err(Into::into)?;
        let cursor_id = stream.read_i64_le_sync().map_err(Into::into)?;

        Ok(Self { zero, full_collection_name, number_to_return, cursor_id })
    }

    /// Parse an OP_GET_MORE from a stream asynchronously.
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 17 {
            return Err(MongoWireError::message_too_short(17, body_length));
        }

        let zero = stream.read_i32_le().await.map_err(Into::into)?;

        // Use sync method since WireRead extends WireReadSync
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let number_to_return = stream.read_i32_le().await.map_err(Into::into)?;
        let cursor_id = stream.read_i64_le().await.map_err(Into::into)?;

        Ok(Self { zero, full_collection_name, number_to_return, cursor_id })
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
    fn test_parse_get_more() {
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_le_bytes()); // zero
        data.extend_from_slice(b"test.collection\0"); // collection name
        data.extend_from_slice(&100i32.to_le_bytes()); // numberToReturn
        data.extend_from_slice(&12345i64.to_le_bytes()); // cursorId

        let stream = SliceStream::new(&data);
        let msg = OpGetMore::parse_sync(&stream, data.len()).expect("parse failed");

        assert_eq!(msg.zero, 0);
        assert_eq!(msg.full_collection_name, "test.collection");
        assert_eq!(msg.number_to_return, 100);
        assert_eq!(msg.cursor_id, 12345);
    }
}
