//! OP_QUERY message parsing (deprecated).
//!
//! OP_QUERY was the original query format, deprecated in MongoDB 5.0.
//! This is provided for compatibility with older drivers.

use crate::MAX_BSON_DOCUMENT_SIZE;
use crate::error::MongoWireError;
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// OP_QUERY flag bits.
pub mod flags {
    /// Reserved bit (must be 0).
    pub const RESERVED: u32 = 1 << 0;
    pub const TAILABLE_CURSOR: u32 = 1 << 1;
    pub const SLAVE_OK: u32 = 1 << 2;
    pub const OPLOG_REPLAY: u32 = 1 << 3;
    pub const NO_CURSOR_TIMEOUT: u32 = 1 << 4;
    pub const AWAIT_DATA: u32 = 1 << 5;
    pub const EXHAUST: u32 = 1 << 6;
    pub const PARTIAL: u32 = 1 << 7;
    /// Mask of all valid flag bits. Reserved bits must be zero.
    pub const VALID_FLAGS_MASK: u32 = TAILABLE_CURSOR | SLAVE_OK | OPLOG_REPLAY | NO_CURSOR_TIMEOUT | AWAIT_DATA | EXHAUST | PARTIAL;
}

/// Parsed OP_QUERY message.
#[derive(Clone, Debug)]
pub struct OpQuery {
    pub flags: u32,
    pub full_collection_name: String,
    pub number_to_skip: i32,
    pub number_to_return: i32,
    pub query: Vec<u8>,
    pub return_fields_selector: Option<Vec<u8>>,
}

impl OpQuery {
    /// Parse an OP_QUERY from a stream (after header has been read).
    #[inline]
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 12 {
            return Err(MongoWireError::message_too_short(12, body_length));
        }

        let flags = stream.read_u32_le_sync().map_err(Into::into)?;

        // Validate reserved bits are zero (per MongoDB wire protocol spec)
        let reserved_bits = flags & !flags::VALID_FLAGS_MASK;
        if reserved_bits != 0 {
            return Err(MongoWireError::InvalidFlags { flags, reserved: reserved_bits });
        }

        // Read collection name (null-terminated)
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let number_to_skip = stream.read_i32_le_sync().map_err(Into::into)?;
        let number_to_return = stream.read_i32_le_sync().map_err(Into::into)?;

        // Read query document
        let query_len_i32 = stream.read_i32_le_sync().map_err(Into::into)?;
        if query_len_i32 < 0 {
            return Err(MongoWireError::InvalidBson("negative query document length".into()));
        }
        if query_len_i32 < 5 {
            return Err(MongoWireError::InvalidBson("query document too short".into()));
        }
        let query_len = query_len_i32 as usize;
        if query_len > MAX_BSON_DOCUMENT_SIZE {
            return Err(MongoWireError::DocumentTooLarge { length: query_len, max: MAX_BSON_DOCUMENT_SIZE });
        }
        // Use conservative initial capacity to prevent allocation DoS attacks.
        let initial_capacity = query_len.min(64 * 1024);
        let mut query = Vec::with_capacity(initial_capacity);
        query.extend_from_slice(&query_len_i32.to_le_bytes());
        let remaining = stream.read_bytes_sync(query_len - 4).map_err(Into::into)?;
        if remaining.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }
        query.extend_from_slice(&remaining);

        // Optional return fields selector
        let peek = stream.peek(Some(4)).map_err(Into::into)?;
        let return_fields_selector = if peek.len() < 4 {
            None
        } else {
            let len_i32 = i32::from_le_bytes([peek[0], peek[1], peek[2], peek[3]]);
            if len_i32 < 0 {
                return Err(MongoWireError::InvalidBson("negative return fields selector length".into()));
            }
            if len_i32 < 5 {
                return Err(MongoWireError::InvalidBson("return fields selector too short".into()));
            }
            let len = len_i32 as usize;
            if len > MAX_BSON_DOCUMENT_SIZE {
                return Err(MongoWireError::DocumentTooLarge { length: len, max: MAX_BSON_DOCUMENT_SIZE });
            }
            // Use conservative initial capacity to prevent allocation DoS attacks.
            let initial_capacity = len.min(64 * 1024);
            let mut doc = Vec::with_capacity(initial_capacity);
            doc.extend_from_slice(&len_i32.to_le_bytes());
            stream.advance_by(4).map_err(Into::into)?;
            let rest = stream.read_bytes_sync(len - 4).map_err(Into::into)?;
            if rest.last() != Some(&0) {
                return Err(MongoWireError::MissingNullTerminator);
            }
            doc.extend_from_slice(&rest);
            Some(doc)
        };

        Ok(Self {
            flags,
            full_collection_name,
            number_to_skip,
            number_to_return,
            query,
            return_fields_selector,
        })
    }

    /// Parse an OP_QUERY from a stream asynchronously (after header has been read).
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 12 {
            return Err(MongoWireError::message_too_short(12, body_length));
        }

        let flags = stream.read_u32_le().await.map_err(Into::into)?;

        // Validate reserved bits are zero (per MongoDB wire protocol spec)
        let reserved_bits = flags & !flags::VALID_FLAGS_MASK;
        if reserved_bits != 0 {
            return Err(MongoWireError::InvalidFlags { flags, reserved: reserved_bits });
        }

        // Read collection name (null-terminated) - use sync method since WireRead extends WireReadSync
        let coll_result = stream.read_cstring_sync().map_err(Into::into)?;
        let full_collection_name = match coll_result {
            Ok(bytes) => std::str::from_utf8(&bytes)?.to_string(),
            Err(_) => return Err(MongoWireError::MissingNullTerminator),
        };

        let number_to_skip = stream.read_i32_le().await.map_err(Into::into)?;
        let number_to_return = stream.read_i32_le().await.map_err(Into::into)?;

        // Read query document
        let query_len_i32 = stream.read_i32_le().await.map_err(Into::into)?;
        if query_len_i32 < 0 {
            return Err(MongoWireError::InvalidBson("negative query document length".into()));
        }
        if query_len_i32 < 5 {
            return Err(MongoWireError::InvalidBson("query document too short".into()));
        }
        let query_len = query_len_i32 as usize;
        if query_len > MAX_BSON_DOCUMENT_SIZE {
            return Err(MongoWireError::DocumentTooLarge { length: query_len, max: MAX_BSON_DOCUMENT_SIZE });
        }
        let initial_capacity = query_len.min(64 * 1024);
        let mut query = Vec::with_capacity(initial_capacity);
        query.extend_from_slice(&query_len_i32.to_le_bytes());
        let remaining = stream.peek_read(Some(query_len - 4)).await.map_err(Into::into)?;
        if remaining.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }
        query.extend_from_slice(&remaining);
        stream.accept(&remaining, None).map_err(Into::into)?;

        // Optional return fields selector
        let peek = stream.peek(Some(4)).map_err(Into::into)?;
        let return_fields_selector = if peek.len() < 4 {
            None
        } else {
            let len_i32 = i32::from_le_bytes([peek[0], peek[1], peek[2], peek[3]]);
            if len_i32 < 0 {
                return Err(MongoWireError::InvalidBson("negative return fields selector length".into()));
            }
            if len_i32 < 5 {
                return Err(MongoWireError::InvalidBson("return fields selector too short".into()));
            }
            let len = len_i32 as usize;
            if len > MAX_BSON_DOCUMENT_SIZE {
                return Err(MongoWireError::DocumentTooLarge { length: len, max: MAX_BSON_DOCUMENT_SIZE });
            }
            let initial_capacity = len.min(64 * 1024);
            let mut doc = Vec::with_capacity(initial_capacity);
            doc.extend_from_slice(&len_i32.to_le_bytes());
            stream.advance_by(4).map_err(Into::into)?;
            let rest = stream.peek_read(Some(len - 4)).await.map_err(Into::into)?;
            if rest.last() != Some(&0) {
                return Err(MongoWireError::MissingNullTerminator);
            }
            doc.extend_from_slice(&rest);
            stream.accept(&rest, None).map_err(Into::into)?;
            Some(doc)
        };

        Ok(Self {
            flags,
            full_collection_name,
            number_to_skip,
            number_to_return,
            query,
            return_fields_selector,
        })
    }

    /// Split the full collection name into (database, collection).
    pub fn split_collection_name(&self) -> Option<(&str, &str)> {
        self.full_collection_name.split_once('.')
    }
}
