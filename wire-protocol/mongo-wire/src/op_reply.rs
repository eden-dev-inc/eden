//! OP_REPLY message parsing (deprecated).
//!
//! OP_REPLY was the original server response format.

use crate::error::MongoWireError;
use crate::{MAX_BSON_DOCUMENT_SIZE, MAX_DOCUMENTS_PER_MESSAGE};
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// OP_REPLY flag bits.
pub mod flags {
    pub const CURSOR_NOT_FOUND: u32 = 1 << 0;
    pub const QUERY_FAILURE: u32 = 1 << 1;
    pub const SHARD_CONFIG_STALE: u32 = 1 << 2;
    pub const AWAIT_CAPABLE: u32 = 1 << 3;
}

/// Parsed OP_REPLY message.
#[derive(Clone, Debug)]
pub struct OpReply {
    pub response_flags: u32,
    pub cursor_id: i64,
    pub starting_from: i32,
    pub number_returned: i32,
    pub documents: Vec<Vec<u8>>,
}

impl OpReply {
    /// Parse an OP_REPLY from a stream (after header has been read).
    #[inline]
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 20 {
            return Err(MongoWireError::message_too_short(20, body_length));
        }

        let response_flags = stream.read_u32_le_sync().map_err(Into::into)?;
        let cursor_id = stream.read_i64_le_sync().map_err(Into::into)?;
        let starting_from = stream.read_i32_le_sync().map_err(Into::into)?;
        let number_returned = stream.read_i32_le_sync().map_err(Into::into)?;
        if number_returned < 0 {
            return Err(MongoWireError::InvalidBson("negative number_returned".into()));
        }
        let number_returned_usize = number_returned as usize;
        if number_returned_usize > MAX_DOCUMENTS_PER_MESSAGE {
            return Err(MongoWireError::InvalidBson(
                format!("number_returned exceeds limit: {} > {}", number_returned_usize, MAX_DOCUMENTS_PER_MESSAGE).into(),
            ));
        }

        // Use conservative initial capacity to prevent allocation DoS attacks.
        // A malicious message could claim 100,000 documents but provide none.
        let initial_capacity = number_returned_usize.min(64);
        let mut documents = Vec::with_capacity(initial_capacity);

        for _ in 0..number_returned_usize {
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

            // Use conservative initial capacity to prevent allocation DoS attacks.
            let initial_capacity = doc_len.min(64 * 1024);
            let mut document = Vec::with_capacity(initial_capacity);
            document.extend_from_slice(&doc_len_i32.to_le_bytes());
            let remaining = stream.read_bytes_sync(doc_len - 4).map_err(Into::into)?;
            if remaining.last() != Some(&0) {
                return Err(MongoWireError::MissingNullTerminator);
            }
            document.extend_from_slice(&remaining);

            documents.push(document);
        }

        Ok(Self {
            response_flags,
            cursor_id,
            starting_from,
            number_returned,
            documents,
        })
    }

    /// Check if the cursor was not found.
    pub fn cursor_not_found(&self) -> bool {
        (self.response_flags & flags::CURSOR_NOT_FOUND) != 0
    }

    /// Check if the query failed.
    pub fn query_failure(&self) -> bool {
        (self.response_flags & flags::QUERY_FAILURE) != 0
    }

    /// Parse an OP_REPLY from a stream asynchronously (after header has been read).
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 20 {
            return Err(MongoWireError::message_too_short(20, body_length));
        }

        let response_flags = stream.read_u32_le().await.map_err(Into::into)?;
        let cursor_id = stream.read_i64_le().await.map_err(Into::into)?;
        let starting_from = stream.read_i32_le().await.map_err(Into::into)?;
        let number_returned = stream.read_i32_le().await.map_err(Into::into)?;
        if number_returned < 0 {
            return Err(MongoWireError::InvalidBson("negative number_returned".into()));
        }
        let number_returned_usize = number_returned as usize;
        if number_returned_usize > MAX_DOCUMENTS_PER_MESSAGE {
            return Err(MongoWireError::TooManyDocuments { count: number_returned_usize, max: MAX_DOCUMENTS_PER_MESSAGE });
        }

        let initial_capacity = number_returned_usize.min(64);
        let mut documents = Vec::with_capacity(initial_capacity);

        for _ in 0..number_returned_usize {
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
            let mut document = Vec::with_capacity(initial_capacity);
            document.extend_from_slice(&doc_len_i32.to_le_bytes());
            let remaining = stream.peek_read(Some(doc_len - 4)).await.map_err(Into::into)?;
            if remaining.last() != Some(&0) {
                return Err(MongoWireError::MissingNullTerminator);
            }
            document.extend_from_slice(&remaining);
            stream.accept(&remaining, None).map_err(Into::into)?;

            documents.push(document);
        }

        Ok(Self {
            response_flags,
            cursor_id,
            starting_from,
            number_returned,
            documents,
        })
    }
}
