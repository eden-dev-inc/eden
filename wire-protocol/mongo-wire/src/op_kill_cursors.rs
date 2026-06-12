//! OP_KILL_CURSORS message parsing and building (deprecated).
//!
//! OP_KILL_CURSORS notifies the database that cursors are no longer needed.
//! Deprecated in MongoDB 3.6+ in favor of the killCursors command via OP_MSG.

use crate::OpCode;
use crate::error::MongoWireError;
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// Maximum number of cursor IDs in a single OP_KILL_CURSORS message.
const MAX_CURSOR_IDS: usize = 10_000;

/// Parsed OP_KILL_CURSORS message.
#[derive(Clone, Debug)]
#[deprecated(since = "0.1.0", note = "OP_KILL_CURSORS is deprecated; use killCursors command via OP_MSG")]
pub struct OpKillCursors {
    /// Reserved field (must be 0).
    pub zero: i32,
    /// Cursor IDs to kill.
    pub cursor_ids: Vec<i64>,
}

#[allow(deprecated)]
impl OpKillCursors {
    /// Parse an OP_KILL_CURSORS from a stream (after header has been read).
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        // Minimum: zero (4) + numberOfCursorIds (4) = 8 bytes
        if body_length < 8 {
            return Err(MongoWireError::message_too_short(8, body_length));
        }

        let zero = stream.read_i32_le_sync().map_err(Into::into)?;
        let number_of_cursor_ids = stream.read_i32_le_sync().map_err(Into::into)?;

        if number_of_cursor_ids < 0 {
            return Err(MongoWireError::InvalidBson("negative cursor count".into()));
        }

        let count = number_of_cursor_ids as usize;
        if count > MAX_CURSOR_IDS {
            return Err(MongoWireError::InvalidBson(
                format!("too many cursor IDs: {} (max {})", count, MAX_CURSOR_IDS).into(),
            ));
        }

        // Each cursor ID is 8 bytes
        let expected_size = 8 + count * 8;
        if body_length < expected_size {
            return Err(MongoWireError::message_too_short(expected_size, body_length));
        }

        let initial_capacity = count.min(64);
        let mut cursor_ids = Vec::with_capacity(initial_capacity);

        for _ in 0..count {
            let cursor_id = stream.read_i64_le_sync().map_err(Into::into)?;
            cursor_ids.push(cursor_id);
        }

        Ok(Self { zero, cursor_ids })
    }

    /// Parse an OP_KILL_CURSORS from a stream asynchronously.
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < 8 {
            return Err(MongoWireError::message_too_short(8, body_length));
        }

        let zero = stream.read_i32_le().await.map_err(Into::into)?;
        let number_of_cursor_ids = stream.read_i32_le().await.map_err(Into::into)?;

        if number_of_cursor_ids < 0 {
            return Err(MongoWireError::InvalidBson("negative cursor count".into()));
        }

        let count = number_of_cursor_ids as usize;
        if count > MAX_CURSOR_IDS {
            return Err(MongoWireError::InvalidBson(
                format!("too many cursor IDs: {} (max {})", count, MAX_CURSOR_IDS).into(),
            ));
        }

        let expected_size = 8 + count * 8;
        if body_length < expected_size {
            return Err(MongoWireError::message_too_short(expected_size, body_length));
        }

        let initial_capacity = count.min(64);
        let mut cursor_ids = Vec::with_capacity(initial_capacity);

        for _ in 0..count {
            let cursor_id = stream.read_i64_le().await.map_err(Into::into)?;
            cursor_ids.push(cursor_id);
        }

        Ok(Self { zero, cursor_ids })
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for constructing OP_KILL_CURSORS messages.
#[deprecated(since = "0.1.0", note = "OP_KILL_CURSORS is deprecated; use killCursors command via OP_MSG")]
pub struct OpKillCursorsBuilder {
    buf: Vec<u8>,
    request_id: i32,
    cursor_ids: Vec<i64>,
}

#[allow(deprecated)]
impl OpKillCursorsBuilder {
    /// Create a new OP_KILL_CURSORS builder.
    pub fn new(request_id: i32) -> Self {
        Self {
            buf: Vec::with_capacity(64),
            request_id,
            cursor_ids: Vec::new(),
        }
    }

    /// Add a cursor ID to kill.
    pub fn cursor_id(mut self, id: i64) -> Self {
        self.cursor_ids.push(id);
        self
    }

    /// Add multiple cursor IDs to kill.
    pub fn cursor_ids(mut self, ids: &[i64]) -> Self {
        self.cursor_ids.extend_from_slice(ids);
        self
    }

    /// Build the final message.
    pub fn build(mut self) -> Vec<u8> {
        // Header (16) + zero (4) + numberOfCursorIds (4) + cursorIds (8 each)
        let body_length = 4 + 4 + self.cursor_ids.len() * 8;
        let message_length = (16 + body_length) as i32;

        // Reserve space for header
        self.buf.extend_from_slice(&[0u8; 16]);

        // Write body
        self.buf.extend_from_slice(&0i32.to_le_bytes()); // zero
        self.buf.extend_from_slice(&(self.cursor_ids.len() as i32).to_le_bytes());

        for cursor_id in &self.cursor_ids {
            self.buf.extend_from_slice(&cursor_id.to_le_bytes());
        }

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::KillCursors as i32).to_le_bytes());

        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    #[allow(deprecated)]
    fn test_parse_kill_cursors() {
        let data = [
            0x00, 0x00, 0x00, 0x00, // zero
            0x02, 0x00, 0x00, 0x00, // numberOfCursorIds = 2
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cursor 1
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cursor 2
        ];

        let stream = SliceStream::new(&data);
        let msg = OpKillCursors::parse_sync(&stream, data.len()).expect("parse failed");

        assert_eq!(msg.zero, 0);
        assert_eq!(msg.cursor_ids.len(), 2);
        assert_eq!(msg.cursor_ids[0], 1);
        assert_eq!(msg.cursor_ids[1], 2);
    }

    #[test]
    #[allow(deprecated)]
    fn test_build_kill_cursors() {
        let msg = OpKillCursorsBuilder::new(42).cursor_id(100).cursor_id(200).build();

        // Verify header
        let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, OpCode::KillCursors as i32);

        // Verify cursor count
        let count = i32::from_le_bytes([msg[20], msg[21], msg[22], msg[23]]);
        assert_eq!(count, 2);
    }
}
