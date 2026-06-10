//! TDS DONE token types.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream};

/// Status flags for DONE tokens.
pub mod done_flags {
    /// More results are coming.
    pub const MORE: u16 = 0x0001;
    /// Command produced an error.
    pub const ERROR: u16 = 0x0002;
    /// Transaction is in progress.
    pub const INXACT: u16 = 0x0004;
    /// Count is valid.
    pub const COUNT: u16 = 0x0010;
    /// Attention acknowledged.
    pub const ATTN: u16 = 0x0020;
    /// Server error.
    pub const SRVERROR: u16 = 0x0100;
}

/// DONE/DONEPROC/DONEINPROC token.
///
/// Indicates the end of a result set, stored procedure, or statement.
#[derive(Clone, Debug)]
pub struct Done {
    /// Status flags.
    pub status: u16,
    /// Current command (for batches).
    pub cur_cmd: u16,
    /// Row count (if COUNT flag is set).
    pub done_row_count: u64,
}

impl Done {
    /// Check if more results are coming.
    pub fn has_more(&self) -> bool {
        self.status & done_flags::MORE != 0
    }

    /// Check if the command produced an error.
    pub fn has_error(&self) -> bool {
        self.status & done_flags::ERROR != 0
    }

    /// Check if a transaction is in progress.
    pub fn in_transaction(&self) -> bool {
        self.status & done_flags::INXACT != 0
    }

    /// Check if the row count is valid.
    pub fn has_count(&self) -> bool {
        self.status & done_flags::COUNT != 0
    }

    /// Get the row count if valid.
    pub fn row_count(&self) -> Option<u64> {
        if self.has_count() { Some(self.done_row_count) } else { None }
    }

    /// Check if this is an attention acknowledgment.
    pub fn is_attention_ack(&self) -> bool {
        self.status & done_flags::ATTN != 0
    }

    /// Parse a DONE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Done, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Status (2 bytes, little-endian)
        let status = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Current command (2 bytes, little-endian)
        let cur_cmd = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Row count (4 bytes for TDS 4.2/5.0, 8 bytes for TDS 7.2+)
        // We'll read 4 bytes for Sybase
        let done_row_count = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as u64;

        Ok(Done { status, cur_cmd, done_row_count })
    }
}
