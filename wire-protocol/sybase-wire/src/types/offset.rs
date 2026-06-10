//! TDS OFFSET token.
//!
//! Indicates position in SQL batch for debugging.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream};

/// OFFSET token.
///
/// Indicates the offset position within a SQL batch.
/// This is a fixed-length token (no length prefix).
#[derive(Clone, Debug)]
pub struct Offset {
    /// Offset type.
    pub offset_type: u16,
    /// Offset value (position in SQL text).
    pub offset: u16,
}

impl Offset {
    /// Parse an OFFSET token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Offset, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Offset type (2 bytes)
        let offset_type = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Offset value (2 bytes)
        let offset = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(Offset { offset_type, offset })
    }
}
