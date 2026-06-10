//! TDS ALTROW token.
//!
//! Alternate row format for COMPUTE BY results.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// ALTROW token.
///
/// Contains computed results from COMPUTE BY clauses.
#[derive(Clone, Debug)]
pub struct AltRow {
    /// Alternate row ID.
    pub id: u16,
    /// Column values (raw bytes for each computed column).
    pub values: Vec<Option<Vec<u8>>>,
}

impl AltRow {
    /// Parse an ALTROW token after the token type byte has been read.
    /// Note: Requires knowing the column count from a preceding ALTFMT token.
    pub fn parse_with_column_count_sync<'s>(
        stream: &'s SliceStream<'s>,
        column_count: usize,
    ) -> Result<AltRow, SybaseParseError<SliceReadError, SybaseWireError>> {
        // ID (2 bytes)
        let id = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Parse column values
        let mut values = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            // Length prefix (1 byte for most types)
            let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            if len > 0 && len != 255 {
                let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                let data = borrow[..len].to_vec();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                values.push(Some(data));
            } else {
                values.push(None);
            }
        }

        Ok(AltRow { id, values })
    }

    /// Parse an ALTROW token with unknown column count (reads until format changes).
    /// This is a fallback when ALTFMT wasn't received.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<AltRow, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Without ALTFMT, we can only read the ID
        let id = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
        Ok(AltRow { id, values: Vec::new() })
    }
}
