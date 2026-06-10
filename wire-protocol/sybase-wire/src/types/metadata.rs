//! TDS metadata tokens (TABNAME, COLINFO, ORDERBY).
//!
//! These tokens provide additional metadata about result sets.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// TABNAME token.
///
/// Contains table names for result set columns.
#[derive(Clone, Debug)]
pub struct TabName {
    /// Token length.
    pub length: u16,
    /// Table names.
    pub tables: Vec<String>,
}

impl TabName {
    /// Parse a TABNAME token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<TabName, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut tables = Vec::new();
        let mut bytes_read = 0u16;

        // Read table names until we've consumed length bytes
        while bytes_read < length {
            // Table name length (1 byte)
            let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            bytes_read += 1;

            if name_len > 0 {
                let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
                let name = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                bytes_read += name_len as u16;
                tables.push(name);
            } else {
                tables.push(String::new());
            }
        }

        Ok(TabName { length, tables })
    }
}

/// COLINFO column info entry.
#[derive(Clone, Debug)]
pub struct ColInfoEntry {
    /// Column ordinal (1-based).
    pub column_ordinal: u8,
    /// Table index (1-based, 0 means no table).
    pub table_index: u8,
    /// Column status flags.
    pub status: u8,
    /// Actual column name (if different from label).
    pub column_name: Option<String>,
}

/// COLINFO status flags.
pub mod colinfo_status {
    /// Column is an expression.
    pub const EXPRESSION: u8 = 0x04;
    /// Column has different actual name.
    pub const DIFFERENT_NAME: u8 = 0x20;
}

/// COLINFO token.
///
/// Provides additional column metadata including table associations.
#[derive(Clone, Debug)]
pub struct ColInfo {
    /// Token length.
    pub length: u16,
    /// Column information entries.
    pub columns: Vec<ColInfoEntry>,
}

impl ColInfo {
    /// Parse a COLINFO token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<ColInfo, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut columns = Vec::new();
        let mut bytes_read = 0u16;

        // Read column info entries until we've consumed length bytes
        while bytes_read < length {
            // Column ordinal (1 byte)
            let column_ordinal = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
            bytes_read += 1;

            // Table index (1 byte)
            let table_index = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
            bytes_read += 1;

            // Status (1 byte)
            let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
            bytes_read += 1;

            // Column name (only if DIFFERENT_NAME flag is set)
            let column_name = if status & colinfo_status::DIFFERENT_NAME != 0 {
                let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                bytes_read += 1;
                if name_len > 0 {
                    let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
                    let name = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    bytes_read += name_len as u16;
                    Some(name)
                } else {
                    Some(String::new())
                }
            } else {
                None
            };

            columns.push(ColInfoEntry { column_ordinal, table_index, status, column_name });
        }

        Ok(ColInfo { length, columns })
    }
}

/// ORDERBY token.
///
/// Indicates the order of columns in the result set (for ORDER BY processing).
#[derive(Clone, Debug)]
pub struct OrderBy {
    /// Token length.
    pub length: u16,
    /// Column ordinals in order (1-based).
    pub columns: Vec<u16>,
}

impl OrderBy {
    /// Parse an ORDERBY token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<OrderBy, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Number of columns = length / 2
        let count = length as usize / 2;
        let mut columns = Vec::with_capacity(count);

        for _ in 0..count {
            let ordinal = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
            columns.push(ordinal);
        }

        Ok(OrderBy { length, columns })
    }
}

/// CONTROL token.
///
/// Contains format control information for result columns.
#[derive(Clone, Debug)]
pub struct Control {
    /// Token length.
    pub length: u16,
    /// Format strings for each column.
    pub formats: Vec<String>,
}

impl Control {
    /// Parse a CONTROL token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Control, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut formats = Vec::new();
        let mut bytes_read = 0u16;

        // Read format strings until we've consumed length bytes
        while bytes_read < length {
            // Format length (1 byte)
            let fmt_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            bytes_read += 1;

            if fmt_len > 0 {
                let borrow = stream.peek(Some(fmt_len)).map_err(SybaseParseError::Stream)?;
                let fmt = String::from_utf8_lossy(&borrow[..fmt_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                bytes_read += fmt_len as u16;
                formats.push(fmt);
            } else {
                formats.push(String::new());
            }
        }

        Ok(Control { length, formats })
    }
}
