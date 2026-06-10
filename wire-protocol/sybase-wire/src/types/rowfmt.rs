//! TDS ROWFMT and ROWFMT2 tokens.
//!
//! These tokens describe the format of result set rows, similar to COLMETADATA
//! but used in different contexts (primarily with TDS 5.0 dynamic SQL).

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

type TypeInfo = (Option<u16>, Option<u8>, Option<u8>);

/// Column format information for ROWFMT.
#[derive(Clone, Debug)]
pub struct RowFmtColumn {
    /// Column name (label).
    pub name: String,
    /// Status flags.
    pub status: u8,
    /// User type (for UDTs).
    pub user_type: u32,
    /// Data type.
    pub data_type: u8,
    /// Maximum length (for variable-length types).
    pub max_length: Option<u16>,
    /// Precision (for decimal/numeric).
    pub precision: Option<u8>,
    /// Scale (for decimal/numeric).
    pub scale: Option<u8>,
    /// Locale information (TDS 5.0).
    pub locale: Option<String>,
}

/// ROWFMT token (TDS 5.0).
///
/// Describes the format of result set rows for dynamic SQL.
#[derive(Clone, Debug)]
pub struct RowFmt {
    /// Token length.
    pub length: u16,
    /// Number of columns.
    pub column_count: u16,
    /// Column information.
    pub columns: Vec<RowFmtColumn>,
}

impl RowFmt {
    /// Parse a ROWFMT token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<RowFmt, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Column count (2 bytes)
        let column_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            // Column name
            let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            let name = if name_len > 0 {
                let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
                let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                n
            } else {
                String::new()
            };

            // Status
            let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // User type (4 bytes)
            let user_type = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

            // Data type
            let data_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Parse type-specific info
            let (max_length, precision, scale) = Self::parse_type_info(stream, data_type)?;

            // Locale (for some types) - simplified
            let locale = None;

            columns.push(RowFmtColumn {
                name,
                status,
                user_type,
                data_type,
                max_length,
                precision,
                scale,
                locale,
            });
        }

        Ok(RowFmt { length, column_count, columns })
    }

    /// Parse type-specific information.
    fn parse_type_info<'s>(
        stream: &'s SliceStream<'s>,
        data_type: u8,
    ) -> Result<TypeInfo, SybaseParseError<SliceReadError, SybaseWireError>> {
        match data_type {
            // Fixed-length types - no additional info
            data_types::INT1TYPE
            | data_types::INT2TYPE
            | data_types::INT4TYPE
            | data_types::INT8TYPE
            | data_types::FLT4TYPE
            | data_types::FLT8TYPE
            | data_types::BITTYPE
            | data_types::MONEYTYPE
            | data_types::MONEY4TYPE
            | data_types::DATETIMETYPE
            | data_types::DATETIM4TYPE => Ok((None, None, None)),

            // Variable-length types with 1-byte max length
            data_types::INTNTYPE | data_types::FLTNTYPE | data_types::MONEYNTYPE | data_types::DATETIMNTYPE | data_types::BITNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Decimal/Numeric with precision and scale
            data_types::DECIMALNTYPE | data_types::NUMERICNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let precision = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let scale = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), Some(precision), Some(scale)))
            }

            // Character/binary types with 1-byte length
            data_types::CHARTYPE | data_types::VARCHARTYPE | data_types::BINARYTYPE | data_types::VARBINARYTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Text/image types with 4-byte length
            data_types::TEXTTYPE | data_types::IMAGETYPE => {
                let len = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Default: assume 1-byte length
            _ => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }
        }
    }
}

/// ROWFMT2 token (TDS 5.0 extended).
///
/// Extended row format with additional metadata (catalog, schema, table names).
#[derive(Clone, Debug)]
pub struct RowFmt2 {
    /// Token length (4 bytes for ROWFMT2).
    pub length: u32,
    /// Number of columns.
    pub column_count: u16,
    /// Column information.
    pub columns: Vec<RowFmt2Column>,
}

/// Extended column format information for ROWFMT2.
#[derive(Clone, Debug)]
pub struct RowFmt2Column {
    /// Column label (display name).
    pub label: String,
    /// Catalog name (database).
    pub catalog: String,
    /// Schema name (owner).
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Column name (actual name).
    pub column_name: String,
    /// Status flags (4 bytes).
    pub status: u32,
    /// User type (for UDTs).
    pub user_type: u32,
    /// Data type.
    pub data_type: u8,
    /// Maximum length (for variable-length types).
    pub max_length: Option<u16>,
    /// Precision (for decimal/numeric).
    pub precision: Option<u8>,
    /// Scale (for decimal/numeric).
    pub scale: Option<u8>,
    /// Locale information.
    pub locale: Option<String>,
}

impl RowFmt2 {
    /// Parse a ROWFMT2 token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<RowFmt2, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (4 bytes for ROWFMT2)
        let length = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Column count (2 bytes)
        let column_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            // Label name (1-byte length)
            let label = Self::read_string_u8(stream)?;

            // Catalog name (1-byte length)
            let catalog = Self::read_string_u8(stream)?;

            // Schema name (1-byte length)
            let schema = Self::read_string_u8(stream)?;

            // Table name (1-byte length)
            let table = Self::read_string_u8(stream)?;

            // Column name (1-byte length)
            let column_name = Self::read_string_u8(stream)?;

            // Status (4 bytes for ROWFMT2)
            let status = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

            // User type (4 bytes)
            let user_type = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

            // Data type
            let data_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Parse type-specific info
            let (max_length, precision, scale) = RowFmt::parse_type_info(stream, data_type)?;

            // Locale length and value
            let locale_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            let locale = if locale_len > 0 {
                let borrow = stream.peek(Some(locale_len)).map_err(SybaseParseError::Stream)?;
                let l = String::from_utf8_lossy(&borrow[..locale_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                Some(l)
            } else {
                None
            };

            columns.push(RowFmt2Column {
                label,
                catalog,
                schema,
                table,
                column_name,
                status,
                user_type,
                data_type,
                max_length,
                precision,
                scale,
                locale,
            });
        }

        Ok(RowFmt2 { length, column_count, columns })
    }

    /// Helper to read a string with 1-byte length prefix.
    fn read_string_u8<'s>(stream: &'s SliceStream<'s>) -> Result<String, SybaseParseError<SliceReadError, SybaseWireError>> {
        let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        if len > 0 {
            let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
            let s = String::from_utf8_lossy(&borrow[..len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            Ok(s)
        } else {
            Ok(String::new())
        }
    }
}
