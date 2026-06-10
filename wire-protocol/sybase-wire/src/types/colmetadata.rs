//! TDS column metadata token.

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

type TypeInfo = (Option<u16>, Option<u8>, Option<u8>);

/// Column data type information.
#[derive(Clone, Debug)]
pub struct ColumnType {
    /// Type identifier.
    pub type_id: u8,
    /// Maximum length (for variable-length types).
    pub max_length: Option<u16>,
    /// Precision (for decimal/numeric).
    pub precision: Option<u8>,
    /// Scale (for decimal/numeric).
    pub scale: Option<u8>,
}

impl ColumnType {
    /// Check if this is a fixed-length type.
    pub fn is_fixed_length(&self) -> bool {
        matches!(
            self.type_id,
            data_types::NULLTYPE
                | data_types::INT1TYPE
                | data_types::BITTYPE
                | data_types::INT2TYPE
                | data_types::INT4TYPE
                | data_types::INT8TYPE
                | data_types::DATETIM4TYPE
                | data_types::DATETIMETYPE
                | data_types::FLT4TYPE
                | data_types::FLT8TYPE
                | data_types::MONEYTYPE
                | data_types::MONEY4TYPE
        )
    }

    /// Get the fixed size in bytes for fixed-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self.type_id {
            data_types::NULLTYPE => Some(0),
            data_types::INT1TYPE | data_types::BITTYPE => Some(1),
            data_types::INT2TYPE => Some(2),
            data_types::INT4TYPE | data_types::DATETIM4TYPE | data_types::FLT4TYPE | data_types::MONEY4TYPE => Some(4),
            data_types::INT8TYPE | data_types::DATETIMETYPE | data_types::FLT8TYPE | data_types::MONEYTYPE => Some(8),
            _ => None,
        }
    }

    /// Check if this type is nullable (variable-length).
    pub fn is_nullable(&self) -> bool {
        !self.is_fixed_length()
    }
}

/// Column metadata.
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column type.
    pub col_type: ColumnType,
    /// Status flags.
    pub status: u8,
    /// User type (for UDTs).
    pub user_type: Option<u32>,
}

impl ColumnInfo {
    /// Check if the column is nullable.
    pub fn is_nullable(&self) -> bool {
        self.col_type.is_nullable() || (self.status & 0x01) != 0
    }

    /// Check if the column is an identity column.
    pub fn is_identity(&self) -> bool {
        (self.status & 0x10) != 0
    }
}

/// Column metadata token.
///
/// Describes the columns in a result set.
#[derive(Clone, Debug)]
pub struct ColMetaData {
    /// Number of columns.
    pub column_count: u16,
    /// Column information.
    pub columns: Vec<ColumnInfo>,
}

impl ColMetaData {
    /// Parse a COLMETADATA token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(
        stream: &'s SliceStream<'s>,
    ) -> Result<ColMetaData, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Column count (2 bytes, little-endian)
        let column_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Special case: 0xFFFF means no metadata
        if column_count == 0xFFFF {
            return Ok(ColMetaData { column_count: 0, columns: Vec::new() });
        }

        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            // User type (4 bytes for TDS 7.2+, 2 bytes for earlier)
            // For Sybase TDS 5.0, it's typically 4 bytes
            let user_type = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

            // Flags (2 bytes)
            let flags = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
            let status = (flags & 0xFF) as u8;

            // Type ID (1 byte)
            let type_id = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Parse type-specific information
            let (max_length, precision, scale) = Self::parse_type_info(stream, type_id)?;

            let col_type = ColumnType { type_id, max_length, precision, scale };

            // Column name (length-prefixed)
            let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
            let name = if name_len > 0 {
                let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
                let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                n
            } else {
                String::new()
            };

            columns.push(ColumnInfo { name, col_type, status, user_type: Some(user_type) });
        }

        Ok(ColMetaData { column_count, columns })
    }

    /// Parse type-specific information (length, precision, scale).
    fn parse_type_info<'s>(
        stream: &'s SliceStream<'s>,
        type_id: u8,
    ) -> Result<TypeInfo, SybaseParseError<SliceReadError, SybaseWireError>> {
        match type_id {
            // Fixed-length types - no additional info
            data_types::NULLTYPE
            | data_types::INT1TYPE
            | data_types::BITTYPE
            | data_types::INT2TYPE
            | data_types::INT4TYPE
            | data_types::INT8TYPE
            | data_types::DATETIM4TYPE
            | data_types::DATETIMETYPE
            | data_types::FLT4TYPE
            | data_types::FLT8TYPE
            | data_types::MONEYTYPE
            | data_types::MONEY4TYPE => Ok((None, None, None)),

            // Variable-length types with 1-byte length
            data_types::INTNTYPE
            | data_types::BITNTYPE
            | data_types::FLTNTYPE
            | data_types::MONEYNTYPE
            | data_types::DATETIMNTYPE
            | data_types::GUIDTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Decimal/Numeric with precision and scale
            data_types::DECIMALTYPE | data_types::NUMERICTYPE | data_types::DECIMALNTYPE | data_types::NUMERICNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let precision = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let scale = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), Some(precision), Some(scale)))
            }

            // Character types with 1-byte length
            data_types::CHARTYPE | data_types::VARCHARTYPE | data_types::BINARYTYPE | data_types::VARBINARYTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Large object types with 4-byte length
            data_types::TEXTTYPE | data_types::IMAGETYPE | data_types::NTEXTTYPE => {
                let len = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }

            // Unicode types with 2-byte length
            // Note: NCHARTYPE (0x6F) overlaps with DATETIMNTYPE in Sybase TDS 5.0
            // so we only match NVARCHARTYPE here
            data_types::NVARCHARTYPE => {
                let len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len), None, None))
            }

            // Default: assume 1-byte length for unknown types
            _ => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((Some(len as u16), None, None))
            }
        }
    }
}
