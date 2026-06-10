//! TDS PARAMFMT and PARAMFMT2 tokens.
//!
//! These tokens describe the format of parameters in RPC calls and dynamic SQL.

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

type TypeInfo = (Option<u16>, Option<u8>, Option<u8>);

/// Parameter status flags.
pub mod param_status {
    /// Parameter allows null values.
    pub const NULLABLE: u8 = 0x20;
    /// Parameter is an output parameter.
    pub const OUTPUT: u8 = 0x01;
    /// Parameter has a default value.
    pub const DEFAULT: u8 = 0x02;
}

/// Parameter format information.
#[derive(Clone, Debug)]
pub struct ParamInfo {
    /// Parameter name.
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

impl ParamInfo {
    /// Check if the parameter is nullable.
    pub fn is_nullable(&self) -> bool {
        self.status & param_status::NULLABLE != 0
    }

    /// Check if the parameter is an output parameter.
    pub fn is_output(&self) -> bool {
        self.status & param_status::OUTPUT != 0
    }

    /// Check if the parameter has a default value.
    pub fn has_default(&self) -> bool {
        self.status & param_status::DEFAULT != 0
    }
}

/// PARAMFMT token (TDS 5.0).
///
/// Describes parameter formats for RPC and dynamic SQL.
#[derive(Clone, Debug)]
pub struct ParamFmt {
    /// Token length.
    pub length: u16,
    /// Number of parameters.
    pub param_count: u16,
    /// Parameter information.
    pub params: Vec<ParamInfo>,
}

impl ParamFmt {
    /// Parse a PARAMFMT token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<ParamFmt, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Parameter count (2 bytes)
        let param_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut params = Vec::with_capacity(param_count as usize);

        for _ in 0..param_count {
            // Parameter name
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

            // Locale (for some types)
            let locale = None; // Simplified - full implementation would parse locale

            params.push(ParamInfo {
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

        Ok(ParamFmt { length, param_count, params })
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

/// PARAMFMT2 token (TDS 5.0 extended).
///
/// Extended parameter format with additional metadata.
#[derive(Clone, Debug)]
pub struct ParamFmt2 {
    /// Token length (4 bytes for PARAMFMT2).
    pub length: u32,
    /// Number of parameters.
    pub param_count: u16,
    /// Parameter information.
    pub params: Vec<ParamInfo>,
}

impl ParamFmt2 {
    /// Parse a PARAMFMT2 token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<ParamFmt2, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (4 bytes for PARAMFMT2)
        let length = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Parameter count (2 bytes)
        let param_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        let mut params = Vec::with_capacity(param_count as usize);

        for _ in 0..param_count {
            // Parameter name (2-byte length for PARAMFMT2)
            let name_len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
            let name = if name_len > 0 {
                let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
                let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                n
            } else {
                String::new()
            };

            // Status (4 bytes for PARAMFMT2)
            let status = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as u8;

            // User type (4 bytes)
            let user_type = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

            // Data type
            let data_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

            // Parse type-specific info (same as PARAMFMT)
            let (max_length, precision, scale) = ParamFmt::parse_type_info(stream, data_type)?;

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

            params.push(ParamInfo {
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

        Ok(ParamFmt2 { length, param_count, params })
    }
}
