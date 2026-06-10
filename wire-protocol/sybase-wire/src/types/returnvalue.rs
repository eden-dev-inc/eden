//! TDS RETURNVALUE token.
//!
//! This token contains output parameter values returned from stored procedures.

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

type ParsedValue = (Option<u16>, Option<u8>, Option<u8>, Option<Vec<u8>>);

/// RETURNVALUE token.
///
/// Contains an output parameter value from a stored procedure.
#[derive(Clone, Debug)]
pub struct ReturnValue {
    /// Token length.
    pub length: u16,
    /// Parameter ordinal (position in parameter list).
    pub param_ordinal: u16,
    /// Parameter name.
    pub param_name: String,
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
    /// The actual value data.
    pub value: Option<Vec<u8>>,
}

impl ReturnValue {
    /// Parse a RETURNVALUE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(
        stream: &'s SliceStream<'s>,
    ) -> Result<ReturnValue, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Parameter ordinal (2 bytes)
        let param_ordinal = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Parameter name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let param_name = if name_len > 0 {
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

        // Parse type-specific metadata and value
        let (max_length, precision, scale, value) = Self::parse_value(stream, data_type)?;

        Ok(ReturnValue {
            length,
            param_ordinal,
            param_name,
            status,
            user_type,
            data_type,
            max_length,
            precision,
            scale,
            value,
        })
    }

    /// Parse the value based on data type.
    fn parse_value<'s>(
        stream: &'s SliceStream<'s>,
        data_type: u8,
    ) -> Result<ParsedValue, SybaseParseError<SliceReadError, SybaseWireError>> {
        match data_type {
            // Fixed-length integer types
            data_types::INT1TYPE => {
                let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(vec![v])))
            }
            data_types::INT2TYPE => {
                let v = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }
            data_types::INT4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }
            data_types::INT8TYPE => {
                let v = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }

            // Fixed-length float types
            data_types::FLT4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }
            data_types::FLT8TYPE => {
                let v = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }

            // Fixed-length money types
            data_types::MONEY4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }
            data_types::MONEYTYPE => {
                let v = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(v.to_le_bytes().to_vec())))
            }

            // Fixed-length datetime types
            data_types::DATETIM4TYPE => {
                let borrow = stream.peek(Some(4)).map_err(SybaseParseError::Stream)?;
                let data = borrow[..4].to_vec();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(data)))
            }
            data_types::DATETIMETYPE => {
                let borrow = stream.peek(Some(8)).map_err(SybaseParseError::Stream)?;
                let data = borrow[..8].to_vec();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(data)))
            }

            // Bit type
            data_types::BITTYPE => {
                let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok((None, None, None, Some(vec![v])))
            }

            // Nullable integer types
            data_types::INTNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Nullable float types
            data_types::FLTNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Nullable money types
            data_types::MONEYNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Nullable datetime types
            data_types::DATETIMNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Nullable bit type
            data_types::BITNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Decimal/Numeric types
            data_types::DECIMALNTYPE | data_types::NUMERICNTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let precision = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let scale = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), Some(precision), Some(scale), value))
            }

            // Character types
            data_types::CHARTYPE | data_types::VARCHARTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as u16;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len), None, None, value))
            }

            // Binary types
            data_types::BINARYTYPE | data_types::VARBINARYTYPE => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as u16;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len), None, None, value))
            }

            // Text/Image types with 4-byte length prefix
            data_types::TEXTTYPE | data_types::IMAGETYPE => {
                let max_len = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                // Text pointer (16 bytes) + timestamp (8 bytes)
                let borrow = stream.peek(Some(24)).map_err(SybaseParseError::Stream)?;
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                // Actual length
                let actual_len = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }

            // Default: read as binary with length prefix
            _ => {
                let max_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                let actual_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                let value = if actual_len > 0 && actual_len != 255 {
                    let borrow = stream.peek(Some(actual_len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..actual_len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Some(data)
                } else {
                    None
                };
                Ok((Some(max_len as u16), None, None, value))
            }
        }
    }
}
