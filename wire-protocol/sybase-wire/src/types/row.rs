//! TDS row data token.

use crate::error::{SybaseWireError, data_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

use super::colmetadata::{ColMetaData, ColumnType};

/// A single column value.
#[derive(Clone, Debug)]
pub enum ColumnValue {
    /// NULL value.
    Null,
    /// Tiny integer (1 byte).
    TinyInt(u8),
    /// Small integer (2 bytes).
    SmallInt(i16),
    /// Integer (4 bytes).
    Int(i32),
    /// Big integer (8 bytes).
    BigInt(i64),
    /// Single-precision float.
    Float(f32),
    /// Double-precision float.
    Double(f64),
    /// Boolean/bit.
    Bit(bool),
    /// Binary data.
    Binary(Vec<u8>),
    /// Character data.
    String(String),
    /// Decimal/Numeric value (stored as bytes).
    Decimal { precision: u8, scale: u8, value: Vec<u8> },
    /// Money value.
    Money(i64),
    /// Small money value.
    SmallMoney(i32),
    /// DateTime value (days since 1900-01-01, 1/300 seconds).
    DateTime { days: i32, time: u32 },
    /// SmallDateTime value.
    SmallDateTime { days: u16, minutes: u16 },
    /// GUID value.
    Guid([u8; 16]),
}

impl ColumnValue {
    /// Check if this is a NULL value.
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    /// Try to get as an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::TinyInt(v) => Some(*v as i64),
            ColumnValue::SmallInt(v) => Some(*v as i64),
            ColumnValue::Int(v) => Some(*v as i64),
            ColumnValue::BigInt(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ColumnValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            ColumnValue::Binary(b) => Some(b),
            ColumnValue::String(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

/// A row of column values.
#[derive(Clone, Debug)]
pub struct Row {
    /// Column values.
    pub values: Vec<ColumnValue>,
}

impl Row {
    /// Create a new row with the given values.
    pub fn new(values: Vec<ColumnValue>) -> Self {
        Self { values }
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by index.
    pub fn get(&self, index: usize) -> Option<&ColumnValue> {
        self.values.get(index)
    }

    /// Parse a ROW token after the token type byte has been read.
    pub fn parse_with_columns_sync<'s>(
        stream: &'s SliceStream<'s>,
        columns: &ColMetaData,
    ) -> Result<Row, SybaseParseError<SliceReadError, SybaseWireError>> {
        let mut values = Vec::with_capacity(columns.columns.len());

        for col in &columns.columns {
            let value = Self::parse_column_value(stream, &col.col_type)?;
            values.push(value);
        }

        Ok(Row { values })
    }

    /// Parse a single column value.
    fn parse_column_value<'s>(
        stream: &'s SliceStream<'s>,
        col_type: &ColumnType,
    ) -> Result<ColumnValue, SybaseParseError<SliceReadError, SybaseWireError>> {
        match col_type.type_id {
            // Fixed-length types
            data_types::NULLTYPE => Ok(ColumnValue::Null),

            data_types::INT1TYPE => {
                let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::TinyInt(v))
            }

            data_types::BITTYPE => {
                let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::Bit(v != 0))
            }

            data_types::INT2TYPE => {
                let v = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::SmallInt(v as i16))
            }

            data_types::INT4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::Int(v as i32))
            }

            data_types::INT8TYPE => {
                let v = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::BigInt(v as i64))
            }

            data_types::FLT4TYPE => {
                let bits = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::Float(f32::from_bits(bits)))
            }

            data_types::FLT8TYPE => {
                let bits = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::Double(f64::from_bits(bits)))
            }

            data_types::MONEY4TYPE => {
                let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as i32;
                Ok(ColumnValue::SmallMoney(v))
            }

            data_types::MONEYTYPE => {
                let hi = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                let lo = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                let v = ((hi as i64) << 32) | (lo as i64);
                Ok(ColumnValue::Money(v))
            }

            data_types::DATETIM4TYPE => {
                let days = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                let minutes = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::SmallDateTime { days, minutes })
            }

            data_types::DATETIMETYPE => {
                let days = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as i32;
                let time = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                Ok(ColumnValue::DateTime { days, time })
            }

            // Variable-length integer types
            data_types::INTNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                match len {
                    0 => Ok(ColumnValue::Null),
                    1 => {
                        let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::TinyInt(v))
                    }
                    2 => {
                        let v = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::SmallInt(v as i16))
                    }
                    4 => {
                        let v = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::Int(v as i32))
                    }
                    8 => {
                        let v = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::BigInt(v as i64))
                    }
                    _ => Err(SybaseParseError::Parse(SybaseWireError::InvalidDataType(col_type.type_id))),
                }
            }

            // Variable-length float types
            data_types::FLTNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                match len {
                    0 => Ok(ColumnValue::Null),
                    4 => {
                        let bits = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::Float(f32::from_bits(bits)))
                    }
                    8 => {
                        let bits = stream.read_u64_le_sync().map_err(SybaseParseError::Stream)?;
                        Ok(ColumnValue::Double(f64::from_bits(bits)))
                    }
                    _ => Err(SybaseParseError::Parse(SybaseWireError::InvalidDataType(col_type.type_id))),
                }
            }

            // Variable-length bit type
            data_types::BITNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                if len == 0 {
                    Ok(ColumnValue::Null)
                } else {
                    let v = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::Bit(v != 0))
                }
            }

            // GUID type
            data_types::GUIDTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
                if len == 0 {
                    Ok(ColumnValue::Null)
                } else {
                    let borrow = stream.peek(Some(16)).map_err(SybaseParseError::Stream)?;
                    let mut guid = [0u8; 16];
                    guid.copy_from_slice(&borrow[..16]);
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::Guid(guid))
                }
            }

            // Character/binary types with 1-byte length
            data_types::VARCHARTYPE | data_types::CHARTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                if len == 0 {
                    Ok(ColumnValue::String(String::new()))
                } else {
                    let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                    let s = String::from_utf8_lossy(&borrow[..len]).into_owned();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::String(s))
                }
            }

            data_types::VARBINARYTYPE | data_types::BINARYTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                if len == 0 {
                    Ok(ColumnValue::Binary(Vec::new()))
                } else {
                    let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::Binary(data))
                }
            }

            // Decimal/Numeric types
            data_types::DECIMALNTYPE | data_types::NUMERICNTYPE => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                if len == 0 {
                    Ok(ColumnValue::Null)
                } else {
                    let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                    let value = borrow[..len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::Decimal {
                        precision: col_type.precision.unwrap_or(18),
                        scale: col_type.scale.unwrap_or(0),
                        value,
                    })
                }
            }

            // Default: read as binary with 1-byte length
            _ => {
                let len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
                if len == 0 || len == 255 {
                    // 255 often means NULL for some types
                    Ok(ColumnValue::Null)
                } else {
                    let borrow = stream.peek(Some(len)).map_err(SybaseParseError::Stream)?;
                    let data = borrow[..len].to_vec();
                    stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                    Ok(ColumnValue::Binary(data))
                }
            }
        }
    }
}
