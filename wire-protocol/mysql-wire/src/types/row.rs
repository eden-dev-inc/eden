//! MySQL row data packets.
//!
//! Rows can be in text protocol (from COM_QUERY) or binary protocol
//! (from COM_STMT_EXECUTE).

use crate::mysql_ext::MysqlReadSync;
use crate::parse::MysqlParseError;
use wire_stream::WireReadSync;

/// Text protocol row data.
///
/// Column values are length-encoded strings, or 0xFB for NULL.
#[derive(Clone, Debug)]
pub struct TextRow {
    /// Column values (None for NULL).
    pub values: Vec<Option<Vec<u8>>>,
}

impl TextRow {
    /// Create a new text row.
    pub fn new(values: Vec<Option<Vec<u8>>>) -> Self {
        Self { values }
    }

    /// Get a column value by index.
    pub fn get(&self, index: usize) -> Option<&Option<Vec<u8>>> {
        self.values.get(index)
    }

    /// Get a column value as a string.
    pub fn get_string(&self, index: usize) -> Option<String> {
        self.values.get(index).and_then(|v| v.as_ref().map(|bytes| String::from_utf8_lossy(bytes).into_owned()))
    }

    /// Get a column value as an i64.
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.get_string(index).and_then(|s| s.parse().ok())
    }

    /// Get a column value as an f64.
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.get_string(index).and_then(|s| s.parse().ok())
    }

    /// Check if a column is NULL.
    pub fn is_null(&self, index: usize) -> bool {
        self.values.get(index).map(|v| v.is_none()).unwrap_or(true)
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum TextRowError {
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
    #[error("row data too short")]
    TooShort,
}

impl TextRow {
    /// Parse a text row with known column count.
    pub fn parse_with_columns_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        column_count: usize,
    ) -> Result<Self, MysqlParseError<S::ReadError, TextRowError>> {
        let mut values = Vec::with_capacity(column_count);

        for _ in 0..column_count {
            // Peek at first byte to check for NULL
            let first = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

            if first == 0xFB {
                // NULL value
                values.push(None);
            } else {
                // Length-encoded string - first byte is part of length
                let len = match first {
                    0..=0xFA => first as u64,
                    0xFC => stream.read_u16_le_sync().map_err(MysqlParseError::Stream)? as u64,
                    0xFD => stream.read_u24_le_sync().map_err(MysqlParseError::Stream)? as u64,
                    0xFE => stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?,
                    _ => {
                        return Err(MysqlParseError::Parse(TextRowError::InvalidLenEnc));
                    }
                };

                let data = stream.read_bytes_sync(len as usize).map_err(MysqlParseError::Stream)?;
                values.push(Some(data));
            }
        }

        Ok(TextRow { values })
    }
}

/// Binary protocol row data.
///
/// Used with prepared statements (COM_STMT_EXECUTE).
#[derive(Clone, Debug)]
pub struct BinaryRow {
    /// Column values (None for NULL).
    pub values: Vec<Option<Vec<u8>>>,
}

impl BinaryRow {
    /// Create a new binary row.
    pub fn new(values: Vec<Option<Vec<u8>>>) -> Self {
        Self { values }
    }

    /// Get a column value by index.
    pub fn get(&self, index: usize) -> Option<&Option<Vec<u8>>> {
        self.values.get(index)
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Check if a column is NULL.
    pub fn is_null(&self, index: usize) -> bool {
        self.values.get(index).map(|v| v.is_none()).unwrap_or(true)
    }

    /// Get a column value as i8 (MYSQL_TYPE_TINY signed).
    pub fn get_i8(&self, index: usize) -> Option<i8> {
        self.values
            .get(index)
            .and_then(|v| v.as_ref().and_then(|bytes| if !bytes.is_empty() { Some(bytes[0] as i8) } else { None }))
    }

    /// Get a column value as u8 (MYSQL_TYPE_TINY unsigned).
    pub fn get_u8(&self, index: usize) -> Option<u8> {
        self.values.get(index).and_then(|v| v.as_ref().and_then(|bytes| bytes.first().copied()))
    }

    /// Get a column value as i16 (MYSQL_TYPE_SHORT signed).
    pub fn get_i16(&self, index: usize) -> Option<i16> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 2 {
                    Some(i16::from_le_bytes([bytes[0], bytes[1]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as u16 (MYSQL_TYPE_SHORT unsigned, MYSQL_TYPE_YEAR).
    pub fn get_u16(&self, index: usize) -> Option<u16> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 2 {
                    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as i32 (MYSQL_TYPE_LONG signed, MYSQL_TYPE_INT24).
    pub fn get_i32(&self, index: usize) -> Option<i32> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 4 {
                    Some(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                } else if bytes.len() >= 3 {
                    // INT24
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
                    // Sign extend if negative (bit 23 set)
                    if bytes[2] & 0x80 != 0 {
                        Some(val | 0xFF000000u32 as i32)
                    } else {
                        Some(val)
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as u32 (MYSQL_TYPE_LONG unsigned).
    pub fn get_u32(&self, index: usize) -> Option<u32> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 4 {
                    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as i64 (MYSQL_TYPE_LONGLONG signed).
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 8 {
                    Some(i64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as u64 (MYSQL_TYPE_LONGLONG unsigned).
    pub fn get_u64(&self, index: usize) -> Option<u64> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 8 {
                    Some(u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as f32 (MYSQL_TYPE_FLOAT).
    pub fn get_f32(&self, index: usize) -> Option<f32> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 4 {
                    Some(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as f64 (MYSQL_TYPE_DOUBLE).
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 8 {
                    Some(f64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as a string (for string/text types).
    pub fn get_string(&self, index: usize) -> Option<String> {
        self.values.get(index).and_then(|v| v.as_ref().map(|bytes| String::from_utf8_lossy(bytes).into_owned()))
    }

    /// Get a column value as raw bytes (for BLOB types).
    pub fn get_bytes(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index).and_then(|v| v.as_ref().map(|b| b.as_slice()))
    }

    /// Get a column value as a date (MYSQL_TYPE_DATE).
    ///
    /// Returns (year, month, day).
    pub fn get_date(&self, index: usize) -> Option<(u16, u8, u8)> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.len() >= 4 {
                    let year = u16::from_le_bytes([bytes[0], bytes[1]]);
                    let month = bytes[2];
                    let day = bytes[3];
                    Some((year, month, day))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as a time (MYSQL_TYPE_TIME).
    ///
    /// Returns (is_negative, days, hours, minutes, seconds, microseconds).
    pub fn get_time(&self, index: usize) -> Option<(bool, u32, u8, u8, u8, u32)> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.is_empty() {
                    return Some((false, 0, 0, 0, 0, 0));
                }
                if bytes.len() >= 8 {
                    let is_negative = bytes[0] != 0;
                    let days = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                    let hours = bytes[5];
                    let minutes = bytes[6];
                    let seconds = bytes[7];
                    let micros = if bytes.len() >= 12 {
                        u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]])
                    } else {
                        0
                    };
                    Some((is_negative, days, hours, minutes, seconds, micros))
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as a datetime (MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP).
    ///
    /// Returns (year, month, day, hour, minute, second, microseconds).
    pub fn get_datetime(&self, index: usize) -> Option<(u16, u8, u8, u8, u8, u8, u32)> {
        self.values.get(index).and_then(|v| {
            v.as_ref().and_then(|bytes| {
                if bytes.is_empty() {
                    return Some((0, 0, 0, 0, 0, 0, 0));
                }
                if bytes.len() >= 4 {
                    let year = u16::from_le_bytes([bytes[0], bytes[1]]);
                    let month = bytes[2];
                    let day = bytes[3];

                    if bytes.len() >= 7 {
                        let hour = bytes[4];
                        let minute = bytes[5];
                        let second = bytes[6];
                        let micros = if bytes.len() >= 11 {
                            u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]])
                        } else {
                            0
                        };
                        Some((year, month, day, hour, minute, second, micros))
                    } else {
                        Some((year, month, day, 0, 0, 0, 0))
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Get a column value as a year (MYSQL_TYPE_YEAR).
    pub fn get_year(&self, index: usize) -> Option<u16> {
        self.get_u16(index)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum BinaryRowError {
    #[error("invalid binary row header")]
    InvalidHeader,
    #[error("invalid null bitmap")]
    InvalidNullBitmap,
    #[error("binary row data too short")]
    TooShort,
}

impl BinaryRow {
    /// Parse a binary row with known column count and types.
    ///
    /// Binary rows have:
    /// - 1 byte header (always 0x00)
    /// - NULL bitmap: (column_count + 7 + 2) / 8 bytes
    /// - Column values (not NULL columns only)
    pub fn parse_with_columns_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        column_count: usize,
        column_types: &[u8],
    ) -> Result<Self, MysqlParseError<S::ReadError, BinaryRowError>> {
        // Header (0x00)
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != 0x00 {
            return Err(MysqlParseError::Parse(BinaryRowError::InvalidHeader));
        }

        // NULL bitmap
        // Offset of 2 bits at the start (for compatibility)
        let null_bitmap_len = (column_count + 7 + 2) / 8;
        let null_bitmap = stream.read_bytes_sync(null_bitmap_len).map_err(MysqlParseError::Stream)?;

        // Parse values
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            // Check NULL bitmap (with offset of 2)
            let byte_pos = (i + 2) / 8;
            let bit_pos = (i + 2) % 8;

            if null_bitmap[byte_pos] & (1 << bit_pos) != 0 {
                // NULL value
                values.push(None);
            } else {
                // Read value based on column type
                let col_type = column_types.get(i).copied().unwrap_or(0);
                let data = read_binary_value(stream, col_type)?;
                values.push(Some(data));
            }
        }

        Ok(BinaryRow { values })
    }
}

/// Read a binary value based on column type.
fn read_binary_value<S: WireReadSync + ?Sized>(stream: &S, col_type: u8) -> Result<Vec<u8>, MysqlParseError<S::ReadError, BinaryRowError>> {
    use crate::error::column_types::*;

    match col_type {
        MYSQL_TYPE_TINY => {
            let val = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            Ok(vec![val])
        }
        MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
            let val = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(val.to_le_bytes().to_vec())
        }
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
            let val = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(val.to_le_bytes().to_vec())
        }
        MYSQL_TYPE_LONGLONG => {
            let val = stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(val.to_le_bytes().to_vec())
        }
        MYSQL_TYPE_FLOAT => {
            let val = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(val.to_le_bytes().to_vec())
        }
        MYSQL_TYPE_DOUBLE => {
            let val = stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(val.to_le_bytes().to_vec())
        }
        // String types use length-encoded strings
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_JSON
        | MYSQL_TYPE_GEOMETRY
        | MYSQL_TYPE_BIT
        | MYSQL_TYPE_DECIMAL
        | MYSQL_TYPE_NEWDECIMAL
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET => {
            let result = stream
                .read_lenenc_string_sync()
                .map_err(MysqlParseError::Stream)?
                .map_err(|_| MysqlParseError::Parse(BinaryRowError::TooShort))?;
            Ok(result)
        }
        // Date/time types
        MYSQL_TYPE_DATE | MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP => {
            // Read length byte first
            let len = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            let data = stream.read_bytes_sync(len as usize).map_err(MysqlParseError::Stream)?;
            Ok(data)
        }
        MYSQL_TYPE_TIME => {
            let len = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            let data = stream.read_bytes_sync(len as usize).map_err(MysqlParseError::Stream)?;
            Ok(data)
        }
        MYSQL_TYPE_NULL => Ok(Vec::new()),
        _ => {
            // Unknown type - try to read as length-encoded string
            let result = stream
                .read_lenenc_string_sync()
                .map_err(MysqlParseError::Stream)?
                .map_err(|_| MysqlParseError::Parse(BinaryRowError::TooShort))?;
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::write_lenenc_string;
    use wire_stream::SliceStream;

    #[test]
    fn test_text_row_simple() {
        let mut data = Vec::new();
        write_lenenc_string(&mut data, b"hello").unwrap();
        write_lenenc_string(&mut data, b"world").unwrap();

        let stream = SliceStream::new(&data);
        let row = TextRow::parse_with_columns_sync(&stream, 2).unwrap();

        assert_eq!(row.len(), 2);
        assert_eq!(row.get_string(0), Some("hello".to_string()));
        assert_eq!(row.get_string(1), Some("world".to_string()));
    }

    #[test]
    fn test_text_row_with_null() {
        let mut data = Vec::new();
        write_lenenc_string(&mut data, b"value").unwrap();
        data.push(0xFB); // NULL

        let stream = SliceStream::new(&data);
        let row = TextRow::parse_with_columns_sync(&stream, 2).unwrap();

        assert_eq!(row.len(), 2);
        assert!(!row.is_null(0));
        assert!(row.is_null(1));
    }

    #[test]
    fn test_text_row_numeric() {
        let mut data = Vec::new();
        write_lenenc_string(&mut data, b"42").unwrap();
        write_lenenc_string(&mut data, b"3.25").unwrap();

        let stream = SliceStream::new(&data);
        let row = TextRow::parse_with_columns_sync(&stream, 2).unwrap();

        assert_eq!(row.get_i64(0), Some(42));
        assert!((row.get_f64(1).unwrap() - 3.25).abs() < 0.001);
    }
}
