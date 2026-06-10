//! Data type conversion utilities.
//!
//! Converts raw TDS data bytes to Rust types and vice versa.

use crate::error::SybaseWireError;

/// TDS data type codes.
pub mod type_codes {
    // Fixed-length types
    pub const INT1: u8 = 0x30; // TINYINT (1 byte)
    pub const INT2: u8 = 0x34; // SMALLINT (2 bytes)
    pub const INT4: u8 = 0x38; // INT (4 bytes)
    pub const INT8: u8 = 0x7F; // BIGINT (8 bytes)
    pub const FLT4: u8 = 0x3B; // REAL (4 bytes)
    pub const FLT8: u8 = 0x3E; // FLOAT (8 bytes)
    pub const BIT: u8 = 0x32; // BIT (1 byte)
    pub const MONEY4: u8 = 0x7A; // SMALLMONEY (4 bytes)
    pub const MONEY: u8 = 0x3C; // MONEY (8 bytes)
    pub const DATETIME4: u8 = 0x3A; // SMALLDATETIME (4 bytes)
    pub const DATETIME: u8 = 0x3D; // DATETIME (8 bytes)

    // Variable-length types
    pub const INTN: u8 = 0x26; // Nullable integer
    pub const FLTN: u8 = 0x6D; // Nullable float
    pub const BITN: u8 = 0x68; // Nullable bit
    pub const MONEYN: u8 = 0x6E; // Nullable money
    pub const DATETIMN: u8 = 0x6F; // Nullable datetime

    // String types
    pub const CHAR: u8 = 0x2F; // Fixed-length char
    pub const VARCHAR: u8 = 0x27; // Variable-length char
    pub const LONGCHAR: u8 = 0xAF; // Long varchar
    pub const TEXT: u8 = 0x23; // Text

    // Binary types
    pub const BINARY: u8 = 0x2D; // Fixed-length binary
    pub const VARBINARY: u8 = 0x25; // Variable-length binary
    pub const LONGBINARY: u8 = 0xE1; // Long varbinary
    pub const IMAGE: u8 = 0x22; // Image

    // Unicode types
    pub const UNITEXT: u8 = 0xAE; // Unicode text
    pub const UNIVARCHAR: u8 = 0x9B; // Unicode varchar

    // Numeric types
    pub const NUMERIC: u8 = 0x6C; // Numeric
    pub const NUMERICN: u8 = 0x6C; // Nullable numeric (same as NUMERIC)
    pub const DECIMAL: u8 = 0x6A; // Decimal
    pub const DECIMALN: u8 = 0x6A; // Nullable decimal (same as DECIMAL)

    // Date/time types (TDS 7.3+)
    pub const DATE: u8 = 0x31; // Date only
    pub const TIME: u8 = 0x33; // Time only
    pub const DATETIME2: u8 = 0x2A; // DateTime2
    pub const DATETIMEOFFSET: u8 = 0x2B; // DateTimeOffset

    // Other types
    pub const GUID: u8 = 0x24; // GUID/UUID
    pub const XML: u8 = 0xF1; // XML
    pub const UDT: u8 = 0xF0; // User-defined type
}

/// A decoded TDS value.
#[derive(Clone, Debug, PartialEq)]
pub enum TdsValue {
    /// Null value.
    Null,
    /// Tiny integer (TINYINT).
    TinyInt(u8),
    /// Small integer (SMALLINT).
    SmallInt(i16),
    /// Integer (INT).
    Int(i32),
    /// Big integer (BIGINT).
    BigInt(i64),
    /// Single-precision float (REAL).
    Real(f32),
    /// Double-precision float (FLOAT).
    Float(f64),
    /// Boolean (BIT).
    Bit(bool),
    /// Small money (SMALLMONEY).
    SmallMoney(i32),
    /// Money (MONEY).
    Money(i64),
    /// Small datetime (SMALLDATETIME).
    SmallDateTime { days: u16, minutes: u16 },
    /// Datetime (DATETIME).
    DateTime { days: i32, time_ticks: u32 },
    /// String (CHAR, VARCHAR, TEXT).
    String(String),
    /// Binary (BINARY, VARBINARY, IMAGE).
    Binary(Vec<u8>),
    /// Numeric/Decimal.
    Numeric { precision: u8, scale: u8, value: i128 },
    /// GUID/UUID.
    Guid([u8; 16]),
    /// Date only.
    Date { days: u32 },
    /// Time only.
    Time { ticks: u64, scale: u8 },
    /// DateTime2.
    DateTime2 { days: u32, ticks: u64, scale: u8 },
    /// DateTimeOffset.
    DateTimeOffset {
        days: u32,
        ticks: u64,
        scale: u8,
        offset_minutes: i16,
    },
    /// Raw bytes (for unrecognized types).
    Raw(Vec<u8>),
}

impl TdsValue {
    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, TdsValue::Null)
    }

    /// Try to get as i32.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            TdsValue::TinyInt(v) => Some(*v as i32),
            TdsValue::SmallInt(v) => Some(*v as i32),
            TdsValue::Int(v) => Some(*v),
            TdsValue::BigInt(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            TdsValue::TinyInt(v) => Some(*v as i64),
            TdsValue::SmallInt(v) => Some(*v as i64),
            TdsValue::Int(v) => Some(*v as i64),
            TdsValue::BigInt(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            TdsValue::Real(v) => Some(*v as f64),
            TdsValue::Float(v) => Some(*v),
            TdsValue::TinyInt(v) => Some(*v as f64),
            TdsValue::SmallInt(v) => Some(*v as f64),
            TdsValue::Int(v) => Some(*v as f64),
            TdsValue::BigInt(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            TdsValue::Bit(v) => Some(*v),
            TdsValue::TinyInt(v) => Some(*v != 0),
            TdsValue::SmallInt(v) => Some(*v != 0),
            TdsValue::Int(v) => Some(*v != 0),
            _ => None,
        }
    }

    /// Try to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            TdsValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            TdsValue::Binary(b) => Some(b),
            TdsValue::Raw(b) => Some(b),
            _ => None,
        }
    }
}

/// Decode a fixed-length integer.
pub fn decode_int(data: &[u8], data_type: u8) -> Result<TdsValue, SybaseWireError> {
    match data_type {
        type_codes::INT1 => {
            if data.is_empty() {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::TinyInt(data[0]))
        }
        type_codes::INT2 => {
            if data.len() < 2 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::SmallInt(i16::from_le_bytes([data[0], data[1]])))
        }
        type_codes::INT4 => {
            if data.len() < 4 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]])))
        }
        type_codes::INT8 => {
            if data.len() < 8 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::BigInt(i64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])))
        }
        _ => Err(SybaseWireError::InvalidTokenType(data_type)),
    }
}

/// Decode a nullable integer.
pub fn decode_intn(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    match data.len() {
        0 => Ok(TdsValue::Null),
        1 => Ok(TdsValue::TinyInt(data[0])),
        2 => Ok(TdsValue::SmallInt(i16::from_le_bytes([data[0], data[1]]))),
        4 => Ok(TdsValue::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]]))),
        8 => Ok(TdsValue::BigInt(i64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]))),
        _ => Err(SybaseWireError::InvalidDataType(type_codes::INTN)),
    }
}

/// Decode a floating-point number.
pub fn decode_float(data: &[u8], data_type: u8) -> Result<TdsValue, SybaseWireError> {
    match data_type {
        type_codes::FLT4 => {
            if data.len() < 4 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::Real(f32::from_le_bytes([data[0], data[1], data[2], data[3]])))
        }
        type_codes::FLT8 => {
            if data.len() < 8 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::Float(f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])))
        }
        _ => Err(SybaseWireError::InvalidTokenType(data_type)),
    }
}

/// Decode a nullable float.
pub fn decode_fltn(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    match data.len() {
        0 => Ok(TdsValue::Null),
        4 => Ok(TdsValue::Real(f32::from_le_bytes([data[0], data[1], data[2], data[3]]))),
        8 => Ok(TdsValue::Float(f64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]))),
        _ => Err(SybaseWireError::InvalidDataType(type_codes::FLTN)),
    }
}

/// Decode a bit value.
pub fn decode_bit(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    if data.is_empty() {
        return Ok(TdsValue::Null);
    }
    Ok(TdsValue::Bit(data[0] != 0))
}

/// Decode a money value.
pub fn decode_money(data: &[u8], data_type: u8) -> Result<TdsValue, SybaseWireError> {
    match data_type {
        type_codes::MONEY4 => {
            if data.len() < 4 {
                return Ok(TdsValue::Null);
            }
            Ok(TdsValue::SmallMoney(i32::from_le_bytes([data[0], data[1], data[2], data[3]])))
        }
        type_codes::MONEY => {
            if data.len() < 8 {
                return Ok(TdsValue::Null);
            }
            // MONEY is stored as high 4 bytes + low 4 bytes
            let high = i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i64;
            let low = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as i64;
            Ok(TdsValue::Money((high << 32) | low))
        }
        _ => Err(SybaseWireError::InvalidTokenType(data_type)),
    }
}

/// Decode a nullable money value.
pub fn decode_moneyn(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    match data.len() {
        0 => Ok(TdsValue::Null),
        4 => Ok(TdsValue::SmallMoney(i32::from_le_bytes([data[0], data[1], data[2], data[3]]))),
        8 => {
            let high = i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i64;
            let low = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as i64;
            Ok(TdsValue::Money((high << 32) | low))
        }
        _ => Err(SybaseWireError::InvalidDataType(type_codes::MONEYN)),
    }
}

/// Decode a datetime value.
pub fn decode_datetime(data: &[u8], data_type: u8) -> Result<TdsValue, SybaseWireError> {
    match data_type {
        type_codes::DATETIME4 => {
            if data.len() < 4 {
                return Ok(TdsValue::Null);
            }
            let days = u16::from_le_bytes([data[0], data[1]]);
            let minutes = u16::from_le_bytes([data[2], data[3]]);
            Ok(TdsValue::SmallDateTime { days, minutes })
        }
        type_codes::DATETIME => {
            if data.len() < 8 {
                return Ok(TdsValue::Null);
            }
            let days = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            let time_ticks = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
            Ok(TdsValue::DateTime { days, time_ticks })
        }
        _ => Err(SybaseWireError::InvalidTokenType(data_type)),
    }
}

/// Decode a nullable datetime value.
pub fn decode_datetimn(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    match data.len() {
        0 => Ok(TdsValue::Null),
        4 => {
            let days = u16::from_le_bytes([data[0], data[1]]);
            let minutes = u16::from_le_bytes([data[2], data[3]]);
            Ok(TdsValue::SmallDateTime { days, minutes })
        }
        8 => {
            let days = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            let time_ticks = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
            Ok(TdsValue::DateTime { days, time_ticks })
        }
        _ => Err(SybaseWireError::InvalidDataType(type_codes::DATETIMN)),
    }
}

/// Decode a string value.
pub fn decode_string(data: &[u8]) -> TdsValue {
    if data.is_empty() {
        return TdsValue::Null;
    }
    TdsValue::String(String::from_utf8_lossy(data).into_owned())
}

/// Decode a binary value.
pub fn decode_binary(data: &[u8]) -> TdsValue {
    if data.is_empty() {
        return TdsValue::Null;
    }
    TdsValue::Binary(data.to_vec())
}

/// Decode a numeric/decimal value.
pub fn decode_numeric(data: &[u8], precision: u8, scale: u8) -> Result<TdsValue, SybaseWireError> {
    if data.is_empty() {
        return Ok(TdsValue::Null);
    }

    // First byte is sign (0 = negative, 1 = positive)
    let sign = data[0];
    let bytes = &data[1..];

    // Value is stored as little-endian integer
    let mut value: i128 = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        value |= (byte as i128) << (i * 8);
    }

    if sign == 0 {
        value = -value;
    }

    Ok(TdsValue::Numeric { precision, scale, value })
}

/// Decode a GUID value.
pub fn decode_guid(data: &[u8]) -> Result<TdsValue, SybaseWireError> {
    if data.len() < 16 {
        return Ok(TdsValue::Null);
    }

    let mut guid = [0u8; 16];
    guid.copy_from_slice(&data[..16]);
    Ok(TdsValue::Guid(guid))
}

/// Decode a value based on data type.
pub fn decode_value(data: &[u8], data_type: u8, precision: u8, scale: u8) -> Result<TdsValue, SybaseWireError> {
    match data_type {
        type_codes::INT1 | type_codes::INT2 | type_codes::INT4 | type_codes::INT8 => decode_int(data, data_type),
        type_codes::INTN => decode_intn(data),
        type_codes::FLT4 | type_codes::FLT8 => decode_float(data, data_type),
        type_codes::FLTN => decode_fltn(data),
        type_codes::BIT | type_codes::BITN => decode_bit(data),
        type_codes::MONEY | type_codes::MONEY4 => decode_money(data, data_type),
        type_codes::MONEYN => decode_moneyn(data),
        type_codes::DATETIME | type_codes::DATETIME4 => decode_datetime(data, data_type),
        type_codes::DATETIMN => decode_datetimn(data),
        type_codes::CHAR | type_codes::VARCHAR | type_codes::LONGCHAR | type_codes::TEXT => Ok(decode_string(data)),
        type_codes::BINARY | type_codes::VARBINARY | type_codes::LONGBINARY | type_codes::IMAGE => Ok(decode_binary(data)),
        type_codes::NUMERIC | type_codes::DECIMAL => decode_numeric(data, precision, scale),
        type_codes::GUID => decode_guid(data),
        _ => Ok(TdsValue::Raw(data.to_vec())),
    }
}

/// Encode an i32 value.
pub fn encode_i32(value: i32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Encode an i64 value.
pub fn encode_i64(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Encode an f32 value.
pub fn encode_f32(value: f32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Encode an f64 value.
pub fn encode_f64(value: f64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Encode a bool value.
pub fn encode_bool(value: bool) -> Vec<u8> {
    vec![if value { 1 } else { 0 }]
}

/// Encode a string value.
pub fn encode_string(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

/// Encode a TdsValue.
pub fn encode_value(value: &TdsValue) -> Vec<u8> {
    match value {
        TdsValue::Null => Vec::new(),
        TdsValue::TinyInt(v) => vec![*v],
        TdsValue::SmallInt(v) => v.to_le_bytes().to_vec(),
        TdsValue::Int(v) => v.to_le_bytes().to_vec(),
        TdsValue::BigInt(v) => v.to_le_bytes().to_vec(),
        TdsValue::Real(v) => v.to_le_bytes().to_vec(),
        TdsValue::Float(v) => v.to_le_bytes().to_vec(),
        TdsValue::Bit(v) => vec![if *v { 1 } else { 0 }],
        TdsValue::SmallMoney(v) => v.to_le_bytes().to_vec(),
        TdsValue::Money(v) => {
            let high = ((*v >> 32) as i32).to_le_bytes();
            let low = (*v as u32).to_le_bytes();
            [high, low].concat()
        }
        TdsValue::SmallDateTime { days, minutes } => [days.to_le_bytes(), minutes.to_le_bytes()].concat(),
        TdsValue::DateTime { days, time_ticks } => [days.to_le_bytes().to_vec(), time_ticks.to_le_bytes().to_vec()].concat(),
        TdsValue::String(s) => s.as_bytes().to_vec(),
        TdsValue::Binary(b) => b.clone(),
        TdsValue::Numeric { value, .. } => {
            let sign = if *value >= 0 { 1u8 } else { 0u8 };
            let abs_value = value.unsigned_abs();
            let mut bytes = vec![sign];
            // Encode as little-endian
            let value_bytes = abs_value.to_le_bytes();
            // Find the last non-zero byte
            let len = value_bytes.iter().rposition(|&b| b != 0).map(|i| i + 1).unwrap_or(1);
            bytes.extend_from_slice(&value_bytes[..len]);
            bytes
        }
        TdsValue::Guid(g) => g.to_vec(),
        TdsValue::Date { days } => days.to_le_bytes()[..3].to_vec(),
        TdsValue::Time { ticks, scale: _ } => ticks.to_le_bytes()[..5].to_vec(),
        TdsValue::DateTime2 { days, ticks, scale: _ } => {
            let mut bytes = ticks.to_le_bytes()[..5].to_vec();
            bytes.extend_from_slice(&days.to_le_bytes()[..3]);
            bytes
        }
        TdsValue::DateTimeOffset { days, ticks, scale: _, offset_minutes } => {
            let mut bytes = ticks.to_le_bytes()[..5].to_vec();
            bytes.extend_from_slice(&days.to_le_bytes()[..3]);
            bytes.extend_from_slice(&offset_minutes.to_le_bytes());
            bytes
        }
        TdsValue::Raw(b) => b.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_int() {
        assert_eq!(decode_int(&[42], type_codes::INT1).unwrap(), TdsValue::TinyInt(42));
        assert_eq!(decode_int(&[0x01, 0x00], type_codes::INT2).unwrap(), TdsValue::SmallInt(1));
        assert_eq!(decode_int(&[0x01, 0x00, 0x00, 0x00], type_codes::INT4).unwrap(), TdsValue::Int(1));
    }

    #[test]
    fn test_decode_intn() {
        assert_eq!(decode_intn(&[]).unwrap(), TdsValue::Null);
        assert_eq!(decode_intn(&[42]).unwrap(), TdsValue::TinyInt(42));
        assert_eq!(decode_intn(&[0xFF, 0xFF]).unwrap(), TdsValue::SmallInt(-1));
    }

    #[test]
    fn test_decode_float() {
        let data = 3.25f32.to_le_bytes();
        match decode_float(&data, type_codes::FLT4).unwrap() {
            TdsValue::Real(v) => assert!((v - 3.25).abs() < 0.001),
            _ => panic!("Expected Real"),
        }
    }

    #[test]
    fn test_decode_string() {
        assert_eq!(decode_string(b"hello"), TdsValue::String("hello".to_string()));
        assert_eq!(decode_string(&[]), TdsValue::Null);
    }

    #[test]
    fn test_decode_bit() {
        assert_eq!(decode_bit(&[0]).unwrap(), TdsValue::Bit(false));
        assert_eq!(decode_bit(&[1]).unwrap(), TdsValue::Bit(true));
        assert_eq!(decode_bit(&[]).unwrap(), TdsValue::Null);
    }

    #[test]
    fn test_tds_value_conversions() {
        let val = TdsValue::Int(42);
        assert_eq!(val.as_i32(), Some(42));
        assert_eq!(val.as_i64(), Some(42));
        assert_eq!(val.as_f64(), Some(42.0));
        assert!(!val.is_null());

        let null = TdsValue::Null;
        assert!(null.is_null());
        assert_eq!(null.as_i32(), None);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = TdsValue::Int(12345);
        let encoded = encode_value(&original);
        let decoded = decode_int(&encoded, type_codes::INT4).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_numeric() {
        // Positive value: sign=1, value=12345
        let data = [1, 0x39, 0x30, 0x00, 0x00]; // sign + 12345 in LE
        let result = decode_numeric(&data, 10, 2).unwrap();
        match result {
            TdsValue::Numeric { precision, scale, value } => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
                assert_eq!(value, 12345);
            }
            _ => panic!("Expected Numeric"),
        }
    }
}
