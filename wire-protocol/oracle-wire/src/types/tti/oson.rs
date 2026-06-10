//! OSON (Oracle Binary JSON) parsing for Oracle 21c+.
//!
//! OSON is Oracle's compact binary representation of JSON data introduced in
//! Oracle Database 21c. It provides more efficient storage and parsing compared
//! to text-based JSON.
//!
//! # Format Overview
//!
//! OSON uses a header followed by encoded values:
//! - Magic bytes: `0xFF 0x4A` (identifies OSON format)
//! - Version byte
//! - Flags byte
//! - Root value (recursively encoded)
//!
//! # Value Encoding
//!
//! Values are prefixed with a type byte:
//! - Scalars: null, boolean, numbers, strings
//! - Containers: objects, arrays
//! - Extended: dates, timestamps, binary data
//!
//! # Example
//!
//! ```rust,ignore
//! use oracle_wire::types::tti::oson::{OsonParser, OsonValue};
//!
//! let binary_data = &[0xFF, 0x4A, 0x01, 0x00, /* ... */];
//! let parser = OsonParser::new(binary_data)?;
//! let value = parser.parse()?;
//! ```

use std::collections::HashMap;
use std::fmt;

/// OSON magic bytes that identify the binary JSON format.
pub const OSON_MAGIC: [u8; 2] = [0xFF, 0x4A];

/// OSON format versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OsonVersion {
    /// Version 1 (Oracle 21c).
    V1,
    /// Version 2 (Oracle 23c with extended types).
    V2,
    /// Unknown version.
    Unknown(u8),
}

impl OsonVersion {
    /// Create from raw version byte.
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x01 => Self::V1,
            0x02 => Self::V2,
            other => Self::Unknown(other),
        }
    }

    /// Convert to raw version byte.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::V1 => 0x01,
            Self::V2 => 0x02,
            Self::Unknown(v) => *v,
        }
    }
}

/// OSON type codes for value encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OsonType {
    /// Null value.
    Null,
    /// Boolean true.
    True,
    /// Boolean false.
    False,
    /// String (UTF-8 encoded).
    String,
    /// Small integer (-128 to 127).
    Int8,
    /// 16-bit integer.
    Int16,
    /// 32-bit integer.
    Int32,
    /// 64-bit integer.
    Int64,
    /// Oracle NUMBER (variable precision).
    OracleNumber,
    /// IEEE 754 float.
    Float,
    /// IEEE 754 double.
    Double,
    /// Binary data.
    Binary,
    /// Date (YYYY-MM-DD).
    Date,
    /// Timestamp.
    Timestamp,
    /// Timestamp with timezone.
    TimestampTz,
    /// Interval year to month.
    IntervalYm,
    /// Interval day to second.
    IntervalDs,
    /// JSON object.
    Object,
    /// JSON array.
    Array,
    /// Extended type (23c+).
    Extended,
    /// Unknown type.
    Unknown(u8),
}

impl OsonType {
    /// Create from raw type byte.
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x00 => Self::Null,
            0x01 => Self::True,
            0x02 => Self::False,
            0x03 => Self::String,
            0x04 => Self::Int8,
            0x05 => Self::Int16,
            0x06 => Self::Int32,
            0x07 => Self::Int64,
            0x08 => Self::OracleNumber,
            0x09 => Self::Float,
            0x0A => Self::Double,
            0x0B => Self::Binary,
            0x0C => Self::Date,
            0x0D => Self::Timestamp,
            0x0E => Self::TimestampTz,
            0x0F => Self::IntervalYm,
            0x10 => Self::IntervalDs,
            0x20 => Self::Object,
            0x21 => Self::Array,
            0xFE => Self::Extended,
            other => Self::Unknown(other),
        }
    }

    /// Convert to raw type byte.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Null => 0x00,
            Self::True => 0x01,
            Self::False => 0x02,
            Self::String => 0x03,
            Self::Int8 => 0x04,
            Self::Int16 => 0x05,
            Self::Int32 => 0x06,
            Self::Int64 => 0x07,
            Self::OracleNumber => 0x08,
            Self::Float => 0x09,
            Self::Double => 0x0A,
            Self::Binary => 0x0B,
            Self::Date => 0x0C,
            Self::Timestamp => 0x0D,
            Self::TimestampTz => 0x0E,
            Self::IntervalYm => 0x0F,
            Self::IntervalDs => 0x10,
            Self::Object => 0x20,
            Self::Array => 0x21,
            Self::Extended => 0xFE,
            Self::Unknown(v) => *v,
        }
    }

    /// Check if this is a scalar type.
    pub fn is_scalar(&self) -> bool {
        !matches!(self, Self::Object | Self::Array)
    }

    /// Check if this is a container type.
    pub fn is_container(&self) -> bool {
        matches!(self, Self::Object | Self::Array)
    }

    /// Check if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::OracleNumber | Self::Float | Self::Double
        )
    }

    /// Check if this is a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(self, Self::Date | Self::Timestamp | Self::TimestampTz | Self::IntervalYm | Self::IntervalDs)
    }
}

/// OSON parsing error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OsonError {
    /// Invalid magic bytes.
    InvalidMagic([u8; 2]),
    /// Unsupported version.
    UnsupportedVersion(u8),
    /// Unexpected end of data.
    UnexpectedEof { expected: usize, available: usize },
    /// Invalid type byte.
    InvalidType(u8),
    /// Invalid UTF-8 string.
    InvalidUtf8,
    /// Invalid number encoding.
    InvalidNumber,
    /// Nesting depth exceeded.
    NestingTooDeep { depth: usize, max: usize },
    /// Container size exceeded.
    ContainerTooLarge { size: usize, max: usize },
    /// Invalid date/time value.
    InvalidDateTime,
    /// Corrupted data structure.
    CorruptedData(String),
}

impl std::error::Error for OsonError {}

impl fmt::Display for OsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic(bytes) => {
                write!(f, "invalid OSON magic: {:02X} {:02X}", bytes[0], bytes[1])
            }
            Self::UnsupportedVersion(v) => write!(f, "unsupported OSON version: {}", v),
            Self::UnexpectedEof { expected, available } => {
                write!(f, "unexpected EOF: need {} bytes, have {}", expected, available)
            }
            Self::InvalidType(t) => write!(f, "invalid OSON type: 0x{:02X}", t),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 string"),
            Self::InvalidNumber => write!(f, "invalid number encoding"),
            Self::NestingTooDeep { depth, max } => {
                write!(f, "nesting too deep: {} exceeds max {}", depth, max)
            }
            Self::ContainerTooLarge { size, max } => {
                write!(f, "container too large: {} exceeds max {}", size, max)
            }
            Self::InvalidDateTime => write!(f, "invalid date/time value"),
            Self::CorruptedData(msg) => write!(f, "corrupted data: {}", msg),
        }
    }
}

/// Parsed OSON value.
#[derive(Clone, Debug, PartialEq)]
pub enum OsonValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Integer value (fits in i64).
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// Oracle NUMBER as string (preserves precision).
    Number(String),
    /// String value.
    String(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Date as string (YYYY-MM-DD).
    Date(String),
    /// Timestamp as string.
    Timestamp(String),
    /// Object (key-value pairs).
    Object(HashMap<String, OsonValue>),
    /// Array of values.
    Array(Vec<OsonValue>),
}

impl OsonValue {
    /// Check if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get as boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(n) => Some(*n),
            Self::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            Self::Number(s) => Some(s),
            Self::Date(s) => Some(s),
            Self::Timestamp(s) => Some(s),
            _ => None,
        }
    }

    /// Get as binary data.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            _ => None,
        }
    }

    /// Get as object.
    pub fn as_object(&self) -> Option<&HashMap<String, OsonValue>> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Get as array.
    pub fn as_array(&self) -> Option<&[OsonValue]> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Get a value from an object by key.
    pub fn get(&self, key: &str) -> Option<&OsonValue> {
        self.as_object()?.get(key)
    }

    /// Get a value from an array by index.
    pub fn get_index(&self, index: usize) -> Option<&OsonValue> {
        self.as_array()?.get(index)
    }

    /// Get the type name of this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "boolean",
            Self::Int(_) => "integer",
            Self::Float(_) => "float",
            Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::Binary(_) => "binary",
            Self::Date(_) => "date",
            Self::Timestamp(_) => "timestamp",
            Self::Object(_) => "object",
            Self::Array(_) => "array",
        }
    }
}

/// OSON header information.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OsonHeader {
    /// OSON format version.
    pub version: OsonVersion,
    /// Header flags.
    pub flags: u8,
}

impl OsonHeader {
    /// Check if the header indicates compressed content.
    pub fn is_compressed(&self) -> bool {
        self.flags & 0x01 != 0
    }

    /// Check if the header indicates extended types.
    pub fn has_extended_types(&self) -> bool {
        self.flags & 0x02 != 0
    }

    /// Check if field names are interned (shared string table).
    pub fn has_interned_names(&self) -> bool {
        self.flags & 0x04 != 0
    }
}

/// Maximum nesting depth for OSON parsing.
pub const MAX_NESTING_DEPTH: usize = 128;

/// Maximum container size (elements in array or object).
pub const MAX_CONTAINER_SIZE: usize = 1_000_000;

/// OSON parser.
pub struct OsonParser<'a> {
    data: &'a [u8],
    pos: usize,
    header: OsonHeader,
    /// String table for interned field names.
    string_table: Vec<String>,
    /// Current nesting depth.
    depth: usize,
}

impl<'a> OsonParser<'a> {
    /// Create a new OSON parser.
    pub fn new(data: &'a [u8]) -> Result<Self, OsonError> {
        if data.len() < 4 {
            return Err(OsonError::UnexpectedEof { expected: 4, available: data.len() });
        }

        // Check magic bytes
        if data[0] != OSON_MAGIC[0] || data[1] != OSON_MAGIC[1] {
            return Err(OsonError::InvalidMagic([data[0], data[1]]));
        }

        let version = OsonVersion::from_u8(data[2]);
        let flags = data[3];

        Ok(Self {
            data,
            pos: 4,
            header: OsonHeader { version, flags },
            string_table: Vec::new(),
            depth: 0,
        })
    }

    /// Get the parsed header.
    pub fn header(&self) -> OsonHeader {
        self.header
    }

    /// Parse the OSON data into a value.
    pub fn parse(&mut self) -> Result<OsonValue, OsonError> {
        // Parse string table if present
        if self.header.has_interned_names() {
            self.parse_string_table()?;
        }

        self.parse_value()
    }

    /// Parse the string table.
    fn parse_string_table(&mut self) -> Result<(), OsonError> {
        let count = self.read_varint()? as usize;
        if count > MAX_CONTAINER_SIZE {
            return Err(OsonError::ContainerTooLarge { size: count, max: MAX_CONTAINER_SIZE });
        }

        self.string_table.reserve(count);
        for _ in 0..count {
            let s = self.read_string()?;
            self.string_table.push(s);
        }

        Ok(())
    }

    /// Parse a single value.
    fn parse_value(&mut self) -> Result<OsonValue, OsonError> {
        let type_byte = self.read_u8()?;
        let oson_type = OsonType::from_u8(type_byte);

        match oson_type {
            OsonType::Null => Ok(OsonValue::Null),
            OsonType::True => Ok(OsonValue::Bool(true)),
            OsonType::False => Ok(OsonValue::Bool(false)),
            OsonType::String => {
                let s = self.read_string()?;
                Ok(OsonValue::String(s))
            }
            OsonType::Int8 => {
                let v = self.read_u8()? as i8 as i64;
                Ok(OsonValue::Int(v))
            }
            OsonType::Int16 => {
                let v = self.read_i16()?;
                Ok(OsonValue::Int(v as i64))
            }
            OsonType::Int32 => {
                let v = self.read_i32()?;
                Ok(OsonValue::Int(v as i64))
            }
            OsonType::Int64 => {
                let v = self.read_i64()?;
                Ok(OsonValue::Int(v))
            }
            OsonType::Float => {
                let v = self.read_f32()?;
                Ok(OsonValue::Float(v as f64))
            }
            OsonType::Double => {
                let v = self.read_f64()?;
                Ok(OsonValue::Float(v))
            }
            OsonType::OracleNumber => {
                let s = self.read_oracle_number()?;
                Ok(OsonValue::Number(s))
            }
            OsonType::Binary => {
                let b = self.read_binary()?;
                Ok(OsonValue::Binary(b))
            }
            OsonType::Date => {
                let s = self.read_date()?;
                Ok(OsonValue::Date(s))
            }
            OsonType::Timestamp | OsonType::TimestampTz => {
                let s = self.read_timestamp()?;
                Ok(OsonValue::Timestamp(s))
            }
            OsonType::IntervalYm | OsonType::IntervalDs => {
                // Represent intervals as strings
                let s = self.read_interval()?;
                Ok(OsonValue::String(s))
            }
            OsonType::Object => self.parse_object(),
            OsonType::Array => self.parse_array(),
            OsonType::Extended => self.parse_extended(),
            OsonType::Unknown(t) => Err(OsonError::InvalidType(t)),
        }
    }

    /// Parse an object.
    fn parse_object(&mut self) -> Result<OsonValue, OsonError> {
        self.depth += 1;
        if self.depth > MAX_NESTING_DEPTH {
            return Err(OsonError::NestingTooDeep { depth: self.depth, max: MAX_NESTING_DEPTH });
        }

        let count = self.read_varint()? as usize;
        if count > MAX_CONTAINER_SIZE {
            return Err(OsonError::ContainerTooLarge { size: count, max: MAX_CONTAINER_SIZE });
        }

        let mut map = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = if self.header.has_interned_names() {
                let idx = self.read_varint()? as usize;
                self.string_table.get(idx).cloned().ok_or_else(|| OsonError::CorruptedData(format!("invalid string index: {}", idx)))?
            } else {
                self.read_string()?
            };
            let value = self.parse_value()?;
            map.insert(key, value);
        }

        self.depth -= 1;
        Ok(OsonValue::Object(map))
    }

    /// Parse an array.
    fn parse_array(&mut self) -> Result<OsonValue, OsonError> {
        self.depth += 1;
        if self.depth > MAX_NESTING_DEPTH {
            return Err(OsonError::NestingTooDeep { depth: self.depth, max: MAX_NESTING_DEPTH });
        }

        let count = self.read_varint()? as usize;
        if count > MAX_CONTAINER_SIZE {
            return Err(OsonError::ContainerTooLarge { size: count, max: MAX_CONTAINER_SIZE });
        }

        let mut arr = Vec::with_capacity(count);
        for _ in 0..count {
            arr.push(self.parse_value()?);
        }

        self.depth -= 1;
        Ok(OsonValue::Array(arr))
    }

    /// Parse an extended type (23c+).
    fn parse_extended(&mut self) -> Result<OsonValue, OsonError> {
        let subtype = self.read_u8()?;
        match subtype {
            // Vector type (23c AI vectors)
            0x01 => {
                let dimensions = self.read_varint()? as usize;
                let mut values = Vec::with_capacity(dimensions);
                for _ in 0..dimensions {
                    values.push(OsonValue::Float(self.read_f64()?));
                }
                Ok(OsonValue::Array(values))
            }
            // Boolean array (optimized)
            0x02 => {
                let count = self.read_varint()? as usize;
                let byte_count = count.div_ceil(8);
                let bytes = self.read_bytes(byte_count)?;
                let mut values = Vec::with_capacity(count);
                for i in 0..count {
                    let bit = (bytes[i / 8] >> (i % 8)) & 1;
                    values.push(OsonValue::Bool(bit != 0));
                }
                Ok(OsonValue::Array(values))
            }
            // Other extended types - return as binary for now
            _ => {
                let len = self.read_varint()? as usize;
                let data = self.read_bytes(len)?;
                Ok(OsonValue::Binary(data))
            }
        }
    }

    // ===== Low-level reading methods =====

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, OsonError> {
        if self.pos >= self.data.len() {
            return Err(OsonError::UnexpectedEof { expected: 1, available: 0 });
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>, OsonError> {
        if self.remaining() < n {
            return Err(OsonError::UnexpectedEof { expected: n, available: self.remaining() });
        }
        let bytes = self.data[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(bytes)
    }

    fn read_i16(&mut self) -> Result<i16, OsonError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> Result<i32, OsonError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self) -> Result<i64, OsonError> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
    }

    fn read_f32(&mut self) -> Result<f32, OsonError> {
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_f64(&mut self) -> Result<f64, OsonError> {
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
    }

    /// Read a variable-length integer (LEB128-style).
    fn read_varint(&mut self) -> Result<u64, OsonError> {
        let mut result: u64 = 0;
        let mut shift = 0;

        loop {
            let byte = self.read_u8()?;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(OsonError::InvalidNumber);
            }
        }

        Ok(result)
    }

    /// Read a length-prefixed string.
    fn read_string(&mut self) -> Result<String, OsonError> {
        let len = self.read_varint()? as usize;
        if len == 0 {
            return Ok(String::new());
        }

        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes).map_err(|_| OsonError::InvalidUtf8)
    }

    /// Read length-prefixed binary data.
    fn read_binary(&mut self) -> Result<Vec<u8>, OsonError> {
        let len = self.read_varint()? as usize;
        self.read_bytes(len)
    }

    /// Read an Oracle NUMBER (variable precision decimal).
    fn read_oracle_number(&mut self) -> Result<String, OsonError> {
        let len = self.read_u8()? as usize;
        if len == 0 {
            return Ok("0".to_string());
        }

        let bytes = self.read_bytes(len)?;

        // Oracle NUMBER format: exponent byte + mantissa bytes
        // The first byte is the exponent (biased by 64)
        // For negative numbers, bytes are complemented
        if bytes.is_empty() {
            return Ok("0".to_string());
        }

        let exp_byte = bytes[0];
        let is_negative = exp_byte < 128;

        // Decode exponent
        let exponent = if is_negative {
            -((!exp_byte & 0x7F) as i32) - 65
        } else {
            (exp_byte & 0x7F) as i32 - 65
        };

        // Decode mantissa digits
        let mut digits = String::new();
        for &b in &bytes[1..] {
            let d = if is_negative { 101 - b } else { b - 1 };
            if d < 100 {
                if digits.is_empty() {
                    digits.push_str(&format!("{}", d));
                } else {
                    digits.push_str(&format!("{:02}", d));
                }
            }
        }

        if digits.is_empty() {
            return Ok("0".to_string());
        }

        // Construct the decimal string
        let mut result = String::new();
        if is_negative {
            result.push('-');
        }

        // Apply exponent to position decimal point
        let num_pairs = digits.len();
        let decimal_pos = (exponent + 1) as usize * 2;

        if decimal_pos >= num_pairs {
            result.push_str(&digits);
            for _ in 0..(decimal_pos - num_pairs) {
                result.push('0');
            }
        } else if decimal_pos == 0 {
            result.push_str("0.");
            result.push_str(&digits);
        } else {
            result.push_str(&digits[..decimal_pos.min(digits.len())]);
            if decimal_pos < digits.len() {
                result.push('.');
                result.push_str(&digits[decimal_pos..]);
            }
        }

        Ok(result)
    }

    /// Read a date value.
    fn read_date(&mut self) -> Result<String, OsonError> {
        // Oracle date: 7 bytes (century, year, month, day, hour, minute, second)
        let bytes = self.read_bytes(7)?;
        let century = bytes[0] as i32 - 100;
        let year = bytes[1] as i32 - 100;
        let month = bytes[2];
        let day = bytes[3];

        let full_year = century * 100 + year;
        Ok(format!("{:04}-{:02}-{:02}", full_year, month, day))
    }

    /// Read a timestamp value.
    fn read_timestamp(&mut self) -> Result<String, OsonError> {
        // Timestamp: 11 bytes (date + fractional seconds)
        let bytes = self.read_bytes(11)?;
        let century = bytes[0] as i32 - 100;
        let year = bytes[1] as i32 - 100;
        let month = bytes[2];
        let day = bytes[3];
        let hour = bytes[4] - 1;
        let minute = bytes[5] - 1;
        let second = bytes[6] - 1;

        // Fractional seconds (4 bytes, big-endian)
        let nanos = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);

        let full_year = century * 100 + year;
        Ok(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}",
            full_year, month, day, hour, minute, second, nanos
        ))
    }

    /// Read an interval value.
    fn read_interval(&mut self) -> Result<String, OsonError> {
        // Read interval length first
        let len = self.read_u8()? as usize;
        let bytes = self.read_bytes(len)?;

        // Simple representation as hex for now
        // Full interval parsing would depend on interval type
        Ok(format!("INTERVAL '{}'", bytes.iter().map(|b| format!("{:02X}", b)).collect::<String>()))
    }
}

/// Check if data appears to be OSON format.
pub fn is_oson(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == OSON_MAGIC[0] && data[1] == OSON_MAGIC[1]
}

/// Parse OSON data into a value.
pub fn parse_oson(data: &[u8]) -> Result<OsonValue, OsonError> {
    let mut parser = OsonParser::new(data)?;
    parser.parse()
}

/// Builder for creating OSON binary data.
#[derive(Clone, Debug)]
pub struct OsonBuilder {
    data: Vec<u8>,
    version: OsonVersion,
    use_interned_names: bool,
    string_table: Vec<String>,
    string_indices: HashMap<String, usize>,
}

impl Default for OsonBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OsonBuilder {
    /// Create a new OSON builder.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            version: OsonVersion::V1,
            use_interned_names: false,
            string_table: Vec::new(),
            string_indices: HashMap::new(),
        }
    }

    /// Set the OSON version.
    pub fn version(mut self, version: OsonVersion) -> Self {
        self.version = version;
        self
    }

    /// Enable interned field names.
    pub fn with_interned_names(mut self) -> Self {
        self.use_interned_names = true;
        self
    }

    /// Build OSON from a value.
    pub fn build(mut self, value: &OsonValue) -> Vec<u8> {
        // Collect field names if interning
        if self.use_interned_names {
            self.collect_field_names(value);
        }

        // Write header
        self.data.push(OSON_MAGIC[0]);
        self.data.push(OSON_MAGIC[1]);
        self.data.push(self.version.as_u8());

        let mut flags = 0u8;
        if self.use_interned_names && !self.string_table.is_empty() {
            flags |= 0x04;
        }
        self.data.push(flags);

        // Write string table if interning
        if self.use_interned_names && !self.string_table.is_empty() {
            self.write_varint(self.string_table.len() as u64);
            for s in &self.string_table.clone() {
                self.write_string(s);
            }
        }

        // Write value
        self.write_value(value);

        self.data
    }

    fn collect_field_names(&mut self, value: &OsonValue) {
        match value {
            OsonValue::Object(map) => {
                for (key, val) in map {
                    if !self.string_indices.contains_key(key) {
                        self.string_indices.insert(key.clone(), self.string_table.len());
                        self.string_table.push(key.clone());
                    }
                    self.collect_field_names(val);
                }
            }
            OsonValue::Array(arr) => {
                for val in arr {
                    self.collect_field_names(val);
                }
            }
            _ => {}
        }
    }

    fn write_value(&mut self, value: &OsonValue) {
        match value {
            OsonValue::Null => self.data.push(OsonType::Null.as_u8()),
            OsonValue::Bool(true) => self.data.push(OsonType::True.as_u8()),
            OsonValue::Bool(false) => self.data.push(OsonType::False.as_u8()),
            OsonValue::Int(n) => self.write_int(*n),
            OsonValue::Float(f) => {
                self.data.push(OsonType::Double.as_u8());
                self.data.extend_from_slice(&f.to_be_bytes());
            }
            OsonValue::Number(s) => {
                // Write as string for now (full NUMBER encoding is complex)
                self.data.push(OsonType::String.as_u8());
                self.write_string(s);
            }
            OsonValue::String(s) => {
                self.data.push(OsonType::String.as_u8());
                self.write_string(s);
            }
            OsonValue::Binary(b) => {
                self.data.push(OsonType::Binary.as_u8());
                self.write_varint(b.len() as u64);
                self.data.extend_from_slice(b);
            }
            OsonValue::Date(s) => {
                self.data.push(OsonType::String.as_u8());
                self.write_string(s);
            }
            OsonValue::Timestamp(s) => {
                self.data.push(OsonType::String.as_u8());
                self.write_string(s);
            }
            OsonValue::Object(map) => {
                self.data.push(OsonType::Object.as_u8());
                self.write_varint(map.len() as u64);
                for (key, val) in map {
                    if self.use_interned_names {
                        if let Some(&idx) = self.string_indices.get(key) {
                            self.write_varint(idx as u64);
                        } else {
                            self.write_string(key);
                        }
                    } else {
                        self.write_string(key);
                    }
                    self.write_value(val);
                }
            }
            OsonValue::Array(arr) => {
                self.data.push(OsonType::Array.as_u8());
                self.write_varint(arr.len() as u64);
                for val in arr {
                    self.write_value(val);
                }
            }
        }
    }

    fn write_int(&mut self, n: i64) {
        if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
            self.data.push(OsonType::Int8.as_u8());
            self.data.push(n as i8 as u8);
        } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
            self.data.push(OsonType::Int16.as_u8());
            self.data.extend_from_slice(&(n as i16).to_be_bytes());
        } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
            self.data.push(OsonType::Int32.as_u8());
            self.data.extend_from_slice(&(n as i32).to_be_bytes());
        } else {
            self.data.push(OsonType::Int64.as_u8());
            self.data.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn write_varint(&mut self, mut n: u64) {
        loop {
            let mut byte = (n & 0x7F) as u8;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            self.data.push(byte);
            if n == 0 {
                break;
            }
        }
    }

    fn write_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.write_varint(bytes.len() as u64);
        self.data.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_oson() {
        assert!(is_oson(&[0xFF, 0x4A, 0x01, 0x00]));
        assert!(!is_oson(&[0xFF, 0x00]));
        assert!(!is_oson(&[0x00, 0x4A]));
        assert!(!is_oson(&[]));
    }

    #[test]
    fn test_parse_null() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x00]; // magic, version, flags, null
        let result = parse_oson(&data).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_parse_bool_true() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x01]; // true
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_bool(), Some(true));
    }

    #[test]
    fn test_parse_bool_false() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x02]; // false
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_bool(), Some(false));
    }

    #[test]
    fn test_parse_int8() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x04, 42]; // int8: 42
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_i64(), Some(42));
    }

    #[test]
    fn test_parse_int8_negative() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x04, 0xFE]; // int8: -2
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_i64(), Some(-2));
    }

    #[test]
    fn test_parse_int32() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00]; // int32: 65536
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_i64(), Some(65536));
    }

    #[test]
    fn test_parse_double() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, 0x0A, // double
            0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18, // π
        ];
        let result = parse_oson(&data).unwrap();
        let f = result.as_f64().unwrap();
        assert!((f - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn test_parse_string() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, 0x03, // string type
            0x05, // length: 5
            b'h', b'e', b'l', b'l', b'o',
        ];
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_str(), Some("hello"));
    }

    #[test]
    fn test_parse_empty_string() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x03, 0x00]; // empty string
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_str(), Some(""));
    }

    #[test]
    fn test_parse_binary() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, 0x0B, // binary type
            0x03, // length: 3
            0xDE, 0xAD, 0xBE,
        ];
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_bytes(), Some(&[0xDE, 0xAD, 0xBE][..]));
    }

    #[test]
    fn test_parse_empty_array() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x21, 0x00]; // array, count=0
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_array(), Some(&[][..]));
    }

    #[test]
    fn test_parse_array() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, // header
            0x21, // array
            0x03, // count: 3
            0x04, 0x01, // int8: 1
            0x04, 0x02, // int8: 2
            0x04, 0x03, // int8: 3
        ];
        let result = parse_oson(&data).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64(), Some(1));
        assert_eq!(arr[1].as_i64(), Some(2));
        assert_eq!(arr[2].as_i64(), Some(3));
    }

    #[test]
    fn test_parse_empty_object() {
        let data = [0xFF, 0x4A, 0x01, 0x00, 0x20, 0x00]; // object, count=0
        let result = parse_oson(&data).unwrap();
        assert!(result.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_parse_object() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, // header
            0x20, // object
            0x02, // count: 2
            0x01, b'a', // key: "a"
            0x04, 0x01, // value: int8 1
            0x01, b'b', // key: "b"
            0x04, 0x02, // value: int8 2
        ];
        let result = parse_oson(&data).unwrap();
        let obj = result.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("a").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(obj.get("b").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn test_parse_nested() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, // header
            0x20, // object
            0x01, // count: 1
            0x04, b'n', b'e', b's', b't', // key: "nest"
            0x21, // array
            0x02, // count: 2
            0x01, // true
            0x02, // false
        ];
        let result = parse_oson(&data).unwrap();
        let nested = result.get("nest").unwrap();
        let arr = nested.as_array().unwrap();
        assert_eq!(arr[0].as_bool(), Some(true));
        assert_eq!(arr[1].as_bool(), Some(false));
    }

    #[test]
    fn test_invalid_magic() {
        let data = [0x00, 0x00, 0x01, 0x00];
        let err = parse_oson(&data).unwrap_err();
        assert!(matches!(err, OsonError::InvalidMagic(_)));
    }

    #[test]
    fn test_unexpected_eof() {
        let data = [0xFF, 0x4A]; // incomplete header
        let err = parse_oson(&data).unwrap_err();
        assert!(matches!(err, OsonError::UnexpectedEof { .. }));
    }

    #[test]
    fn test_invalid_utf8() {
        let data = [
            0xFF, 0x4A, 0x01, 0x00, 0x03, // string
            0x02, // length: 2
            0xFF, 0xFE, // invalid UTF-8
        ];
        let err = parse_oson(&data).unwrap_err();
        assert!(matches!(err, OsonError::InvalidUtf8));
    }

    #[test]
    fn test_builder_null() {
        let data = OsonBuilder::new().build(&OsonValue::Null);
        let result = parse_oson(&data).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_builder_bool() {
        let data = OsonBuilder::new().build(&OsonValue::Bool(true));
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_bool(), Some(true));
    }

    #[test]
    fn test_builder_int() {
        let data = OsonBuilder::new().build(&OsonValue::Int(12345));
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_i64(), Some(12345));
    }

    #[test]
    fn test_builder_string() {
        let data = OsonBuilder::new().build(&OsonValue::String("test".to_string()));
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_str(), Some("test"));
    }

    #[test]
    fn test_builder_array() {
        let arr = OsonValue::Array(vec![OsonValue::Int(1), OsonValue::Int(2), OsonValue::Int(3)]);
        let data = OsonBuilder::new().build(&arr);
        let result = parse_oson(&data).unwrap();
        let parsed = result.as_array().unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn test_builder_object() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), OsonValue::String("value".to_string()));
        let obj = OsonValue::Object(map);

        let data = OsonBuilder::new().build(&obj);
        let result = parse_oson(&data).unwrap();
        let parsed = result.as_object().unwrap();
        assert_eq!(parsed.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_roundtrip_complex() {
        let mut inner = HashMap::new();
        inner.insert("nested".to_string(), OsonValue::Bool(true));

        let mut map = HashMap::new();
        map.insert("string".to_string(), OsonValue::String("hello".to_string()));
        map.insert("number".to_string(), OsonValue::Int(-999));
        map.insert("array".to_string(), OsonValue::Array(vec![OsonValue::Null, OsonValue::Float(3.25)]));
        map.insert("object".to_string(), OsonValue::Object(inner));

        let original = OsonValue::Object(map);
        let data = OsonBuilder::new().build(&original);
        let parsed = parse_oson(&data).unwrap();

        assert_eq!(parsed.get("string").and_then(|v| v.as_str()), Some("hello"));
        assert_eq!(parsed.get("number").and_then(|v| v.as_i64()), Some(-999));
        assert!(parsed.get("array").and_then(|v| v.as_array()).is_some());
        assert!(parsed.get("object").and_then(|v| v.as_object()).is_some());
    }

    #[test]
    fn test_oson_type_classification() {
        assert!(OsonType::Null.is_scalar());
        assert!(OsonType::String.is_scalar());
        assert!(!OsonType::Object.is_scalar());
        assert!(!OsonType::Array.is_scalar());

        assert!(OsonType::Object.is_container());
        assert!(OsonType::Array.is_container());
        assert!(!OsonType::String.is_container());

        assert!(OsonType::Int32.is_numeric());
        assert!(OsonType::Double.is_numeric());
        assert!(!OsonType::String.is_numeric());

        assert!(OsonType::Date.is_temporal());
        assert!(OsonType::Timestamp.is_temporal());
        assert!(!OsonType::String.is_temporal());
    }

    #[test]
    fn test_oson_value_type_name() {
        assert_eq!(OsonValue::Null.type_name(), "null");
        assert_eq!(OsonValue::Bool(true).type_name(), "boolean");
        assert_eq!(OsonValue::Int(0).type_name(), "integer");
        assert_eq!(OsonValue::Float(0.0).type_name(), "float");
        assert_eq!(OsonValue::String(String::new()).type_name(), "string");
        assert_eq!(OsonValue::Binary(vec![]).type_name(), "binary");
        assert_eq!(OsonValue::Array(vec![]).type_name(), "array");
        assert_eq!(OsonValue::Object(HashMap::new()).type_name(), "object");
    }

    #[test]
    fn test_oson_header_flags() {
        let header = OsonHeader {
            version: OsonVersion::V1,
            flags: 0x07, // all flags set
        };
        assert!(header.is_compressed());
        assert!(header.has_extended_types());
        assert!(header.has_interned_names());

        let header2 = OsonHeader { version: OsonVersion::V1, flags: 0x00 };
        assert!(!header2.is_compressed());
        assert!(!header2.has_extended_types());
        assert!(!header2.has_interned_names());
    }

    #[test]
    fn test_varint_encoding() {
        // Test varint through builder
        let arr = OsonValue::Array((0..200).map(OsonValue::Int).collect());
        let data = OsonBuilder::new().build(&arr);
        let result = parse_oson(&data).unwrap();
        assert_eq!(result.as_array().unwrap().len(), 200);
    }
}
