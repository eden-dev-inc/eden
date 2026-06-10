//! MySQL query attributes (MySQL 8.0.25+).
//!
//! Query attributes allow sending key-value metadata with queries when
//! the CLIENT_QUERY_ATTRIBUTES capability is negotiated.

use crate::error::column_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::MysqlParseError;
use crate::write::{write_lenenc_int, write_lenenc_string};
use std::io::{self, Write};
use wire_stream::WireReadSync;

/// A query attribute (key-value pair).
#[derive(Clone, Debug, PartialEq)]
pub struct QueryAttribute {
    /// Attribute name.
    pub name: String,
    /// Attribute value.
    pub value: QueryAttributeValue,
}

/// Query attribute value types.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryAttributeValue {
    /// NULL value.
    Null,
    /// String value.
    String(String),
    /// Integer value.
    Int(i64),
    /// Unsigned integer value.
    UInt(u64),
    /// Double value.
    Double(f64),
    /// Binary data.
    Bytes(Vec<u8>),
}

impl QueryAttribute {
    /// Create a new query attribute.
    pub fn new(name: impl Into<String>, value: QueryAttributeValue) -> Self {
        Self { name: name.into(), value }
    }

    /// Create a string attribute.
    pub fn string(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, QueryAttributeValue::String(value.into()))
    }

    /// Create an integer attribute.
    pub fn int(name: impl Into<String>, value: i64) -> Self {
        Self::new(name, QueryAttributeValue::Int(value))
    }

    /// Create an unsigned integer attribute.
    pub fn uint(name: impl Into<String>, value: u64) -> Self {
        Self::new(name, QueryAttributeValue::UInt(value))
    }

    /// Create a double attribute.
    pub fn double(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, QueryAttributeValue::Double(value))
    }

    /// Create a bytes attribute.
    pub fn bytes(name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::new(name, QueryAttributeValue::Bytes(value.into()))
    }

    /// Create a null attribute.
    pub fn null(name: impl Into<String>) -> Self {
        Self::new(name, QueryAttributeValue::Null)
    }

    /// Get the MySQL type for this attribute's value.
    pub fn mysql_type(&self) -> u8 {
        match &self.value {
            QueryAttributeValue::Null => column_types::MYSQL_TYPE_NULL,
            QueryAttributeValue::String(_) => column_types::MYSQL_TYPE_VAR_STRING,
            QueryAttributeValue::Int(_) => column_types::MYSQL_TYPE_LONGLONG,
            QueryAttributeValue::UInt(_) => column_types::MYSQL_TYPE_LONGLONG,
            QueryAttributeValue::Double(_) => column_types::MYSQL_TYPE_DOUBLE,
            QueryAttributeValue::Bytes(_) => column_types::MYSQL_TYPE_BLOB,
        }
    }

    /// Check if the value is unsigned (for integer types).
    pub fn is_unsigned(&self) -> bool {
        matches!(&self.value, QueryAttributeValue::UInt(_))
    }
}

/// Query attributes collection.
#[derive(Clone, Debug, Default)]
pub struct QueryAttributes {
    /// The attributes.
    pub attributes: Vec<QueryAttribute>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum QueryAttributeError {
    #[error("invalid attribute count")]
    InvalidCount,
    #[error("invalid attribute name")]
    InvalidName,
    #[error("invalid attribute type: {0}")]
    InvalidType(u8),
    #[error("invalid attribute value")]
    InvalidValue,
}

impl QueryAttributes {
    /// Create empty query attributes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a single attribute.
    pub fn with(attr: QueryAttribute) -> Self {
        Self { attributes: vec![attr] }
    }

    /// Add an attribute.
    pub fn add(&mut self, attr: QueryAttribute) -> &mut Self {
        self.attributes.push(attr);
        self
    }

    /// Add a string attribute.
    pub fn add_string(&mut self, name: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.add(QueryAttribute::string(name, value))
    }

    /// Add an integer attribute.
    pub fn add_int(&mut self, name: impl Into<String>, value: i64) -> &mut Self {
        self.add(QueryAttribute::int(name, value))
    }

    /// Get an attribute by name.
    pub fn get(&self, name: &str) -> Option<&QueryAttribute> {
        self.attributes.iter().find(|a| a.name == name)
    }

    /// Get a string attribute value by name.
    pub fn get_string(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|a| match &a.value {
            QueryAttributeValue::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    /// Get an integer attribute value by name.
    pub fn get_int(&self, name: &str) -> Option<i64> {
        self.get(name).and_then(|a| match &a.value {
            QueryAttributeValue::Int(v) => Some(*v),
            QueryAttributeValue::UInt(v) => Some(*v as i64),
            _ => None,
        })
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.attributes.is_empty()
    }

    /// Get the count of attributes.
    pub fn len(&self) -> usize {
        self.attributes.len()
    }

    /// Encode the query attributes for sending with a query.
    ///
    /// Format:
    /// - parameter_count (lenenc int)
    /// - parameter_set_count (lenenc int) - always 1
    /// - NULL bitmap (if parameter_count > 0)
    /// - new_params_bind_flag (1) - always 1 for query attributes
    /// - For each parameter:
    ///   - type (2 bytes: type + unsigned flag)
    ///   - name (lenenc string)
    /// - For each non-NULL parameter:
    ///   - value (binary encoded)
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Parameter count
        write_lenenc_int(&mut buf, self.attributes.len() as u64)?;

        if self.attributes.is_empty() {
            return Ok(buf);
        }

        // Parameter set count (always 1)
        write_lenenc_int(&mut buf, 1)?;

        // NULL bitmap
        let null_bitmap_len = self.attributes.len().div_ceil(8);
        let mut null_bitmap = vec![0u8; null_bitmap_len];
        for (i, attr) in self.attributes.iter().enumerate() {
            if matches!(attr.value, QueryAttributeValue::Null) {
                let byte_pos = i / 8;
                let bit_pos = i % 8;
                null_bitmap[byte_pos] |= 1 << bit_pos;
            }
        }
        buf.write_all(&null_bitmap)?;

        // new_params_bind_flag
        buf.push(1);

        // Parameter types and names
        for attr in &self.attributes {
            let type_byte = attr.mysql_type();
            let unsigned_flag = if attr.is_unsigned() { 0x80 } else { 0x00 };
            buf.push(type_byte);
            buf.push(unsigned_flag);
            write_lenenc_string(&mut buf, attr.name.as_bytes())?;
        }

        // Parameter values
        for attr in &self.attributes {
            match &attr.value {
                QueryAttributeValue::Null => {} // No value for NULL
                QueryAttributeValue::String(s) => {
                    write_lenenc_string(&mut buf, s.as_bytes())?;
                }
                QueryAttributeValue::Int(v) => {
                    buf.write_all(&v.to_le_bytes())?;
                }
                QueryAttributeValue::UInt(v) => {
                    buf.write_all(&v.to_le_bytes())?;
                }
                QueryAttributeValue::Double(v) => {
                    buf.write_all(&v.to_le_bytes())?;
                }
                QueryAttributeValue::Bytes(b) => {
                    write_lenenc_string(&mut buf, b)?;
                }
            }
        }

        Ok(buf)
    }

    /// Parse query attributes from a stream.
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MysqlParseError<S::ReadError, QueryAttributeError>> {
        // Read parameter count
        let count = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(QueryAttributeError::InvalidCount))?;

        if count == 0 || count == u64::MAX {
            return Ok(Self::new());
        }

        let count = count as usize;

        // Read parameter set count (should be 1)
        let _set_count = stream.read_lenenc_int_sync().map_err(MysqlParseError::Stream)?;

        // Read NULL bitmap
        let null_bitmap_len = count.div_ceil(8);
        let null_bitmap = stream.read_bytes_sync(null_bitmap_len).map_err(MysqlParseError::Stream)?;

        // Read new_params_bind_flag
        let _bind_flag = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // Read types and names
        let mut types = Vec::with_capacity(count);
        let mut names = Vec::with_capacity(count);

        for _ in 0..count {
            let type_byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            let _unsigned_flag = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            types.push(type_byte);

            let name = stream
                .read_lenenc_string_sync()
                .map_err(MysqlParseError::Stream)?
                .map_err(|_| MysqlParseError::Parse(QueryAttributeError::InvalidName))?;
            names.push(String::from_utf8(name).map_err(|_| MysqlParseError::Parse(QueryAttributeError::InvalidName))?);
        }

        // Read values
        let mut attributes = Vec::with_capacity(count);
        for i in 0..count {
            let byte_pos = i / 8;
            let bit_pos = i % 8;
            let is_null = null_bitmap[byte_pos] & (1 << bit_pos) != 0;

            let value = if is_null {
                QueryAttributeValue::Null
            } else {
                parse_attribute_value(stream, types[i])?
            };

            attributes.push(QueryAttribute { name: names[i].clone(), value });
        }

        Ok(Self { attributes })
    }
}

fn parse_attribute_value<S: WireReadSync + ?Sized>(
    stream: &S,
    type_byte: u8,
) -> Result<QueryAttributeValue, MysqlParseError<S::ReadError, QueryAttributeError>> {
    match type_byte {
        column_types::MYSQL_TYPE_NULL => Ok(QueryAttributeValue::Null),
        column_types::MYSQL_TYPE_LONGLONG => {
            let val = stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(QueryAttributeValue::Int(val as i64))
        }
        column_types::MYSQL_TYPE_DOUBLE => {
            let val = stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?;
            Ok(QueryAttributeValue::Double(f64::from_bits(val)))
        }
        column_types::MYSQL_TYPE_VAR_STRING | column_types::MYSQL_TYPE_STRING | column_types::MYSQL_TYPE_BLOB => {
            let bytes = stream
                .read_lenenc_string_sync()
                .map_err(MysqlParseError::Stream)?
                .map_err(|_| MysqlParseError::Parse(QueryAttributeError::InvalidValue))?;
            match String::from_utf8(bytes.clone()) {
                Ok(s) => Ok(QueryAttributeValue::String(s)),
                Err(_) => Ok(QueryAttributeValue::Bytes(bytes)),
            }
        }
        _ => Err(MysqlParseError::Parse(QueryAttributeError::InvalidType(type_byte))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_attribute_creation() {
        let attr = QueryAttribute::string("trace_id", "abc123");
        assert_eq!(attr.name, "trace_id");
        assert_eq!(attr.value, QueryAttributeValue::String("abc123".to_string()));
    }

    #[test]
    fn test_query_attributes_collection() {
        let mut attrs = QueryAttributes::new();
        attrs.add_string("trace_id", "abc123").add_int("request_id", 42);

        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs.get_string("trace_id"), Some("abc123"));
        assert_eq!(attrs.get_int("request_id"), Some(42));
    }

    #[test]
    fn test_encode_empty() {
        let attrs = QueryAttributes::new();
        let encoded = attrs.encode().unwrap();
        assert_eq!(encoded, vec![0]); // Just the count (0)
    }

    #[test]
    fn test_mysql_types() {
        assert_eq!(QueryAttribute::null("x").mysql_type(), column_types::MYSQL_TYPE_NULL);
        assert_eq!(QueryAttribute::string("x", "y").mysql_type(), column_types::MYSQL_TYPE_VAR_STRING);
        assert_eq!(QueryAttribute::int("x", 1).mysql_type(), column_types::MYSQL_TYPE_LONGLONG);
        assert_eq!(QueryAttribute::double("x", 1.0).mysql_type(), column_types::MYSQL_TYPE_DOUBLE);
    }
}
