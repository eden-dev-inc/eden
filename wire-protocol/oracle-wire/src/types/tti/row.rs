//! Row data parsing for result sets.
//!
//! This module provides structures for representing and parsing row data
//! returned from Oracle queries.
//!
//! # Supported Data Types
//!
//! This module supports all Oracle data types across versions 11g through 23c:
//!
//! - **Character**: VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG
//! - **Numeric**: NUMBER, INTEGER, FLOAT, BINARY_FLOAT, BINARY_DOUBLE
//! - **Date/Time**: DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, INTERVAL
//! - **Binary**: RAW, LONG RAW, BLOB, BFILE
//! - **Special**: BOOLEAN, ROWID, CURSOR, JSON (21c+), XMLTYPE
//! - **Object**: User-defined types, VARRAY, NESTED TABLE

use super::column::ResultSetMetadata;
use super::data_types::OracleDataType;
use super::datetime::{OracleDate, OracleIntervalDs, OracleIntervalYm, OracleTimestamp, OracleTimestampTz};
use super::number::OracleNumber;

/// JSON value representation (Oracle 21c+).
///
/// Oracle's native JSON type can be stored in two formats:
/// - Text: UTF-8 JSON string
/// - OSON: Oracle's binary JSON format for efficient querying
#[derive(Clone, Debug, PartialEq)]
pub struct JsonValue {
    /// Raw JSON data (text or OSON binary format).
    pub data: Vec<u8>,
    /// True if data is in OSON binary format, false if text.
    pub is_oson: bool,
}

impl JsonValue {
    /// Create a JSON value from text.
    pub fn from_text(text: impl Into<String>) -> Self {
        Self { data: text.into().into_bytes(), is_oson: false }
    }

    /// Create a JSON value from binary OSON data.
    pub fn from_oson(data: Vec<u8>) -> Self {
        Self { data, is_oson: true }
    }

    /// Get the JSON as text (returns None if OSON format).
    pub fn as_text(&self) -> Option<&str> {
        if self.is_oson { None } else { std::str::from_utf8(&self.data).ok() }
    }

    /// Get raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Object/Collection value for user-defined types.
///
/// Represents VARRAY, NESTED TABLE, or user-defined ADT (Abstract Data Type).
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectValue {
    /// Type name (e.g., "HR.EMPLOYEE_TYPE", "SYS.XMLTYPE").
    pub type_name: Option<String>,
    /// Type OID (Object Identifier) if available.
    pub type_oid: Option<Vec<u8>>,
    /// Raw serialized data.
    pub data: Vec<u8>,
    /// True if this is a collection (VARRAY/NESTED TABLE).
    pub is_collection: bool,
}

impl ObjectValue {
    /// Create a new object value.
    pub fn new(data: Vec<u8>) -> Self {
        Self { type_name: None, type_oid: None, data, is_collection: false }
    }

    /// Create a collection value.
    pub fn collection(data: Vec<u8>) -> Self {
        Self { type_name: None, type_oid: None, data, is_collection: true }
    }

    /// Set the type name.
    pub fn with_type_name(mut self, name: impl Into<String>) -> Self {
        self.type_name = Some(name.into());
        self
    }

    /// Set the type OID.
    pub fn with_type_oid(mut self, oid: Vec<u8>) -> Self {
        self.type_oid = Some(oid);
        self
    }
}

/// A single column value in a row.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    /// NULL value.
    Null,
    /// VARCHAR2/NVARCHAR2/CHAR/NCHAR/LONG string value.
    String(String),
    /// RAW/LONG RAW/VARRAW binary value.
    Binary(Vec<u8>),
    /// NUMBER/INTEGER/FLOAT/DECIMAL/BINARY_FLOAT/BINARY_DOUBLE value.
    Number(OracleNumber),
    /// DATE value.
    Date(OracleDate),
    /// TIMESTAMP/TIMESTAMP WITH LOCAL TIME ZONE value.
    Timestamp(OracleTimestamp),
    /// TIMESTAMP WITH TIME ZONE value.
    TimestampTz(OracleTimestampTz),
    /// INTERVAL YEAR TO MONTH value.
    IntervalYm(OracleIntervalYm),
    /// INTERVAL DAY TO SECOND value.
    IntervalDs(OracleIntervalDs),
    /// ROWID/UROWID value (as string).
    Rowid(String),
    /// BOOLEAN value (PL/SQL, SQL in 23c+).
    Boolean(bool),
    /// LOB locator (CLOB/NCLOB/BLOB - not the actual data).
    LobLocator(LobLocator),
    /// BFILE locator (external file reference).
    BfileLocator(BfileLocator),
    /// Cursor (REF CURSOR).
    Cursor(u32),
    /// JSON value (Oracle 21c+ native JSON, stored as OSON binary or text).
    Json(JsonValue),
    /// XMLTYPE value.
    Xml(String),
    /// Object/Collection type (VARRAY, NESTED TABLE, user-defined ADT).
    /// Contains raw bytes for complex type; application should deserialize.
    Object(ObjectValue),
}

impl ColumnValue {
    /// Check if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to get as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            Self::Rowid(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            Self::String(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(n) => n.to_i64(),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some(n.to_f64()),
            _ => None,
        }
    }

    /// Try to get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            Self::Number(n) => n.to_i64().map(|i| i != 0),
            _ => None,
        }
    }

    /// Try to get as JSON.
    pub fn as_json(&self) -> Option<&JsonValue> {
        match self {
            Self::Json(j) => Some(j),
            _ => None,
        }
    }

    /// Try to get as JSON text.
    pub fn as_json_text(&self) -> Option<&str> {
        match self {
            Self::Json(j) => j.as_text(),
            _ => None,
        }
    }

    /// Try to get as XML string.
    pub fn as_xml(&self) -> Option<&str> {
        match self {
            Self::Xml(x) => Some(x),
            _ => None,
        }
    }

    /// Try to get as object/collection.
    pub fn as_object(&self) -> Option<&ObjectValue> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Get the Oracle data type of this value.
    pub fn data_type(&self) -> OracleDataType {
        match self {
            Self::Null => OracleDataType::Varchar2, // NULL has no inherent type
            Self::String(_) => OracleDataType::Varchar2,
            Self::Binary(_) => OracleDataType::Raw,
            Self::Number(_) => OracleDataType::Number,
            Self::Date(_) => OracleDataType::Date,
            Self::Timestamp(_) => OracleDataType::Timestamp,
            Self::TimestampTz(_) => OracleDataType::TimestampTz,
            Self::IntervalYm(_) => OracleDataType::IntervalYm,
            Self::IntervalDs(_) => OracleDataType::IntervalDs,
            Self::Rowid(_) => OracleDataType::Rowid,
            Self::Boolean(_) => OracleDataType::Boolean,
            Self::LobLocator(loc) => {
                if loc.is_clob {
                    OracleDataType::Clob
                } else {
                    OracleDataType::Blob
                }
            }
            Self::BfileLocator(_) => OracleDataType::Bfile,
            Self::Cursor(_) => OracleDataType::Cursor,
            Self::Json(_) => OracleDataType::Json,
            Self::Xml(_) => OracleDataType::XmlType,
            Self::Object(o) => {
                if o.is_collection {
                    OracleDataType::Varray
                } else {
                    OracleDataType::NamedType
                }
            }
        }
    }
}

/// A single row from a result set.
#[derive(Clone, Debug)]
pub struct Row {
    /// Column values in order.
    values: Vec<ColumnValue>,
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

    /// Get a column value by index (0-based).
    pub fn get(&self, index: usize) -> Option<&ColumnValue> {
        self.values.get(index)
    }

    /// Get a column value by index, returning NULL for out of bounds.
    pub fn get_or_null(&self, index: usize) -> &ColumnValue {
        self.values.get(index).unwrap_or(&ColumnValue::Null)
    }

    /// Iterate over column values.
    pub fn iter(&self) -> impl Iterator<Item = &ColumnValue> {
        self.values.iter()
    }

    /// Get string value at index.
    pub fn get_string(&self, index: usize) -> Option<&str> {
        self.get(index).and_then(|v| v.as_str())
    }

    /// Get i64 value at index.
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.get(index).and_then(|v| v.as_i64())
    }

    /// Get f64 value at index.
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.get(index).and_then(|v| v.as_f64())
    }

    /// Get bool value at index.
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        self.get(index).and_then(|v| v.as_bool())
    }

    /// Check if value at index is NULL.
    pub fn is_null(&self, index: usize) -> bool {
        self.get(index).is_none_or(|v| v.is_null())
    }
}

impl IntoIterator for Row {
    type Item = ColumnValue;
    type IntoIter = std::vec::IntoIter<ColumnValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// A result set containing rows and metadata.
#[derive(Clone, Debug)]
pub struct ResultSet {
    /// Column metadata.
    pub metadata: ResultSetMetadata,
    /// Rows in the result set.
    rows: Vec<Row>,
    /// Whether there are more rows to fetch.
    pub has_more: bool,
    /// Number of rows affected (for DML).
    pub rows_affected: Option<u64>,
}

impl ResultSet {
    /// Create a new result set.
    pub fn new(metadata: ResultSetMetadata) -> Self {
        Self {
            metadata,
            rows: Vec::new(),
            has_more: false,
            rows_affected: None,
        }
    }

    /// Create with pre-allocated row capacity.
    pub fn with_capacity(metadata: ResultSetMetadata, capacity: usize) -> Self {
        Self {
            metadata,
            rows: Vec::with_capacity(capacity),
            has_more: false,
            rows_affected: None,
        }
    }

    /// Add a row to the result set.
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Get the number of rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.metadata.column_count()
    }

    /// Get a row by index (0-based).
    pub fn row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    /// Iterate over rows.
    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.metadata.column_index(name)
    }

    /// Get a value by row and column index.
    pub fn get(&self, row: usize, col: usize) -> Option<&ColumnValue> {
        self.rows.get(row).and_then(|r| r.get(col))
    }

    /// Get a value by row index and column name.
    pub fn get_by_name(&self, row: usize, col_name: &str) -> Option<&ColumnValue> {
        let col_idx = self.column_index(col_name)?;
        self.get(row, col_idx)
    }

    /// Check if result set is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Set whether there are more rows.
    pub fn set_has_more(&mut self, has_more: bool) {
        self.has_more = has_more;
    }

    /// Set rows affected count.
    pub fn set_rows_affected(&mut self, count: u64) {
        self.rows_affected = Some(count);
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// LOB (Large Object) locator.
///
/// This is a handle to LOB data, not the actual data itself.
/// LOB data must be fetched separately using LOB operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LobLocator {
    /// Locator ID (opaque handle).
    pub id: Vec<u8>,
    /// Whether this is a CLOB (true) or BLOB (false).
    pub is_clob: bool,
    /// LOB size in bytes (may be -1 if unknown).
    pub size: i64,
    /// Chunk size for reading.
    pub chunk_size: u32,
}

impl LobLocator {
    /// Create a new LOB locator.
    pub fn new(id: Vec<u8>, is_clob: bool) -> Self {
        Self { id, is_clob, size: -1, chunk_size: 8192 }
    }

    /// Check if this is a CLOB.
    pub fn is_clob(&self) -> bool {
        self.is_clob
    }

    /// Check if this is a BLOB.
    pub fn is_blob(&self) -> bool {
        !self.is_clob
    }

    /// Check if size is known.
    pub fn has_size(&self) -> bool {
        self.size >= 0
    }
}

/// BFILE (Binary File) locator.
///
/// Points to an external file on the Oracle server's file system.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BfileLocator {
    /// Directory alias (Oracle directory object name).
    pub directory: String,
    /// File name.
    pub filename: String,
}

impl BfileLocator {
    /// Create a new BFILE locator.
    pub fn new(directory: impl Into<String>, filename: impl Into<String>) -> Self {
        Self { directory: directory.into(), filename: filename.into() }
    }
}

/// Error when parsing row data.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RowParseError {
    #[error("data too short for column {column}: expected {expected} bytes, got {actual}")]
    TooShort { column: usize, expected: usize, actual: usize },
    #[error("invalid data type {0} for column {1}")]
    InvalidType(u8, usize),
    #[error("column count mismatch: expected {expected}, got {actual}")]
    ColumnMismatch { expected: usize, actual: usize },
    #[error("invalid string encoding in column {0}")]
    InvalidEncoding(usize),
    #[error("invalid number encoding in column {0}")]
    InvalidNumber(usize),
    #[error("invalid date encoding in column {0}")]
    InvalidDate(usize),
}

/// Row decoder that parses raw wire data using column metadata.
///
/// Oracle row data format:
/// - Each column value is prefixed with a length byte (or special markers)
/// - Length 0 = NULL
/// - Length 254 = length in next 2 bytes
/// - Length 255 = special marker
/// - Otherwise length is the actual data length
#[derive(Clone, Debug)]
pub struct RowDecoder {
    /// Column metadata for decoding.
    metadata: ResultSetMetadata,
    /// Character set ID for string decoding.
    charset_id: u16,
}

impl RowDecoder {
    /// Create a new row decoder with the given metadata.
    pub fn new(metadata: ResultSetMetadata) -> Self {
        Self {
            metadata,
            charset_id: 873, // AL32UTF8 default
        }
    }

    /// Set the character set ID for string decoding.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Get the metadata.
    pub fn metadata(&self) -> &ResultSetMetadata {
        &self.metadata
    }

    /// Decode a single row from wire data.
    ///
    /// Returns the decoded row and the number of bytes consumed.
    pub fn decode_row(&self, data: &[u8]) -> Result<(Row, usize), RowParseError> {
        let mut offset = 0;
        let mut values = Vec::with_capacity(self.metadata.column_count());

        for (col_idx, col_info) in self.metadata.iter().enumerate() {
            if offset >= data.len() {
                return Err(RowParseError::TooShort { column: col_idx, expected: 1, actual: 0 });
            }

            let (value, consumed) = self.decode_value(data, offset, col_idx, col_info.data_type())?;
            values.push(value);
            offset += consumed;
        }

        Ok((Row::new(values), offset))
    }

    /// Decode multiple rows from wire data.
    ///
    /// Returns the decoded rows and total bytes consumed.
    pub fn decode_rows(&self, data: &[u8], count: usize) -> Result<(Vec<Row>, usize), RowParseError> {
        let mut rows = Vec::with_capacity(count);
        let mut offset = 0;

        for _ in 0..count {
            if offset >= data.len() {
                break;
            }

            let (row, consumed) = self.decode_row(&data[offset..])?;
            rows.push(row);
            offset += consumed;
        }

        Ok((rows, offset))
    }

    /// Decode a single column value.
    fn decode_value(
        &self,
        data: &[u8],
        offset: usize,
        col_idx: usize,
        data_type: OracleDataType,
    ) -> Result<(ColumnValue, usize), RowParseError> {
        if offset >= data.len() {
            return Err(RowParseError::TooShort { column: col_idx, expected: 1, actual: 0 });
        }

        let len_byte = data[offset];

        // NULL indicator
        if len_byte == 0 {
            return Ok((ColumnValue::Null, 1));
        }

        // Extended length (>253 bytes)
        let (value_len, header_len) = if len_byte == 254 {
            if offset + 3 > data.len() {
                return Err(RowParseError::TooShort { column: col_idx, expected: 3, actual: data.len() - offset });
            }
            let len = u16::from_be_bytes([data[offset + 1], data[offset + 2]]) as usize;
            (len, 3)
        } else if len_byte == 255 {
            // Special marker (typically also NULL or empty)
            return Ok((ColumnValue::Null, 1));
        } else {
            (len_byte as usize, 1)
        };

        let value_start = offset + header_len;
        let value_end = value_start + value_len;

        if value_end > data.len() {
            return Err(RowParseError::TooShort {
                column: col_idx,
                expected: value_len,
                actual: data.len() - value_start,
            });
        }

        let value_data = &data[value_start..value_end];
        let value = self.decode_typed_value(value_data, col_idx, data_type)?;

        Ok((value, header_len + value_len))
    }

    /// Decode a typed value from raw bytes.
    ///
    /// Supports all Oracle data types across versions 11g through 23c.
    fn decode_typed_value(&self, data: &[u8], col_idx: usize, data_type: OracleDataType) -> Result<ColumnValue, RowParseError> {
        match data_type {
            // ============================================================
            // Character Types - decode as UTF-8 string
            // ============================================================
            OracleDataType::Varchar2
            | OracleDataType::Varchar
            | OracleDataType::Char
            | OracleDataType::Nchar
            | OracleDataType::Charz
            | OracleDataType::String
            | OracleDataType::Long => {
                // For CHARZ, strip trailing null if present
                let data = if data_type == OracleDataType::Charz {
                    data.strip_suffix(&[0]).unwrap_or(data)
                } else {
                    data
                };
                let s = String::from_utf8_lossy(data).into_owned();
                Ok(ColumnValue::String(s))
            }

            // ============================================================
            // Numeric Types - decode as OracleNumber
            // ============================================================
            OracleDataType::Number
            | OracleDataType::Varnum
            | OracleDataType::Float
            | OracleDataType::Decimal
            | OracleDataType::Integer
            | OracleDataType::UnsignedInt => {
                OracleNumber::from_bytes(data).map(ColumnValue::Number).map_err(|_| RowParseError::InvalidNumber(col_idx))
            }

            OracleDataType::BinaryFloat => {
                if data.len() >= 4 {
                    let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(ColumnValue::Number(OracleNumber::from_f64(f as f64)))
                } else {
                    Err(RowParseError::InvalidNumber(col_idx))
                }
            }

            OracleDataType::BinaryDouble => {
                if data.len() >= 8 {
                    let f = f64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
                    Ok(ColumnValue::Number(OracleNumber::from_f64(f)))
                } else {
                    Err(RowParseError::InvalidNumber(col_idx))
                }
            }

            // ============================================================
            // Date/Time Types
            // ============================================================
            OracleDataType::Date => OracleDate::from_bytes(data).map(ColumnValue::Date).map_err(|_| RowParseError::InvalidDate(col_idx)),

            OracleDataType::Timestamp | OracleDataType::TimestampLtz => {
                OracleTimestamp::from_bytes(data).map(ColumnValue::Timestamp).map_err(|_| RowParseError::InvalidDate(col_idx))
            }

            OracleDataType::TimestampTz => {
                OracleTimestampTz::from_bytes(data).map(ColumnValue::TimestampTz).map_err(|_| RowParseError::InvalidDate(col_idx))
            }

            OracleDataType::IntervalYm => {
                OracleIntervalYm::from_bytes(data).map(ColumnValue::IntervalYm).map_err(|_| RowParseError::InvalidDate(col_idx))
            }

            OracleDataType::IntervalDs => {
                OracleIntervalDs::from_bytes(data).map(ColumnValue::IntervalDs).map_err(|_| RowParseError::InvalidDate(col_idx))
            }

            // ============================================================
            // Binary/Raw Types
            // ============================================================
            OracleDataType::Raw | OracleDataType::LongRaw | OracleDataType::Varraw => Ok(ColumnValue::Binary(data.to_vec())),

            // ============================================================
            // Row Identifier Types
            // ============================================================
            OracleDataType::Rowid | OracleDataType::Urowid => {
                let s = String::from_utf8_lossy(data).into_owned();
                Ok(ColumnValue::Rowid(s))
            }

            // ============================================================
            // LOB Types
            // ============================================================
            OracleDataType::Clob => {
                // LOB locator (CLOB or NCLOB, distinguished by charset_form)
                Ok(ColumnValue::LobLocator(LobLocator::new(data.to_vec(), true)))
            }

            OracleDataType::Blob => Ok(ColumnValue::LobLocator(LobLocator::new(data.to_vec(), false))),

            OracleDataType::Bfile => {
                // BFILE locator format: directory_len + directory + filename_len + filename
                if data.len() >= 2 {
                    let dir_len = data[0] as usize;
                    if data.len() > 1 + dir_len {
                        let directory = String::from_utf8_lossy(&data[1..1 + dir_len]).into_owned();
                        let fname_len = data[1 + dir_len] as usize;
                        let filename = if data.len() >= 2 + dir_len + fname_len {
                            String::from_utf8_lossy(&data[2 + dir_len..2 + dir_len + fname_len]).into_owned()
                        } else {
                            String::new()
                        };
                        return Ok(ColumnValue::BfileLocator(BfileLocator::new(directory, filename)));
                    }
                }
                Ok(ColumnValue::BfileLocator(BfileLocator::new("", "")))
            }

            // ============================================================
            // Special Types
            // ============================================================
            OracleDataType::Boolean => {
                let b = !data.is_empty() && data[0] != 0;
                Ok(ColumnValue::Boolean(b))
            }

            OracleDataType::Cursor => {
                // Cursor ID (typically 4 bytes)
                let cursor_id = if data.len() >= 4 {
                    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
                } else {
                    0
                };
                Ok(ColumnValue::Cursor(cursor_id))
            }

            // ============================================================
            // JSON Type (Oracle 21c+)
            // ============================================================
            OracleDataType::Json => {
                // Check if it's OSON (binary) or text JSON
                // OSON starts with 0xFF 0x4A ('J') or version byte
                let is_oson = data.len() >= 2 && (data[0] == 0xFF || data[0] < 0x20);
                if is_oson {
                    Ok(ColumnValue::Json(JsonValue::from_oson(data.to_vec())))
                } else {
                    Ok(ColumnValue::Json(JsonValue::from_text(String::from_utf8_lossy(data).into_owned())))
                }
            }

            // ============================================================
            // XMLTYPE
            // ============================================================
            OracleDataType::XmlType => {
                // XMLTYPE is typically returned as text
                let xml = String::from_utf8_lossy(data).into_owned();
                Ok(ColumnValue::Xml(xml))
            }

            // ============================================================
            // Object/Collection Types
            // ============================================================
            OracleDataType::NamedType | OracleDataType::Ref => {
                // User-defined type or REF - return as object with raw data
                Ok(ColumnValue::Object(ObjectValue::new(data.to_vec())))
            }

            OracleDataType::Varray | OracleDataType::NestedTable => {
                // Collection type - mark as collection
                Ok(ColumnValue::Object(ObjectValue::collection(data.to_vec())))
            }

            // ============================================================
            // Internal/System Types
            // ============================================================
            OracleDataType::AnyData | OracleDataType::AnyType | OracleDataType::AnyDataSet => {
                // These are rarely seen in normal queries; store as binary
                Ok(ColumnValue::Binary(data.to_vec()))
            }

            // ============================================================
            // Unknown Types
            // ============================================================
            OracleDataType::Unknown(_) => {
                // Unknown type - store as binary for forward compatibility
                Ok(ColumnValue::Binary(data.to_vec()))
            }
        }
    }

    /// Create a result set and decode rows into it.
    pub fn decode_result_set(&self, data: &[u8], row_count: usize, has_more: bool) -> Result<ResultSet, RowParseError> {
        let (rows, _) = self.decode_rows(data, row_count)?;
        let mut result_set = ResultSet::with_capacity(self.metadata.clone(), rows.len());

        for row in rows {
            result_set.add_row(row);
        }

        result_set.set_has_more(has_more);
        Ok(result_set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tti::column::MetadataBuilder;

    #[test]
    fn test_column_value() {
        let null = ColumnValue::Null;
        assert!(null.is_null());

        let string = ColumnValue::String("hello".to_string());
        assert_eq!(string.as_str(), Some("hello"));

        let number = ColumnValue::Number(OracleNumber::from_i64(42));
        assert_eq!(number.as_i64(), Some(42));

        let boolean = ColumnValue::Boolean(true);
        assert_eq!(boolean.as_bool(), Some(true));
    }

    #[test]
    fn test_row() {
        let row = Row::new(vec![
            ColumnValue::Number(OracleNumber::from_i64(1)),
            ColumnValue::String("Alice".to_string()),
            ColumnValue::Null,
        ]);

        assert_eq!(row.len(), 3);
        assert_eq!(row.get_i64(0), Some(1));
        assert_eq!(row.get_string(1), Some("Alice"));
        assert!(row.is_null(2));
    }

    #[test]
    fn test_result_set() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).varchar2("NAME", 100).build();

        let mut rs = ResultSet::new(metadata);
        rs.add_row(Row::new(vec![
            ColumnValue::Number(OracleNumber::from_i64(1)),
            ColumnValue::String("Alice".to_string()),
        ]));
        rs.add_row(Row::new(vec![
            ColumnValue::Number(OracleNumber::from_i64(2)),
            ColumnValue::String("Bob".to_string()),
        ]));

        assert_eq!(rs.row_count(), 2);
        assert_eq!(rs.column_count(), 2);
        assert_eq!(rs.get_by_name(0, "NAME").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(rs.get_by_name(1, "id").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn test_lob_locator() {
        let clob = LobLocator::new(vec![1, 2, 3, 4], true);
        assert!(clob.is_clob());
        assert!(!clob.is_blob());

        let blob = LobLocator::new(vec![5, 6, 7, 8], false);
        assert!(blob.is_blob());
        assert!(!blob.is_clob());
    }

    #[test]
    fn test_bfile_locator() {
        let bfile = BfileLocator::new("DATA_DIR", "report.pdf");
        assert_eq!(bfile.directory, "DATA_DIR");
        assert_eq!(bfile.filename, "report.pdf");
    }

    #[test]
    fn test_row_decoder_null() {
        let metadata = MetadataBuilder::new().varchar2("COL1", 100).build();
        let decoder = RowDecoder::new(metadata);

        // NULL value (length byte = 0)
        let data = [0u8];
        let (row, consumed) = decoder.decode_row(&data).unwrap();
        assert_eq!(consumed, 1);
        assert!(row.is_null(0));
    }

    #[test]
    fn test_row_decoder_string() {
        let metadata = MetadataBuilder::new().varchar2("NAME", 100).build();
        let decoder = RowDecoder::new(metadata);

        // String "Hello" with length prefix
        let data = [5u8, b'H', b'e', b'l', b'l', b'o'];
        let (row, consumed) = decoder.decode_row(&data).unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(row.get_string(0), Some("Hello"));
    }

    #[test]
    fn test_row_decoder_number() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).build();
        let decoder = RowDecoder::new(metadata);

        // Number 42 encoded as Oracle NUMBER
        let num = OracleNumber::from_i64(42);
        let num_bytes = num.to_bytes();
        let mut data = vec![num_bytes.len() as u8];
        data.extend_from_slice(&num_bytes);

        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get_i64(0), Some(42));
    }

    #[test]
    fn test_row_decoder_multiple_columns() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).varchar2("NAME", 100).build();
        let decoder = RowDecoder::new(metadata);

        // ID = 1, NAME = "Bob"
        let num = OracleNumber::from_i64(1);
        let num_bytes = num.to_bytes();
        let mut data = vec![num_bytes.len() as u8];
        data.extend_from_slice(&num_bytes);
        data.push(3); // length of "Bob"
        data.extend_from_slice(b"Bob");

        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row.get_i64(0), Some(1));
        assert_eq!(row.get_string(1), Some("Bob"));
    }

    #[test]
    fn test_row_decoder_multiple_rows() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).build();
        let decoder = RowDecoder::new(metadata);

        // Two rows: ID=1 and ID=2
        let num1 = OracleNumber::from_i64(1);
        let num1_bytes = num1.to_bytes();
        let num2 = OracleNumber::from_i64(2);
        let num2_bytes = num2.to_bytes();

        let mut data = vec![num1_bytes.len() as u8];
        data.extend_from_slice(&num1_bytes);
        data.push(num2_bytes.len() as u8);
        data.extend_from_slice(&num2_bytes);

        let (rows, _) = decoder.decode_rows(&data, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_i64(0), Some(1));
        assert_eq!(rows[1].get_i64(0), Some(2));
    }

    #[test]
    fn test_row_decoder_extended_length() {
        let metadata = MetadataBuilder::new().varchar2("LONG_TEXT", 1000).build();
        let decoder = RowDecoder::new(metadata);

        // String with extended length (>253 bytes)
        let text: String = "x".repeat(300);
        let mut data = vec![254u8]; // extended length marker
        data.extend_from_slice(&(300u16).to_be_bytes());
        data.extend_from_slice(text.as_bytes());

        let (row, consumed) = decoder.decode_row(&data).unwrap();
        assert_eq!(consumed, 303); // 1 + 2 + 300
        assert_eq!(row.get_string(0), Some(text.as_str()));
    }

    #[test]
    fn test_row_decoder_result_set() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).varchar2("NAME", 100).build();
        let decoder = RowDecoder::new(metadata);

        // Two rows of data
        let num1 = OracleNumber::from_i64(1);
        let num1_bytes = num1.to_bytes();
        let num2 = OracleNumber::from_i64(2);
        let num2_bytes = num2.to_bytes();

        let mut data = Vec::new();
        // Row 1: ID=1, NAME="Alice"
        data.push(num1_bytes.len() as u8);
        data.extend_from_slice(&num1_bytes);
        data.push(5);
        data.extend_from_slice(b"Alice");
        // Row 2: ID=2, NAME="Bob"
        data.push(num2_bytes.len() as u8);
        data.extend_from_slice(&num2_bytes);
        data.push(3);
        data.extend_from_slice(b"Bob");

        let rs = decoder.decode_result_set(&data, 2, false).unwrap();
        assert_eq!(rs.row_count(), 2);
        assert!(!rs.has_more);
        assert_eq!(rs.get_by_name(0, "NAME").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(rs.get_by_name(1, "NAME").and_then(|v| v.as_str()), Some("Bob"));
    }

    #[test]
    fn test_row_decoder_boolean() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("FLAG", OracleDataType::Boolean)).build();
        let decoder = RowDecoder::new(metadata);

        // Boolean true (1 byte, value 1)
        let data = [1u8, 1u8];
        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(true));

        // Boolean false (1 byte, value 0)
        let data = [1u8, 0u8];
        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(false));
    }

    #[test]
    fn test_row_decoder_binary() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::{OracleDataType, TypeDescriptor};

        let metadata = MetadataBuilder::new()
            .column(ColumnInfo::new("DATA", OracleDataType::Raw).with_type_desc(TypeDescriptor::new(OracleDataType::Raw).with_size(100)))
            .build();
        let decoder = RowDecoder::new(metadata);

        let data = [4u8, 0xDE, 0xAD, 0xBE, 0xEF];
        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get(0).and_then(|v| v.as_bytes()), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn test_json_value() {
        // Text JSON
        let json = JsonValue::from_text(r#"{"name": "test"}"#);
        assert!(!json.is_oson);
        assert_eq!(json.as_text(), Some(r#"{"name": "test"}"#));

        // OSON binary
        let oson = JsonValue::from_oson(vec![0xFF, 0x4A, 0x01, 0x02]);
        assert!(oson.is_oson);
        assert_eq!(oson.as_text(), None);
        assert_eq!(oson.as_bytes(), &[0xFF, 0x4A, 0x01, 0x02]);
    }

    #[test]
    fn test_object_value() {
        // Regular object
        let obj = ObjectValue::new(vec![1, 2, 3]).with_type_name("HR.EMPLOYEE_TYPE");
        assert!(!obj.is_collection);
        assert_eq!(obj.type_name, Some("HR.EMPLOYEE_TYPE".to_string()));

        // Collection
        let coll = ObjectValue::collection(vec![4, 5, 6]);
        assert!(coll.is_collection);
    }

    #[test]
    fn test_row_decoder_json() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("DOC", OracleDataType::Json)).build();
        let decoder = RowDecoder::new(metadata);

        // Text JSON
        let json_text = r#"{"id": 123}"#;
        let mut data = vec![json_text.len() as u8];
        data.extend_from_slice(json_text.as_bytes());

        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get(0).and_then(|v| v.as_json_text()), Some(r#"{"id": 123}"#));
    }

    #[test]
    fn test_row_decoder_xml() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("XML_DATA", OracleDataType::XmlType)).build();
        let decoder = RowDecoder::new(metadata);

        let xml = "<root><item>test</item></root>";
        let mut data = vec![xml.len() as u8];
        data.extend_from_slice(xml.as_bytes());

        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get(0).and_then(|v| v.as_xml()), Some("<root><item>test</item></root>"));
    }

    #[test]
    fn test_row_decoder_varray() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("ITEMS", OracleDataType::Varray)).build();
        let decoder = RowDecoder::new(metadata);

        // Simulated VARRAY data (just raw bytes)
        let data = [4u8, 0x01, 0x02, 0x03, 0x04];
        let (row, _) = decoder.decode_row(&data).unwrap();

        let obj = row.get(0).and_then(|v| v.as_object());
        assert!(obj.is_some());
        assert!(obj.unwrap().is_collection);
    }

    #[test]
    fn test_row_decoder_decimal() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("AMOUNT", OracleDataType::Decimal)).build();
        let decoder = RowDecoder::new(metadata);

        // Decimal is stored as Oracle NUMBER
        let num = OracleNumber::from_i64(12345);
        let num_bytes = num.to_bytes();
        let mut data = vec![num_bytes.len() as u8];
        data.extend_from_slice(&num_bytes);

        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get_i64(0), Some(12345));
    }

    #[test]
    fn test_row_decoder_varraw() {
        use crate::types::tti::column::ColumnInfo;
        use crate::types::tti::data_types::OracleDataType;

        let metadata = MetadataBuilder::new().column(ColumnInfo::new("BINARY_DATA", OracleDataType::Varraw)).build();
        let decoder = RowDecoder::new(metadata);

        let data = [3u8, 0xCA, 0xFE, 0xBA];
        let (row, _) = decoder.decode_row(&data).unwrap();
        assert_eq!(row.get(0).and_then(|v| v.as_bytes()), Some(&[0xCA, 0xFE, 0xBA][..]));
    }

    #[test]
    fn test_data_type_helpers() {
        use crate::types::tti::data_types::OracleDataType;

        // Character types
        assert!(OracleDataType::Varchar2.is_character());
        assert!(OracleDataType::Char.is_character());
        assert!(OracleDataType::Nchar.is_character());
        assert!(OracleDataType::Charz.is_character());
        assert!(OracleDataType::Clob.is_character());

        // Numeric types
        assert!(OracleDataType::Number.is_numeric());
        assert!(OracleDataType::Decimal.is_numeric());
        assert!(OracleDataType::BinaryFloat.is_numeric());

        // Date/time types
        assert!(OracleDataType::Date.is_datetime());
        assert!(OracleDataType::Timestamp.is_datetime());
        assert!(OracleDataType::IntervalDs.is_datetime());

        // Collection types
        assert!(OracleDataType::Varray.is_collection());
        assert!(OracleDataType::NestedTable.is_collection());

        // JSON
        assert!(OracleDataType::Json.is_json());

        // XML
        assert!(OracleDataType::XmlType.is_xml());

        // Version requirements
        assert_eq!(OracleDataType::Json.min_oracle_version(), "21c");
        assert_eq!(OracleDataType::BinaryFloat.min_oracle_version(), "10g");
        assert_eq!(OracleDataType::Varchar2.min_oracle_version(), "7");
    }
}
