//! Bind variable encoding for parameterized queries.
//!
//! Oracle uses bind variables for parameterized SQL execution.
//! This module provides encoding/decoding of bind values.

use super::charset::CharsetId;
use super::data_types::OracleDataType;
use super::datetime::{OracleDate, OracleTimestamp, OracleTimestampTz};
use super::number::OracleNumber;

/// Direction of a bind variable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BindDirection {
    /// Input only (default).
    #[default]
    In,
    /// Output only.
    Out,
    /// Both input and output.
    InOut,
}

impl BindDirection {
    /// Oracle's internal code for this direction.
    pub const fn code(self) -> u8 {
        match self {
            Self::In => 0x01,
            Self::Out => 0x02,
            Self::InOut => 0x03,
        }
    }

    /// Parse from Oracle's internal code.
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(Self::In),
            0x02 => Some(Self::Out),
            0x03 => Some(Self::InOut),
            _ => None,
        }
    }
}

/// Descriptor for a bind variable.
#[derive(Clone, Debug)]
pub struct BindDescriptor {
    /// Data type.
    pub data_type: OracleDataType,
    /// Maximum size in bytes.
    pub max_size: u32,
    /// Precision (for NUMBER).
    pub precision: u8,
    /// Scale (for NUMBER).
    pub scale: i8,
    /// Direction.
    pub direction: BindDirection,
    /// Whether NULL is allowed.
    pub nullable: bool,
    /// Character set for string types.
    pub charset: CharsetId,
    /// Bind name (e.g., ":1" or ":name").
    pub name: Option<String>,
}

impl BindDescriptor {
    /// Create a new bind descriptor.
    pub fn new(data_type: OracleDataType) -> Self {
        Self {
            data_type,
            max_size: 0,
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::AL32UTF8,
            name: None,
        }
    }

    /// Create a VARCHAR2 bind.
    pub fn varchar2(max_size: u32) -> Self {
        Self {
            data_type: OracleDataType::Varchar2,
            max_size,
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::AL32UTF8,
            name: None,
        }
    }

    /// Create a NUMBER bind.
    pub fn number(precision: u8, scale: i8) -> Self {
        Self {
            data_type: OracleDataType::Number,
            max_size: 22, // Oracle NUMBER max size
            precision,
            scale,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::US7ASCII,
            name: None,
        }
    }

    /// Create a DATE bind.
    pub fn date() -> Self {
        Self {
            data_type: OracleDataType::Date,
            max_size: 7,
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::US7ASCII,
            name: None,
        }
    }

    /// Create a TIMESTAMP bind.
    pub fn timestamp() -> Self {
        Self {
            data_type: OracleDataType::Timestamp,
            max_size: 11,
            precision: 0,
            scale: 6, // Default fractional seconds precision
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::US7ASCII,
            name: None,
        }
    }

    /// Create a RAW bind.
    pub fn raw(max_size: u32) -> Self {
        Self {
            data_type: OracleDataType::Raw,
            max_size,
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::US7ASCII,
            name: None,
        }
    }

    /// Create a CLOB bind.
    pub fn clob() -> Self {
        Self {
            data_type: OracleDataType::Clob,
            max_size: 0, // LOBs don't have a max size
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::AL32UTF8,
            name: None,
        }
    }

    /// Create a BLOB bind.
    pub fn blob() -> Self {
        Self {
            data_type: OracleDataType::Blob,
            max_size: 0,
            precision: 0,
            scale: 0,
            direction: BindDirection::In,
            nullable: true,
            charset: CharsetId::US7ASCII,
            name: None,
        }
    }

    /// Set the direction.
    pub fn with_direction(mut self, direction: BindDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Set as output parameter.
    pub fn as_out(mut self) -> Self {
        self.direction = BindDirection::Out;
        self
    }

    /// Set as input/output parameter.
    pub fn as_inout(mut self) -> Self {
        self.direction = BindDirection::InOut;
        self
    }

    /// Set the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set nullability.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Encode the descriptor to bytes for the wire protocol.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);

        // Data type (1 byte)
        buf.push(self.data_type.code());

        // Flags/direction (1 byte)
        buf.push(self.direction.code());

        // Precision (1 byte)
        buf.push(self.precision);

        // Scale (1 byte, signed)
        buf.push(self.scale as u8);

        // Max size (4 bytes, big-endian)
        buf.extend_from_slice(&self.max_size.to_be_bytes());

        // Character set ID (2 bytes)
        buf.extend_from_slice(&self.charset.id().to_be_bytes());

        // Nullable flag (1 byte)
        buf.push(if self.nullable { 1 } else { 0 });

        // Name length and name (if present)
        if let Some(ref name) = self.name {
            let name_bytes = name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
        } else {
            buf.push(0);
        }

        buf
    }
}

/// A bound value ready for wire transmission.
#[derive(Clone, Debug)]
pub enum BindValue {
    /// NULL value.
    Null,
    /// String value (VARCHAR2, CHAR, etc.).
    String(String),
    /// Binary value (RAW).
    Binary(Vec<u8>),
    /// Number value.
    Number(OracleNumber),
    /// Date value.
    Date(OracleDate),
    /// Timestamp value.
    Timestamp(OracleTimestamp),
    /// Timestamp with timezone.
    TimestampTz(OracleTimestampTz),
    /// Boolean (PL/SQL).
    Boolean(bool),
    /// ROWID as string.
    Rowid(String),
}

impl BindValue {
    /// Create a NULL value.
    pub fn null() -> Self {
        Self::Null
    }

    /// Create from a string value.
    pub fn from_string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    /// Create from bytes.
    pub fn from_bytes(b: impl Into<Vec<u8>>) -> Self {
        Self::Binary(b.into())
    }

    /// Create from an i64.
    pub fn from_i64(n: i64) -> Self {
        Self::Number(OracleNumber::from_i64(n))
    }

    /// Create from an f64.
    pub fn from_f64(n: f64) -> Self {
        Self::Number(OracleNumber::from_f64(n))
    }

    /// Create from a bool.
    pub fn from_bool(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Check if this is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Encode the value to bytes for wire transmission.
    ///
    /// Returns (indicator, data) where indicator is 0 for non-null, -1 for null.
    pub fn encode(&self, charset: CharsetId) -> (i16, Vec<u8>) {
        match self {
            Self::Null => (-1, Vec::new()),
            Self::String(s) => {
                let bytes = if charset.is_utf8() {
                    s.as_bytes().to_vec()
                } else {
                    // For non-UTF8, attempt conversion (simplified)
                    s.as_bytes().to_vec()
                };
                (0, encode_with_length(&bytes))
            }
            Self::Binary(b) => (0, encode_with_length(b)),
            Self::Number(n) => (0, n.to_bytes().to_vec()),
            Self::Date(d) => (0, d.to_bytes().to_vec()),
            Self::Timestamp(t) => (0, t.to_bytes().to_vec()),
            Self::TimestampTz(t) => (0, t.to_bytes().to_vec()),
            Self::Boolean(b) => {
                // Oracle PL/SQL boolean: 1 = true, 0 = false
                (0, vec![if *b { 1 } else { 0 }])
            }
            Self::Rowid(s) => (0, encode_with_length(s.as_bytes())),
        }
    }

    /// Get the Oracle data type for this value.
    pub fn data_type(&self) -> OracleDataType {
        match self {
            Self::Null => OracleDataType::Varchar2,
            Self::String(_) => OracleDataType::Varchar2,
            Self::Binary(_) => OracleDataType::Raw,
            Self::Number(_) => OracleDataType::Number,
            Self::Date(_) => OracleDataType::Date,
            Self::Timestamp(_) => OracleDataType::Timestamp,
            Self::TimestampTz(_) => OracleDataType::TimestampTz,
            Self::Boolean(_) => OracleDataType::Boolean,
            Self::Rowid(_) => OracleDataType::Rowid,
        }
    }
}

/// Encode data with a length prefix.
fn encode_with_length(data: &[u8]) -> Vec<u8> {
    let len = data.len();
    let mut buf = Vec::with_capacity(len + 4);

    if len <= 253 {
        // Short form: 1-byte length
        buf.push(len as u8);
    } else if len <= 65535 {
        // Medium form: 0xFE + 2-byte length
        buf.push(0xFE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        // Long form: 0xFF + 4-byte length
        buf.push(0xFF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }

    buf.extend_from_slice(data);
    buf
}

/// A complete set of bind values for a statement.
#[derive(Clone, Debug, Default)]
pub struct BindSet {
    /// Bind descriptors.
    descriptors: Vec<BindDescriptor>,
    /// Bind values (parallel to descriptors).
    values: Vec<BindValue>,
}

impl BindSet {
    /// Create a new empty bind set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            descriptors: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    /// Add a bind variable.
    pub fn add(&mut self, descriptor: BindDescriptor, value: BindValue) {
        self.descriptors.push(descriptor);
        self.values.push(value);
    }

    /// Add a string bind.
    pub fn add_string(&mut self, name: Option<&str>, value: impl Into<String>, max_size: u32) {
        let mut desc = BindDescriptor::varchar2(max_size);
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, BindValue::from_string(value));
    }

    /// Add an integer bind.
    pub fn add_i64(&mut self, name: Option<&str>, value: i64) {
        let mut desc = BindDescriptor::number(38, 0);
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, BindValue::from_i64(value));
    }

    /// Add a float bind.
    pub fn add_f64(&mut self, name: Option<&str>, value: f64) {
        let mut desc = BindDescriptor::number(38, 127); // BINARY_DOUBLE
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, BindValue::from_f64(value));
    }

    /// Add a NULL bind.
    pub fn add_null(&mut self, name: Option<&str>, data_type: OracleDataType) {
        let mut desc = BindDescriptor::new(data_type);
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, BindValue::null());
    }

    /// Get the number of binds.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a bind value by index.
    pub fn get(&self, index: usize) -> Option<(&BindDescriptor, &BindValue)> {
        self.descriptors.get(index).zip(self.values.get(index))
    }

    /// Iterate over binds.
    pub fn iter(&self) -> impl Iterator<Item = (&BindDescriptor, &BindValue)> {
        self.descriptors.iter().zip(self.values.iter())
    }

    /// Encode all descriptors for wire transmission.
    pub fn encode_descriptors(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of binds (2 bytes)
        buf.extend_from_slice(&(self.descriptors.len() as u16).to_be_bytes());

        // Each descriptor
        for desc in &self.descriptors {
            buf.extend(desc.encode());
        }

        buf
    }

    /// Encode all values for wire transmission.
    pub fn encode_values(&self, charset: CharsetId) -> Vec<u8> {
        let mut buf = Vec::new();

        for value in &self.values {
            let (indicator, data) = value.encode(charset);

            // Indicator (2 bytes)
            buf.extend_from_slice(&indicator.to_be_bytes());

            // Data
            buf.extend(data);
        }

        buf
    }
}

/// Error when parsing bind data.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BindError {
    #[error("bind index {0} out of range")]
    IndexOutOfRange(usize),
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: &'static str, actual: &'static str },
    #[error("data too short: expected {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("invalid bind direction: {0}")]
    InvalidDirection(u8),
    #[error("array size {actual} exceeds maximum {max}")]
    ArrayTooLarge { actual: usize, max: usize },
    #[error("array element type mismatch at index {index}")]
    ArrayElementMismatch { index: usize },
    #[error("inconsistent array sizes: expected {expected}, got {actual}")]
    InconsistentArraySize { expected: usize, actual: usize },
}

/// Maximum array bind size.
pub const MAX_ARRAY_SIZE: usize = 32767;

/// Array bind type for PL/SQL table parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArrayBindType {
    /// Index-by table (associative array).
    IndexByTable,
    /// VARRAY (variable-size array).
    Varray,
    /// NESTED TABLE.
    NestedTable,
}

impl ArrayBindType {
    /// Get the Oracle type code.
    pub fn code(&self) -> u8 {
        match self {
            Self::IndexByTable => 0x01,
            Self::Varray => 0x6E,
            Self::NestedTable => 0x6F,
        }
    }

    /// Parse from type code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(Self::IndexByTable),
            0x6E => Some(Self::Varray),
            0x6F => Some(Self::NestedTable),
            _ => None,
        }
    }
}

/// Descriptor for an array bind parameter.
#[derive(Clone, Debug)]
pub struct ArrayBindDescriptor {
    /// Element data type.
    pub element_type: OracleDataType,
    /// Maximum element size in bytes.
    pub max_element_size: u32,
    /// Maximum array size (number of elements).
    pub max_array_size: u32,
    /// Array type.
    pub array_type: ArrayBindType,
    /// Direction.
    pub direction: BindDirection,
    /// Bind name.
    pub name: Option<String>,
    /// Character set for string elements.
    pub charset: CharsetId,
}

impl ArrayBindDescriptor {
    /// Create a new array bind descriptor.
    pub fn new(element_type: OracleDataType, max_array_size: u32) -> Self {
        Self {
            element_type,
            max_element_size: 0,
            max_array_size,
            array_type: ArrayBindType::IndexByTable,
            direction: BindDirection::In,
            name: None,
            charset: CharsetId::AL32UTF8,
        }
    }

    /// Create a VARCHAR2 array bind.
    pub fn varchar2_array(max_element_size: u32, max_array_size: u32) -> Self {
        Self {
            element_type: OracleDataType::Varchar2,
            max_element_size,
            max_array_size,
            array_type: ArrayBindType::IndexByTable,
            direction: BindDirection::In,
            name: None,
            charset: CharsetId::AL32UTF8,
        }
    }

    /// Create a NUMBER array bind.
    pub fn number_array(max_array_size: u32) -> Self {
        Self {
            element_type: OracleDataType::Number,
            max_element_size: 22,
            max_array_size,
            array_type: ArrayBindType::IndexByTable,
            direction: BindDirection::In,
            name: None,
            charset: CharsetId::US7ASCII,
        }
    }

    /// Create a DATE array bind.
    pub fn date_array(max_array_size: u32) -> Self {
        Self {
            element_type: OracleDataType::Date,
            max_element_size: 7,
            max_array_size,
            array_type: ArrayBindType::IndexByTable,
            direction: BindDirection::In,
            name: None,
            charset: CharsetId::US7ASCII,
        }
    }

    /// Set as VARRAY.
    pub fn as_varray(mut self) -> Self {
        self.array_type = ArrayBindType::Varray;
        self
    }

    /// Set as NESTED TABLE.
    pub fn as_nested_table(mut self) -> Self {
        self.array_type = ArrayBindType::NestedTable;
        self
    }

    /// Set the direction.
    pub fn with_direction(mut self, direction: BindDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Set as output parameter.
    pub fn as_out(mut self) -> Self {
        self.direction = BindDirection::Out;
        self
    }

    /// Set as input/output parameter.
    pub fn as_inout(mut self) -> Self {
        self.direction = BindDirection::InOut;
        self
    }

    /// Set the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Encode the descriptor to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(48);

        // Array type marker (1 byte)
        buf.push(0xF0); // Array bind indicator

        // Array collection type (1 byte)
        buf.push(self.array_type.code());

        // Element type (1 byte)
        buf.push(self.element_type.code());

        // Direction (1 byte)
        buf.push(self.direction.code());

        // Max element size (4 bytes)
        buf.extend_from_slice(&self.max_element_size.to_be_bytes());

        // Max array size (4 bytes)
        buf.extend_from_slice(&self.max_array_size.to_be_bytes());

        // Character set (2 bytes)
        buf.extend_from_slice(&self.charset.id().to_be_bytes());

        // Name
        if let Some(ref name) = self.name {
            let name_bytes = name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
        } else {
            buf.push(0);
        }

        buf
    }
}

/// An array of bind values for bulk operations.
#[derive(Clone, Debug)]
pub struct ArrayBindValue {
    /// Element values.
    values: Vec<BindValue>,
    /// Null indicators for each element.
    null_indicators: Vec<bool>,
}

impl ArrayBindValue {
    /// Create a new empty array bind value.
    pub fn new() -> Self {
        Self { values: Vec::new(), null_indicators: Vec::new() }
    }

    /// Create with capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            null_indicators: Vec::with_capacity(capacity),
        }
    }

    /// Create from a vector of values.
    pub fn from_values(values: Vec<BindValue>) -> Self {
        let null_indicators = values.iter().map(|v| v.is_null()).collect();
        Self { values, null_indicators }
    }

    /// Create a string array.
    pub fn from_strings(strings: impl IntoIterator<Item = Option<String>>) -> Self {
        let mut values = Vec::new();
        let mut null_indicators = Vec::new();

        for s in strings {
            match s {
                Some(str) => {
                    values.push(BindValue::String(str));
                    null_indicators.push(false);
                }
                None => {
                    values.push(BindValue::Null);
                    null_indicators.push(true);
                }
            }
        }

        Self { values, null_indicators }
    }

    /// Create an integer array.
    pub fn from_i64s(numbers: impl IntoIterator<Item = Option<i64>>) -> Self {
        let mut values = Vec::new();
        let mut null_indicators = Vec::new();

        for n in numbers {
            match n {
                Some(num) => {
                    values.push(BindValue::from_i64(num));
                    null_indicators.push(false);
                }
                None => {
                    values.push(BindValue::Null);
                    null_indicators.push(true);
                }
            }
        }

        Self { values, null_indicators }
    }

    /// Create a float array.
    pub fn from_f64s(numbers: impl IntoIterator<Item = Option<f64>>) -> Self {
        let mut values = Vec::new();
        let mut null_indicators = Vec::new();

        for n in numbers {
            match n {
                Some(num) => {
                    values.push(BindValue::from_f64(num));
                    null_indicators.push(false);
                }
                None => {
                    values.push(BindValue::Null);
                    null_indicators.push(true);
                }
            }
        }

        Self { values, null_indicators }
    }

    /// Add an element.
    pub fn push(&mut self, value: BindValue) {
        self.null_indicators.push(value.is_null());
        self.values.push(value);
    }

    /// Add a null element.
    pub fn push_null(&mut self) {
        self.null_indicators.push(true);
        self.values.push(BindValue::Null);
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get an element by index.
    pub fn get(&self, index: usize) -> Option<&BindValue> {
        self.values.get(index)
    }

    /// Check if an element is null.
    pub fn is_null(&self, index: usize) -> Option<bool> {
        self.null_indicators.get(index).copied()
    }

    /// Iterate over values.
    pub fn iter(&self) -> impl Iterator<Item = &BindValue> {
        self.values.iter()
    }

    /// Iterate with null indicators.
    pub fn iter_with_nulls(&self) -> impl Iterator<Item = (&BindValue, bool)> {
        self.values.iter().zip(self.null_indicators.iter().copied())
    }

    /// Encode for wire transmission.
    pub fn encode(&self, charset: CharsetId) -> Result<Vec<u8>, BindError> {
        if self.values.len() > MAX_ARRAY_SIZE {
            return Err(BindError::ArrayTooLarge { actual: self.values.len(), max: MAX_ARRAY_SIZE });
        }

        let mut buf = Vec::new();

        // Array length (4 bytes)
        buf.extend_from_slice(&(self.values.len() as u32).to_be_bytes());

        // Null bitmap
        let bitmap_bytes = self.values.len().div_ceil(8);
        let mut bitmap = vec![0u8; bitmap_bytes];
        for (i, &is_null) in self.null_indicators.iter().enumerate() {
            if is_null {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        buf.extend_from_slice(&bitmap);

        // Element data (non-null only)
        for (value, &is_null) in self.values.iter().zip(self.null_indicators.iter()) {
            if !is_null {
                let (_, data) = value.encode(charset);
                buf.extend(data);
            }
        }

        Ok(buf)
    }

    /// Get the data type for elements (based on first non-null).
    pub fn element_data_type(&self) -> OracleDataType {
        self.values.iter().find(|v| !v.is_null()).map(|v| v.data_type()).unwrap_or(OracleDataType::Varchar2)
    }
}

impl Default for ArrayBindValue {
    fn default() -> Self {
        Self::new()
    }
}

/// A complete set of array binds for bulk operations.
#[derive(Clone, Debug, Default)]
pub struct ArrayBindSet {
    /// Array descriptors.
    descriptors: Vec<ArrayBindDescriptor>,
    /// Array values (parallel to descriptors).
    values: Vec<ArrayBindValue>,
    /// Batch size (number of rows).
    batch_size: usize,
}

impl ArrayBindSet {
    /// Create a new empty array bind set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a specific batch size.
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self { descriptors: Vec::new(), values: Vec::new(), batch_size }
    }

    /// Add an array bind.
    pub fn add(&mut self, descriptor: ArrayBindDescriptor, value: ArrayBindValue) -> Result<(), BindError> {
        // Validate array size consistency
        if !self.values.is_empty() && value.len() != self.batch_size {
            return Err(BindError::InconsistentArraySize { expected: self.batch_size, actual: value.len() });
        }

        if self.values.is_empty() {
            self.batch_size = value.len();
        }

        self.descriptors.push(descriptor);
        self.values.push(value);
        Ok(())
    }

    /// Add a string array.
    pub fn add_strings(
        &mut self,
        name: Option<&str>,
        values: impl IntoIterator<Item = Option<String>>,
        max_element_size: u32,
    ) -> Result<(), BindError> {
        let array_value = ArrayBindValue::from_strings(values);
        let mut desc = ArrayBindDescriptor::varchar2_array(max_element_size, array_value.len() as u32);
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, array_value)
    }

    /// Add an integer array.
    pub fn add_i64s(&mut self, name: Option<&str>, values: impl IntoIterator<Item = Option<i64>>) -> Result<(), BindError> {
        let array_value = ArrayBindValue::from_i64s(values);
        let mut desc = ArrayBindDescriptor::number_array(array_value.len() as u32);
        if let Some(n) = name {
            desc = desc.with_name(n);
        }
        self.add(desc, array_value)
    }

    /// Get the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the number of array binds.
    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }

    /// Get an array bind by index.
    pub fn get(&self, index: usize) -> Option<(&ArrayBindDescriptor, &ArrayBindValue)> {
        self.descriptors.get(index).zip(self.values.get(index))
    }

    /// Iterate over array binds.
    pub fn iter(&self) -> impl Iterator<Item = (&ArrayBindDescriptor, &ArrayBindValue)> {
        self.descriptors.iter().zip(self.values.iter())
    }

    /// Encode descriptors for wire transmission.
    pub fn encode_descriptors(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of binds (2 bytes)
        buf.extend_from_slice(&(self.descriptors.len() as u16).to_be_bytes());

        // Batch size (4 bytes)
        buf.extend_from_slice(&(self.batch_size as u32).to_be_bytes());

        // Each descriptor
        for desc in &self.descriptors {
            buf.extend(desc.encode());
        }

        buf
    }

    /// Encode values for wire transmission.
    pub fn encode_values(&self, charset: CharsetId) -> Result<Vec<u8>, BindError> {
        let mut buf = Vec::new();

        for value in &self.values {
            buf.extend(value.encode(charset)?);
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_direction() {
        assert_eq!(BindDirection::In.code(), 0x01);
        assert_eq!(BindDirection::Out.code(), 0x02);
        assert_eq!(BindDirection::InOut.code(), 0x03);

        assert_eq!(BindDirection::from_code(0x01), Some(BindDirection::In));
        assert_eq!(BindDirection::from_code(0x02), Some(BindDirection::Out));
        assert_eq!(BindDirection::from_code(0x03), Some(BindDirection::InOut));
        assert_eq!(BindDirection::from_code(0x00), None);
    }

    #[test]
    fn test_bind_descriptor() {
        let desc = BindDescriptor::varchar2(100).with_name(":name").with_direction(BindDirection::In);

        assert_eq!(desc.data_type, OracleDataType::Varchar2);
        assert_eq!(desc.max_size, 100);
        assert_eq!(desc.name, Some(":name".to_string()));
        assert_eq!(desc.direction, BindDirection::In);
    }

    #[test]
    fn test_bind_value_null() {
        let val = BindValue::null();
        assert!(val.is_null());

        let (indicator, data) = val.encode(CharsetId::AL32UTF8);
        assert_eq!(indicator, -1);
        assert!(data.is_empty());
    }

    #[test]
    fn test_bind_value_string() {
        let val = BindValue::from_string("hello");
        assert!(!val.is_null());

        let (indicator, data) = val.encode(CharsetId::AL32UTF8);
        assert_eq!(indicator, 0);
        assert_eq!(data[0], 5); // Length byte
        assert_eq!(&data[1..], b"hello");
    }

    #[test]
    fn test_bind_value_number() {
        let val = BindValue::from_i64(42);
        assert!(!val.is_null());
        assert_eq!(val.data_type(), OracleDataType::Number);
    }

    #[test]
    fn test_bind_set() {
        let mut binds = BindSet::new();
        binds.add_string(Some(":name"), "Alice", 100);
        binds.add_i64(Some(":id"), 42);
        binds.add_null(Some(":optional"), OracleDataType::Varchar2);

        assert_eq!(binds.len(), 3);

        let (desc, val) = binds.get(0).unwrap();
        assert_eq!(desc.name, Some(":name".to_string()));
        assert!(!val.is_null());

        let (_, val) = binds.get(2).unwrap();
        assert!(val.is_null());
    }

    #[test]
    fn test_encode_with_length() {
        // Short form
        let data = b"hello";
        let encoded = encode_with_length(data);
        assert_eq!(encoded[0], 5);
        assert_eq!(&encoded[1..], data);

        // Medium form (would need 254+ bytes)
        let data = vec![0u8; 300];
        let encoded = encode_with_length(&data);
        assert_eq!(encoded[0], 0xFE);
        assert_eq!(u16::from_be_bytes([encoded[1], encoded[2]]), 300);
    }

    #[test]
    fn test_array_bind_type() {
        assert_eq!(ArrayBindType::IndexByTable.code(), 0x01);
        assert_eq!(ArrayBindType::Varray.code(), 0x6E);
        assert_eq!(ArrayBindType::NestedTable.code(), 0x6F);

        assert_eq!(ArrayBindType::from_code(0x01), Some(ArrayBindType::IndexByTable));
        assert_eq!(ArrayBindType::from_code(0x6E), Some(ArrayBindType::Varray));
        assert_eq!(ArrayBindType::from_code(0x6F), Some(ArrayBindType::NestedTable));
    }

    #[test]
    fn test_array_bind_descriptor() {
        let desc = ArrayBindDescriptor::varchar2_array(100, 50).with_name(":names").as_varray();

        assert_eq!(desc.element_type, OracleDataType::Varchar2);
        assert_eq!(desc.max_element_size, 100);
        assert_eq!(desc.max_array_size, 50);
        assert_eq!(desc.array_type, ArrayBindType::Varray);
        assert_eq!(desc.name, Some(":names".to_string()));
    }

    #[test]
    fn test_array_bind_value_from_strings() {
        let values = ArrayBindValue::from_strings(vec![Some("a".to_string()), None, Some("c".to_string())]);

        assert_eq!(values.len(), 3);
        assert_eq!(values.is_null(0), Some(false));
        assert_eq!(values.is_null(1), Some(true));
        assert_eq!(values.is_null(2), Some(false));
    }

    #[test]
    fn test_array_bind_value_from_i64s() {
        let values = ArrayBindValue::from_i64s(vec![Some(1), Some(2), None, Some(4)]);

        assert_eq!(values.len(), 4);
        assert_eq!(values.is_null(2), Some(true));
        assert_eq!(values.element_data_type(), OracleDataType::Number);
    }

    #[test]
    fn test_array_bind_value_encode() {
        let values = ArrayBindValue::from_i64s(vec![Some(1), None, Some(3)]);
        let encoded = values.encode(CharsetId::AL32UTF8).unwrap();

        // Should have: length (4 bytes) + bitmap (1 byte) + data
        assert!(encoded.len() >= 5);
        assert_eq!(u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]), 3);
    }

    #[test]
    fn test_array_bind_set() {
        let mut binds = ArrayBindSet::new();

        binds.add_strings(Some(":names"), vec![Some("Alice".to_string()), Some("Bob".to_string())], 100).unwrap();

        binds.add_i64s(Some(":ids"), vec![Some(1), Some(2)]).unwrap();

        assert_eq!(binds.len(), 2);
        assert_eq!(binds.batch_size(), 2);
    }

    #[test]
    fn test_array_bind_set_size_mismatch() {
        let mut binds = ArrayBindSet::new();

        binds.add_i64s(Some(":a"), vec![Some(1), Some(2), Some(3)]).unwrap();

        let result = binds.add_i64s(Some(":b"), vec![Some(1), Some(2)]);
        assert!(matches!(result, Err(BindError::InconsistentArraySize { .. })));
    }

    #[test]
    fn test_array_bind_value_push() {
        let mut values = ArrayBindValue::new();
        values.push(BindValue::from_i64(1));
        values.push_null();
        values.push(BindValue::from_i64(3));

        assert_eq!(values.len(), 3);
        assert!(!values.is_empty());

        let collected: Vec<_> = values.iter_with_nulls().collect();
        assert_eq!(collected.len(), 3);
        assert!(collected[1].1); // null indicator
    }
}
