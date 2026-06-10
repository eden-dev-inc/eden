#![allow(unexpected_cfgs)]
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use oracle_client::sql_type::OracleType;
use oracle_client::{Row, SqlValue, Version};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;

// Keep the original OracleRowsOutput for backward compatibility
#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct OracleRowsOutput(pub Vec<RowWrapper>);

impl ToOutput for OracleRowsOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

// Keep the original OracleRowsOutput for backward compatibility
#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct OracleRowOutput(pub RowWrapper);

impl ToOutput for OracleRowOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

// impl ToSchema for OracleRowsOutput {}
//
// impl PartialSchema for OracleRowsOutput {
//     fn schema() -> RefOr<Schema> {
//         RefOr::T(Schema::OneOf(
//             OneOfBuilder::new()
//                 .item(Schema::Object(Object::default()))
//                 .item(Schema::Array(Array::default()))
//                 .build(),
//         ))
//     }
// }

// Serializable wrapper for Oracle Row
#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct RowWrapper {
    pub columns: Vec<ColumnWrapper>,
    pub values: HashMap<String, SerializableValue>,
}

#[derive(Debug, Clone, Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct ColumnWrapper {
    pub name: String,
    pub oracle_type: String,
    pub nullable: bool,
    pub precision: Option<u8>,
    pub scale: Option<i8>,
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[serde(tag = "type", content = "value")]
pub enum SerializableValue {
    Null,
    String(String),
    Integer(i64),
    UnsignedInteger(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
    Timestamp(String), // ISO 8601 format
    IntervalDS(String),
    IntervalYM(String),
    // Add more types as needed for your use case
}

#[allow(dead_code)]
impl RowWrapper {
    pub fn from_oracle_rows(rows: Vec<Row>) -> ResultEP<Vec<Self>> {
        let mut new_rows = vec![];
        for row in rows {
            new_rows.push(RowWrapper::from_oracle_row(&row)?)
        }

        Ok(new_rows)
    }
    /// Convert an Oracle Row to a serializable RowWrapper
    pub fn from_oracle_row(row: &Row) -> ResultEP<Self> {
        let column_info = row.column_info();
        let sql_values = row.sql_values();

        let mut columns = Vec::new();
        let mut values = HashMap::new();

        for (i, info) in column_info.iter().enumerate() {
            // Extract precision and scale from OracleType
            let (precision, scale) = match info.oracle_type() {
                OracleType::Number(prec, scale) => (Some(*prec), Some(*scale)),
                OracleType::Float(prec) => (Some(*prec), None),
                OracleType::Varchar2(size) => (Some(*size as u8), None),
                OracleType::Char(size) => (Some(*size as u8), None),
                OracleType::Timestamp(prec) => (Some(*prec), None),
                OracleType::TimestampTZ(prec) => (Some(*prec), None),
                OracleType::TimestampLTZ(prec) => (Some(*prec), None),
                OracleType::IntervalDS(day_prec, sec_prec) => (Some(*day_prec), Some(*sec_prec as i8)),
                OracleType::IntervalYM(prec) => (Some(*prec), None),
                _ => (None, None),
            };

            // Convert column info
            let column = ColumnWrapper {
                name: info.name().to_string(),
                oracle_type: format!("{:?}", info.oracle_type()),
                nullable: info.nullable(),
                precision,
                scale,
            };
            columns.push(column);

            // Convert the value
            let serializable_value = if i < sql_values.len() {
                convert_sql_value(&sql_values[i])?
            } else {
                SerializableValue::Null
            };

            values.insert(info.name().to_string(), serializable_value);
        }

        Ok(RowWrapper { columns, values })
    }
    /// Get a value by column name
    pub fn get_value(&self, column_name: &str) -> Option<&SerializableValue> {
        self.values.get(column_name)
    }

    /// Get a string value by column name
    pub fn get_string(&self, column_name: &str) -> Option<String> {
        match self.get_value(column_name)? {
            SerializableValue::String(s) => Some(s.clone()),
            SerializableValue::Integer(i) => Some(i.to_string()),
            SerializableValue::UnsignedInteger(u) => Some(u.to_string()),
            SerializableValue::Float(f) => Some(f.to_string()),
            SerializableValue::Double(d) => Some(d.to_string()),
            SerializableValue::Boolean(b) => Some(b.to_string()),
            SerializableValue::Timestamp(t) => Some(t.clone()),
            SerializableValue::IntervalDS(i) => Some(i.clone()),
            SerializableValue::IntervalYM(i) => Some(i.clone()),
            SerializableValue::Null => None,
            _ => None,
        }
    }

    /// Get an integer value by column name
    pub fn get_i64(&self, column_name: &str) -> Option<i64> {
        match self.get_value(column_name)? {
            SerializableValue::Integer(i) => Some(*i),
            SerializableValue::UnsignedInteger(u) => Some(*u as i64),
            SerializableValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get an unsigned integer value by column name
    pub fn get_u64(&self, column_name: &str) -> Option<u64> {
        match self.get_value(column_name)? {
            SerializableValue::UnsignedInteger(u) => Some(*u),
            SerializableValue::Integer(i) => Some(*i as u64),
            SerializableValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get a float value by column name
    pub fn get_f32(&self, column_name: &str) -> Option<f32> {
        match self.get_value(column_name)? {
            SerializableValue::Float(f) => Some(*f),
            SerializableValue::Double(d) => Some(*d as f32),
            SerializableValue::Integer(i) => Some(*i as f32),
            SerializableValue::UnsignedInteger(u) => Some(*u as f32),
            SerializableValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get a double value by column name
    pub fn get_f64(&self, column_name: &str) -> Option<f64> {
        match self.get_value(column_name)? {
            SerializableValue::Double(d) => Some(*d),
            SerializableValue::Float(f) => Some(*f as f64),
            SerializableValue::Integer(i) => Some(*i as f64),
            SerializableValue::UnsignedInteger(u) => Some(*u as f64),
            SerializableValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get a boolean value by column name
    pub fn get_bool(&self, column_name: &str) -> Option<bool> {
        match self.get_value(column_name)? {
            SerializableValue::Boolean(b) => Some(*b),
            SerializableValue::Integer(i) => Some(*i != 0),
            SerializableValue::UnsignedInteger(u) => Some(*u != 0),
            SerializableValue::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => Some(true),
                "false" | "0" | "no" | "n" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Get bytes value by column name
    pub fn get_bytes(&self, column_name: &str) -> Option<Vec<u8>> {
        match self.get_value(column_name)? {
            SerializableValue::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}

/// Convert SqlValue to SerializableValue
/// This function uses the actual SqlValue methods from the oracle crate
fn convert_sql_value(sql_value: &SqlValue) -> ResultEP<SerializableValue> {
    // Check if the value is null first
    if sql_value.is_null().map_err(EpError::serde)? {
        return Ok(SerializableValue::Null);
    }

    // Use the SqlValue's oracle_type to determine the best conversion
    let oracle_type = sql_value.oracle_type().map_err(EpError::serde)?;

    // Convert based on Oracle type for the most accurate representation
    match oracle_type.to_string().as_str() {
        // Numeric types
        s if s.starts_with("NUMBER") => {
            // Try integer first, then float
            if let Ok(i) = sql_value.get::<i64>() {
                Ok(SerializableValue::Integer(i))
            } else if let Ok(f) = sql_value.get::<f64>() {
                Ok(SerializableValue::Double(f))
            } else {
                // Fallback to string for very large numbers
                Ok(SerializableValue::String(sql_value.get::<String>().map_err(EpError::serde)?))
            }
        }
        "BINARY_INTEGER" | "PLS_INTEGER" => Ok(SerializableValue::Integer(sql_value.get::<i64>().map_err(EpError::serde)?)),
        "BINARY_FLOAT" => Ok(SerializableValue::Float(sql_value.get::<f32>().map_err(EpError::serde)?)),
        "BINARY_DOUBLE" => Ok(SerializableValue::Double(sql_value.get::<f64>().map_err(EpError::serde)?)),

        // String types
        s if s.starts_with("VARCHAR") || s.starts_with("CHAR") || s.starts_with("NVARCHAR") || s.starts_with("NCHAR") => {
            Ok(SerializableValue::String(sql_value.get::<String>().map_err(EpError::serde)?))
        }

        // LOB types
        "CLOB" | "NCLOB" => Ok(SerializableValue::String(sql_value.get::<String>().map_err(EpError::serde)?)),
        "BLOB" => Ok(SerializableValue::Bytes(sql_value.get::<Vec<u8>>().map_err(EpError::serde)?)),

        // Raw bytes
        s if s.starts_with("RAW") => Ok(SerializableValue::Bytes(sql_value.get::<Vec<u8>>().map_err(EpError::serde)?)),

        // Date/Time types
        "DATE" => Ok(SerializableValue::Timestamp(sql_value.get::<String>().map_err(EpError::serde)?)),
        s if s.starts_with("TIMESTAMP") => Ok(SerializableValue::Timestamp(sql_value.get::<String>().map_err(EpError::serde)?)),
        s if s.starts_with("INTERVAL DAY") => Ok(SerializableValue::IntervalDS(sql_value.get::<String>().map_err(EpError::serde)?)),
        s if s.starts_with("INTERVAL YEAR") => Ok(SerializableValue::IntervalYM(sql_value.get::<String>().map_err(EpError::serde)?)),

        // Boolean (PL/SQL only)
        "BOOLEAN" => Ok(SerializableValue::Boolean(sql_value.get::<bool>().map_err(EpError::serde)?)),

        // Fallback: try common types in order
        _ => {
            // Try to convert to the most appropriate type
            if let Ok(s) = sql_value.get::<String>() {
                Ok(SerializableValue::String(s))
            } else if let Ok(i) = sql_value.get::<i64>() {
                Ok(SerializableValue::Integer(i))
            } else if let Ok(f) = sql_value.get::<f64>() {
                Ok(SerializableValue::Double(f))
            } else if let Ok(b) = sql_value.get::<Vec<u8>>() {
                Ok(SerializableValue::Bytes(b))
            } else {
                // Last resort: use Display implementation
                Ok(SerializableValue::String(format!("{}", sql_value)))
            }
        }
    }
}

// Example usage and helper functions
#[allow(dead_code)]
impl RowWrapper {
    /// Serialize to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Serialize to pretty JSON string
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize to MessagePack
    #[allow(unexpected_cfgs)]
    #[cfg(feature = "msgpack")]
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack
    #[allow(unexpected_cfgs)]
    #[cfg(feature = "msgpack")]
    pub fn from_msgpack(data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(data)
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Check if a column exists
    pub fn has_column(&self, column_name: &str) -> bool {
        self.values.contains_key(column_name)
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if a specific column value is null
    pub fn is_null(&self, column_name: &str) -> bool {
        matches!(self.get_value(column_name), Some(SerializableValue::Null))
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct EmptyOutput(pub ());

impl ToOutput for EmptyOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    #[allow(clippy::unit_arg)]
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct ServerVersionOutput(pub (VersionWrapper, String));

impl ToOutput for ServerVersionOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct VersionWrapper {
    major: i32,
    minor: i32,
    update: i32,
    patch: i32,
    port_update: i32,
}

impl From<Version> for VersionWrapper {
    fn from(v: Version) -> Self {
        Self {
            major: v.major(),
            minor: v.minor(),
            update: v.update(),
            patch: v.patch(),
            port_update: v.port_update(),
        }
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct StringOutput(pub String);

impl ToOutput for StringOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct U64Output(pub u64);

impl ToOutput for U64Output {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct BoolOutput(pub bool);

impl ToOutput for BoolOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Oracle, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum StatementTypeWrapper {
    /// SELECT statement
    Select,

    /// INSERT statement
    Insert,

    /// UPDATE statement
    Update,

    /// DELETE statement
    Delete,

    /// [MERGE][] statement
    ///
    /// [MERGE]: https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-5692CCB7-24D9-4C0E-81A7-A22436DC968F
    Merge,

    /// CREATE statement
    Create,

    /// ALTER statement
    Alter,

    /// DROP statement
    Drop,

    /// PL/SQL statement without declare clause
    Begin,

    /// PL/SQL statement with declare clause
    Declare,

    /// COMMIT statement
    Commit,

    /// ROLLBACK statement
    Rollback,

    /// [EXPLAIN PLAN][] statement
    ///
    /// [EXPLAIN PLAN]: https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-FD540872-4ED3-4936-96A2-362539931BA0
    ExplainPlan,

    /// [CALL][] statement
    ///
    /// [CALL]: https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-6CD7B9C4-E5DC-4F3C-9B6A-876AD2C63545
    Call,

    /// Unknown statement
    Unknown,
}
