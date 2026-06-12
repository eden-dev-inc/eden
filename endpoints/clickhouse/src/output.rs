use chrono::{DateTime, NaiveDate, Utc};
use clickhouse_client::Row;
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) struct ClickhouseValueOutput(pub Value);

impl ToOutput for ClickhouseValueOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Clickhouse, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(self.0.to_owned())
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

#[derive(Debug)]
pub enum ClickhouseError {
    TypeConversion(String),
    InvalidFormat(String),
    SerdeError(serde_json::Error),
}

impl Display for ClickhouseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClickhouseError::TypeConversion(msg) => write!(f, "Type conversion error: {}", msg),
            ClickhouseError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            ClickhouseError::SerdeError(err) => write!(f, "Serde error: {}", err),
        }
    }
}

impl Error for ClickhouseError {}

impl From<serde_json::Error> for ClickhouseError {
    fn from(err: serde_json::Error) -> Self {
        ClickhouseError::SerdeError(err)
    }
}

impl From<&str> for ClickhouseError {
    fn from(msg: &str) -> Self {
        ClickhouseError::InvalidFormat(msg.to_string())
    }
}

// Enhanced type for Clickhouse row
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct ClickhouseRow(Vec<(String, JsonValue)>);
// {
// Use HashMap to store column name -> value mappings
// Maps are not supported by Clickhouse, we use a Vec of key-value pairs
// values: Vec<(String, JsonValue)>,
// }

impl Deref for ClickhouseRow {
    type Target = Vec<(String, JsonValue)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ClickhouseRow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<(String, JsonValue)>> for ClickhouseRow {
    fn from(value: Vec<(String, JsonValue)>) -> Self {
        Self(value)
    }
}

impl ClickhouseRow {
    pub fn from_json(json: &JsonValue) -> Result<Self, Box<dyn Error>> {
        let values = match json {
            JsonValue::Object(map) => {
                let mut values = Vec::new();
                for (key, value) in map {
                    values.push((key.clone(), value.clone()));
                }
                values
            }
            _ => return Err(ClickhouseError::from("JSON must be an object").into()),
        };

        Ok(Self(values))
    }

    pub fn from_json_str(json_str: &str) -> Result<Self, Box<dyn Error>> {
        let json: JsonValue = serde_json::from_str(json_str)?;
        Self::from_json(&json)
    }

    pub fn get(&self, column: &str) -> Option<&JsonValue> {
        self.iter().find(|(k, _)| k == column).map(|(_, v)| v)
    }

    pub fn set(&mut self, column: String, value: JsonValue) {
        self.iter_mut().find(|(k, _)| k == &column).map(|(_, v)| *v = value.clone()).or_else(|| {
            self.push((column.clone(), value.clone()));
            None
        });
    }

    pub fn get_columns(&self) -> Vec<String> {
        self.iter().map(|(k, _)| k.to_owned()).collect()
    }

    pub fn to_json(&self) -> JsonValue {
        JsonValue::Object(serde_json::Map::from_iter(self.iter().map(|(k, v)| (k.clone(), v.clone()))))
    }

    // Type-specific getters for convenience

    pub fn get_bool(&self, column: &str) -> Option<bool> {
        self.get(column).and_then(|v| v.as_bool())
    }

    pub fn get_i8(&self, column: &str) -> Option<i8> {
        self.get(column).and_then(|v| v.as_i64().map(|n| n as i8))
    }

    pub fn get_i16(&self, column: &str) -> Option<i16> {
        self.get(column).and_then(|v| v.as_i64().map(|n| n as i16))
    }

    pub fn get_i32(&self, column: &str) -> Option<i32> {
        self.get(column).and_then(|v| v.as_i64().map(|n| n as i32))
    }

    pub fn get_i64(&self, column: &str) -> Option<i64> {
        self.get(column).and_then(|v| v.as_i64())
    }

    pub fn get_u8(&self, column: &str) -> Option<u8> {
        self.get(column).and_then(|v| v.as_u64().map(|n| n as u8))
    }

    pub fn get_u16(&self, column: &str) -> Option<u16> {
        self.get(column).and_then(|v| v.as_u64().map(|n| n as u16))
    }

    pub fn get_u32(&self, column: &str) -> Option<u32> {
        self.get(column).and_then(|v| v.as_u64().map(|n| n as u32))
    }

    pub fn get_u64(&self, column: &str) -> Option<u64> {
        self.get(column).and_then(|v| v.as_u64())
    }

    pub fn get_f32(&self, column: &str) -> Option<f32> {
        self.get(column).and_then(|v| v.as_f64().map(|n| n as f32))
    }

    pub fn get_f64(&self, column: &str) -> Option<f64> {
        self.get(column).and_then(|v| v.as_f64())
    }

    pub fn get_string(&self, column: &str) -> Option<String> {
        self.get(column).and_then(|v| v.as_str().map(|s| s.to_string()))
    }

    pub fn get_uuid(&self, column: &str) -> Option<Uuid> {
        self.get(column).and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok())
    }

    pub fn get_ipv4(&self, column: &str) -> Option<Ipv4Addr> {
        self.get(column).and_then(|v| v.as_str()).and_then(|s| Ipv4Addr::from_str(s).ok())
    }

    pub fn get_ipv6(&self, column: &str) -> Option<Ipv6Addr> {
        self.get(column).and_then(|v| v.as_str()).and_then(|s| Ipv6Addr::from_str(s).ok())
    }

    pub fn get_date(&self, column: &str) -> Option<NaiveDate> {
        match self.get(column) {
            Some(JsonValue::String(date_str)) => NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok(),
            Some(JsonValue::Number(num)) => {
                let days = num.as_i64()?;
                // Assuming days since epoch (1970-01-01)
                NaiveDate::from_ymd_opt(1970, 1, 1).map(|epoch| epoch.checked_add_days(chrono::Days::new(days as u64)).unwrap_or_default())
            }
            _ => None,
        }
    }

    pub fn get_datetime(&self, column: &str) -> Option<DateTime<Utc>> {
        match self.get(column) {
            Some(JsonValue::String(dt_str)) => DateTime::parse_from_rfc3339(dt_str).ok().map(|dt| dt.with_timezone(&Utc)).or_else(|| {
                chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S"))
                    .ok()
                    .map(|dt| dt.and_utc())
            }),
            Some(JsonValue::Number(num)) => {
                let timestamp = num.as_i64()?;
                // Assuming seconds since epoch
                DateTime::<Utc>::from_timestamp(timestamp, 0)
            }
            _ => None,
        }
    }

    pub fn get_datetime64(&self, column: &str) -> Option<DateTime<Utc>> {
        match self.get(column) {
            Some(JsonValue::String(dt_str)) => DateTime::parse_from_rfc3339(dt_str).ok().map(|dt| dt.with_timezone(&Utc)).or_else(|| {
                chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S"))
                    .ok()
                    .map(|dt| dt.and_utc())
            }),
            Some(JsonValue::Number(num)) => {
                let timestamp_micros = num.as_i64()?;
                let seconds = timestamp_micros / 1_000_000;
                let nanos = ((timestamp_micros % 1_000_000) * 1000) as u32;
                DateTime::<Utc>::from_timestamp(seconds, nanos)
            }
            _ => None,
        }
    }

    pub fn get_array(&self, column: &str) -> Option<&Vec<JsonValue>> {
        self.get(column).and_then(|v| v.as_array())
    }

    pub fn get_map(&self, column: &str) -> Option<&serde_json::Map<String, JsonValue>> {
        self.get(column).and_then(|v| v.as_object())
    }

    pub fn get_geo_point(&self, column: &str) -> Option<(f64, f64)> {
        self.get(column).and_then(|v| {
            if let JsonValue::Array(coords) = v {
                if coords.len() == 2 {
                    let x = coords[0].as_f64()?;
                    let y = coords[1].as_f64()?;
                    Some((x, y))
                } else {
                    None
                }
            } else {
                None
            }
        })
    }
}

// Implement Row trait for ClickhouseRow
impl Row for ClickhouseRow {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

// Enhanced schema helper for type conversion
#[derive(Clone, Debug)]
pub enum ClickhouseType {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Float32,
    Float64,
    Decimal(u8, u8), // precision, scale
    Boolean,
    String,
    FixedString(usize),
    Enum(Vec<(String, i32)>),
    UUID,
    IPv4,
    IPv6,
    Date,
    Date32,
    DateTime(Option<String>),       // timezone
    DateTime64(u8, Option<String>), // precision, timezone
    Tuple(Vec<ClickhouseType>),
    Array(Box<ClickhouseType>),
    Map(Box<ClickhouseType>, Box<ClickhouseType>),
    Nullable(Box<ClickhouseType>),
    LowCardinality(Box<ClickhouseType>),
    Nested(Vec<(String, ClickhouseType)>),
    Geo(GeoType),
    Variant(Vec<ClickhouseType>),
    JSON,
    Unknown(String),
}

#[derive(Clone, Debug)]
pub enum GeoType {
    Point,
    Ring,
    Polygon,
    MultiPolygon,
}

pub struct ColumnSchema {
    name: String,
    column_type: ClickhouseType,
}

impl ColumnSchema {
    pub fn new(name: &str, column_type_str: &str) -> Result<Self, Box<dyn Error>> {
        let column_type = Self::parse_type(column_type_str)?;

        Ok(Self { name: name.to_string(), column_type })
    }

    // Parse Clickhouse type string into ClickhouseType enum
    fn parse_type(type_str: &str) -> Result<ClickhouseType, Box<dyn Error>> {
        // Handle simple types first
        let type_result = match type_str {
            "Int8" => ClickhouseType::Int8,
            "Int16" => ClickhouseType::Int16,
            "Int32" => ClickhouseType::Int32,
            "Int64" => ClickhouseType::Int64,
            "Int128" => ClickhouseType::Int128,
            "Int256" => ClickhouseType::Int256,
            "UInt8" => ClickhouseType::UInt8,
            "UInt16" => ClickhouseType::UInt16,
            "UInt32" => ClickhouseType::UInt32,
            "UInt64" => ClickhouseType::UInt64,
            "UInt128" => ClickhouseType::UInt128,
            "UInt256" => ClickhouseType::UInt256,
            "Float32" => ClickhouseType::Float32,
            "Float64" => ClickhouseType::Float64,
            "Boolean" | "Bool" => ClickhouseType::Boolean,
            "String" => ClickhouseType::String,
            "UUID" => ClickhouseType::UUID,
            "IPv4" => ClickhouseType::IPv4,
            "IPv6" => ClickhouseType::IPv6,
            "Date" => ClickhouseType::Date,
            "Date32" => ClickhouseType::Date32,
            "JSON" => ClickhouseType::JSON,
            // Complex types that need parsing
            _ => {
                // FixedString(N)
                if type_str.starts_with("FixedString(") && type_str.ends_with(')') {
                    let size_str = type_str.trim_start_matches("FixedString(").trim_end_matches(')');
                    let size = size_str.parse::<usize>()?;
                    ClickhouseType::FixedString(size)
                }
                // Decimal(P, S)
                else if type_str.starts_with("Decimal(") && type_str.ends_with(')') {
                    let params_str = type_str.trim_start_matches("Decimal(").trim_end_matches(')');
                    let params: Vec<&str> = params_str.split(',').map(|s| s.trim()).collect();
                    if params.len() == 2 {
                        let precision = params[0].parse::<u8>()?;
                        let scale = params[1].parse::<u8>()?;
                        ClickhouseType::Decimal(precision, scale)
                    } else {
                        return Err(format!("Invalid Decimal format: {}", type_str).into());
                    }
                }
                // DateTime('Timezone')
                else if type_str.starts_with("DateTime(") && type_str.ends_with(')') {
                    let tz_str = type_str.trim_start_matches("DateTime(").trim_end_matches(')');
                    let timezone = if tz_str.is_empty() {
                        None
                    } else {
                        // Remove quotes if present
                        Some(tz_str.trim_matches('\'').to_string())
                    };
                    ClickhouseType::DateTime(timezone)
                }
                // DateTime64(P, 'Timezone')
                else if type_str.starts_with("DateTime64(") && type_str.ends_with(')') {
                    let params_str = type_str.trim_start_matches("DateTime64(").trim_end_matches(')');
                    let params: Vec<&str> = params_str.split(',').map(|s| s.trim()).collect();

                    let precision = params[0].parse::<u8>()?;
                    let timezone = if params.len() > 1 {
                        Some(params[1].trim_matches('\'').to_string())
                    } else {
                        None
                    };

                    ClickhouseType::DateTime64(precision, timezone)
                }
                // Enum(...)
                else if type_str.starts_with("Enum(") || type_str.starts_with("Enum8(") || type_str.starts_with("Enum16(") {
                    // This is a simplification - proper parsing would require more complex logic
                    // to handle the exact format of enum values
                    ClickhouseType::Enum(vec![])
                }
                // Nullable(T)
                else if type_str.starts_with("Nullable(") && type_str.ends_with(')') {
                    let inner_type = type_str.trim_start_matches("Nullable(").trim_end_matches(')');
                    let inner = Self::parse_type(inner_type)?;
                    ClickhouseType::Nullable(Box::new(inner))
                }
                // LowCardinality(T)
                else if type_str.starts_with("LowCardinality(") && type_str.ends_with(')') {
                    let inner_type = type_str.trim_start_matches("LowCardinality(").trim_end_matches(')');
                    let inner = Self::parse_type(inner_type)?;
                    ClickhouseType::LowCardinality(Box::new(inner))
                }
                // Array(T)
                else if type_str.starts_with("Array(") && type_str.ends_with(')') {
                    let inner_type = type_str.trim_start_matches("Array(").trim_end_matches(')');
                    let inner = Self::parse_type(inner_type)?;
                    ClickhouseType::Array(Box::new(inner))
                }
                // Map(K, V)
                else if type_str.starts_with("Map(") && type_str.ends_with(')') {
                    let params_str = type_str.trim_start_matches("Map(").trim_end_matches(')');
                    // Simple splitting by comma is not sufficient for nested types
                    // This is a simplified version
                    let split_idx = params_str.find(',').ok_or("Invalid Map format")?;
                    let key_type = Self::parse_type(params_str[..split_idx].trim())?;
                    let value_type = Self::parse_type(params_str[split_idx + 1..].trim())?;

                    ClickhouseType::Map(Box::new(key_type), Box::new(value_type))
                }
                // Tuple(...)
                else if type_str.starts_with("Tuple(") && type_str.ends_with(')') {
                    let inner_types_str = type_str.trim_start_matches("Tuple(").trim_end_matches(')');
                    // Simple splitting - this would need to be more sophisticated for real use
                    let inner_types: Result<Vec<ClickhouseType>, Box<dyn Error>> =
                        inner_types_str.split(',').map(|s| Self::parse_type(s.trim())).collect();

                    ClickhouseType::Tuple(inner_types?)
                }
                // Geo types
                else if type_str == "Point" {
                    ClickhouseType::Geo(GeoType::Point)
                } else if type_str == "Ring" {
                    ClickhouseType::Geo(GeoType::Ring)
                } else if type_str == "Polygon" {
                    ClickhouseType::Geo(GeoType::Polygon)
                } else if type_str == "MultiPolygon" {
                    ClickhouseType::Geo(GeoType::MultiPolygon)
                }
                // Variant(...)
                else if type_str.starts_with("Variant(") && type_str.ends_with(')') {
                    let inner_types_str = type_str.trim_start_matches("Variant(").trim_end_matches(')');
                    // Simplified parsing
                    let inner_types: Result<Vec<ClickhouseType>, Box<dyn Error>> =
                        inner_types_str.split(',').map(|s| Self::parse_type(s.trim())).collect();

                    ClickhouseType::Variant(inner_types?)
                }
                // Default case for unknown types
                else {
                    ClickhouseType::Unknown(type_str.to_string())
                }
            }
        };

        Ok(type_result)
    }

    // Convert JSON value to appropriate ClickHouse type
    pub fn convert_value(&self, value: &JsonValue) -> Result<JsonValue, Box<dyn Error>> {
        if value.is_null() && !matches!(self.column_type, ClickhouseType::Nullable(_)) {
            return Err(format!("Null value not allowed for non-Nullable type: {}", self.name).into());
        }

        match &self.column_type {
            ClickhouseType::Int8 | ClickhouseType::Int16 | ClickhouseType::Int32 | ClickhouseType::Int64 => {
                if let Some(num) = value.as_i64() {
                    Ok(json!(num))
                } else {
                    Err(format!("Cannot convert {:?} to integer type", value).into())
                }
            }
            ClickhouseType::UInt8 | ClickhouseType::UInt16 | ClickhouseType::UInt32 | ClickhouseType::UInt64 => {
                if let Some(num) = value.as_u64() {
                    Ok(json!(num))
                } else {
                    Err(format!("Cannot convert {:?} to unsigned integer type", value).into())
                }
            }
            ClickhouseType::Float32 | ClickhouseType::Float64 => {
                if let Some(num) = value.as_f64() {
                    Ok(json!(num))
                } else {
                    Err(format!("Cannot convert {:?} to float type", value).into())
                }
            }
            ClickhouseType::Decimal(_, _) => {
                // For simplicity, treating decimal as string or number
                if value.is_number() || value.is_string() {
                    Ok(value.clone())
                } else {
                    Err(format!("Cannot convert {:?} to Decimal type", value).into())
                }
            }
            ClickhouseType::Boolean => {
                if let Some(b) = value.as_bool() {
                    Ok(json!(b))
                } else {
                    Err(format!("Cannot convert {:?} to Boolean type", value).into())
                }
            }
            ClickhouseType::String => {
                if let Some(s) = value.as_str() {
                    Ok(json!(s))
                } else {
                    // Convert any type to string
                    Ok(json!(value.to_string()))
                }
            }
            ClickhouseType::FixedString(_) => {
                if let Some(s) = value.as_str() {
                    Ok(json!(s))
                } else {
                    Err(format!("Cannot convert {:?} to FixedString type", value).into())
                }
            }
            ClickhouseType::UUID => {
                if let Some(s) = value.as_str() {
                    // Validate UUID format
                    let _ = Uuid::parse_str(s)?;
                    Ok(json!(s))
                } else {
                    Err(format!("Cannot convert {:?} to UUID type", value).into())
                }
            }
            ClickhouseType::IPv4 => {
                if let Some(s) = value.as_str() {
                    // Validate IPv4 format
                    let _ = Ipv4Addr::from_str(s)?;
                    Ok(json!(s))
                } else {
                    Err(format!("Cannot convert {:?} to IPv4 type", value).into())
                }
            }
            ClickhouseType::IPv6 => {
                if let Some(s) = value.as_str() {
                    // Validate IPv6 format
                    let _ = Ipv6Addr::from_str(s)?;
                    Ok(json!(s))
                } else {
                    Err(format!("Cannot convert {:?} to IPv6 type", value).into())
                }
            }
            ClickhouseType::Date => {
                match value {
                    JsonValue::String(s) => {
                        // Validate date format (YYYY-MM-DD)
                        let _ = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
                        Ok(value.clone())
                    }
                    JsonValue::Number(n) => {
                        // Number of days since epoch
                        if let Some(days) = n.as_u64() {
                            Ok(json!(days))
                        } else {
                            Err(format!("Cannot convert {:?} to Date type", value).into())
                        }
                    }
                    _ => Err(format!("Cannot convert {:?} to Date type", value).into()),
                }
            }
            ClickhouseType::Date32 => {
                match value {
                    JsonValue::String(s) => {
                        // Validate date format (YYYY-MM-DD)
                        let _ = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
                        Ok(value.clone())
                    }
                    JsonValue::Number(n) => {
                        // Number of days since epoch (i32)
                        if let Some(days) = n.as_i64() {
                            if days >= i32::MIN as i64 && days <= i32::MAX as i64 {
                                Ok(json!(days))
                            } else {
                                Err(format!("Date32 value out of range: {}", days).into())
                            }
                        } else {
                            Err(format!("Cannot convert {:?} to Date32 type", value).into())
                        }
                    }
                    _ => Err(format!("Cannot convert {:?} to Date32 type", value).into()),
                }
            }
            ClickhouseType::DateTime(_) => {
                match value {
                    JsonValue::String(s) => {
                        // Validate datetime format
                        let _ = DateTime::parse_from_rfc3339(s)
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.and_utc().into()))?;
                        Ok(value.clone())
                    }
                    JsonValue::Number(n) => {
                        // Seconds since epoch
                        if let Some(seconds) = n.as_i64() {
                            Ok(json!(seconds))
                        } else {
                            Err(format!("Cannot convert {:?} to DateTime type", value).into())
                        }
                    }
                    _ => Err(format!("Cannot convert {:?} to DateTime type", value).into()),
                }
            }
            ClickhouseType::DateTime64(_, _) => {
                match value {
                    JsonValue::String(s) => {
                        // Validate datetime format with higher precision
                        let _ = DateTime::parse_from_rfc3339(s)
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.and_utc().into()))?;
                        Ok(value.clone())
                    }
                    JsonValue::Number(n) => {
                        // Microseconds since epoch
                        if let Some(micros) = n.as_i64() {
                            Ok(json!(micros))
                        } else {
                            Err(format!("Cannot convert {:?} to DateTime64 type", value).into())
                        }
                    }
                    _ => Err(format!("Cannot convert {:?} to DateTime64 type", value).into()),
                }
            }
            ClickhouseType::Array(inner_type) => {
                if let Some(arr) = value.as_array() {
                    let inner_schema = ColumnSchema { name: self.name.clone(), column_type: (**inner_type).clone() };

                    let mut result = Vec::new();
                    for elem in arr {
                        result.push(inner_schema.convert_value(elem)?);
                    }
                    Ok(json!(result))
                } else {
                    Err(format!("Cannot convert {:?} to Array type", value).into())
                }
            }
            ClickhouseType::Map(key_type, value_type) => {
                // Create schemas for the key and value types
                let _key_schema = ColumnSchema {
                    name: format!("{}_key", self.name),
                    column_type: (**key_type).clone(),
                };

                let value_schema = ColumnSchema {
                    name: format!("{}_value", self.name),
                    column_type: (**value_type).clone(),
                };

                if let Some(obj) = value.as_object() {
                    let mut result = serde_json::Map::new();
                    for (k, v) in obj {
                        // For simplicity, assuming keys are already valid strings
                        // In a more complete implementation, we might need to convert complex key types
                        let new_value = value_schema.convert_value(v)?;
                        result.insert(k.clone(), new_value);
                    }
                    Ok(JsonValue::Object(result))
                } else if let Some(arr) = value.as_array() {
                    // Map can also be represented as array of key-value pairs
                    // This is a simplification - would need more robust handling in practice
                    let mut result = serde_json::Map::new();
                    for pair in arr {
                        if let Some(pair_arr) = pair.as_array()
                            && pair_arr.len() == 2
                            && let Some(key) = pair_arr[0].as_str()
                        {
                            let new_value = value_schema.convert_value(&pair_arr[1])?;
                            result.insert(key.to_string(), new_value);
                        }
                    }
                    Ok(JsonValue::Object(result))
                } else {
                    Err(format!("Cannot convert {:?} to Map type", value).into())
                }
            }
            ClickhouseType::Tuple(inner_types) => {
                if let Some(arr) = value.as_array() {
                    if arr.len() != inner_types.len() {
                        return Err(format!("Tuple size mismatch: expected {}, got {}", inner_types.len(), arr.len()).into());
                    }

                    let mut result = Vec::new();
                    for (i, elem) in arr.iter().enumerate() {
                        if i < inner_types.len() {
                            let inner_schema = ColumnSchema {
                                name: format!("{}_{}", self.name, i),
                                column_type: inner_types[i].clone(),
                            };
                            result.push(inner_schema.convert_value(elem)?);
                        }
                    }
                    Ok(JsonValue::Array(result))
                } else {
                    Err(format!("Cannot convert {:?} to Tuple type", value).into())
                }
            }
            ClickhouseType::Nullable(inner_type) => {
                if value.is_null() {
                    Ok(JsonValue::Null)
                } else {
                    let inner_schema = ColumnSchema { name: self.name.clone(), column_type: (**inner_type).clone() };
                    inner_schema.convert_value(value)
                }
            }
            ClickhouseType::LowCardinality(inner_type) => {
                // LowCardinality is a storage optimization, so just convert the inner value
                let inner_schema = ColumnSchema { name: self.name.clone(), column_type: (**inner_type).clone() };
                inner_schema.convert_value(value)
            }
            ClickhouseType::Enum(_) => {
                // Simplified enum handling - in reality we'd need to validate against defined enum values
                match value {
                    JsonValue::String(_) => Ok(value.clone()),
                    JsonValue::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(json!(i))
                        } else {
                            Err(format!("Cannot convert {:?} to Enum type", value).into())
                        }
                    }
                    _ => Err(format!("Cannot convert {:?} to Enum type", value).into()),
                }
            }
            ClickhouseType::Geo(geo_type) => {
                match geo_type {
                    GeoType::Point => {
                        // Point is a tuple of two coordinates (x, y)
                        if let Some(arr) = value.as_array() {
                            if arr.len() == 2 && arr[0].is_number() && arr[1].is_number() {
                                Ok(value.clone())
                            } else {
                                Err(format!("Invalid Point format: {:?}", value).into())
                            }
                        } else {
                            Err(format!("Cannot convert {:?} to Point type", value).into())
                        }
                    }
                    GeoType::Ring | GeoType::Polygon | GeoType::MultiPolygon => {
                        // These are arrays of points or nested arrays
                        if value.is_array() {
                            // We'd need more validation here in a real implementation
                            Ok(value.clone())
                        } else {
                            Err(format!("Cannot convert {:?} to Geo type", value).into())
                        }
                    }
                }
            }
            ClickhouseType::Variant(inner_types) => {
                // Variant type handling
                if let Some(obj) = value.as_object() {
                    if obj.len() == 1 {
                        for (type_name, variant_value) in obj {
                            // Find the corresponding type by index from type name
                            if let Ok(index) = type_name.parse::<usize>()
                                && index < inner_types.len()
                            {
                                let inner_schema = ColumnSchema {
                                    name: format!("{}_{}", self.name, index),
                                    column_type: inner_types[index].clone(),
                                };

                                let converted_value = inner_schema.convert_value(variant_value)?;
                                let mut result = serde_json::Map::new();
                                result.insert(type_name.clone(), converted_value);
                                return Ok(JsonValue::Object(result));
                            }
                        }
                        Err("Invalid variant type index".into())
                    } else {
                        Err("Variant must contain exactly one field".into())
                    }
                } else {
                    Err(format!("Cannot convert {:?} to Variant type", value).into())
                }
            }
            ClickhouseType::JSON => {
                // JSON type - just pass through
                Ok(value.clone())
            }
            ClickhouseType::Nested(columns) => {
                // Nested columns are represented as multiple arrays
                if let Some(arr) = value.as_array() {
                    let mut result = serde_json::Map::new();

                    for (col_name, col_type) in columns {
                        let mut col_values = Vec::new();

                        for row in arr {
                            if let Some(row_obj) = row.as_object()
                                && let Some(col_value) = row_obj.get(col_name)
                            {
                                let inner_schema = ColumnSchema { name: col_name.clone(), column_type: col_type.clone() };

                                col_values.push(inner_schema.convert_value(col_value)?);
                            }
                        }

                        result.insert(col_name.clone(), JsonValue::Array(col_values));
                    }

                    Ok(JsonValue::Object(result))
                } else {
                    Err(format!("Cannot convert {:?} to Nested type", value).into())
                }
            }
            ClickhouseType::Int128 | ClickhouseType::Int256 | ClickhouseType::UInt128 | ClickhouseType::UInt256 => {
                // Handle these as strings since they exceed JavaScript number precision
                if let Some(s) = value.as_str() {
                    Ok(json!(s))
                } else if value.is_number() {
                    Ok(json!(value.to_string()))
                } else {
                    Err(format!("Cannot convert {:?} to large integer type", value).into())
                }
            }
            ClickhouseType::Unknown(_type_name) => {
                // Unknown type - just pass through
                Ok(value.clone())
            }
        }
    }
}

// Schema for a table
pub struct TableSchema {
    columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> Self {
        Self { columns }
    }

    // Fetch schema from the database
    pub async fn from_db(client: &clickhouse_client::Client, table: &str) -> Result<Self, Box<dyn Error>> {
        let query = format!("DESCRIBE TABLE {}", table);
        let rows: Vec<ClickhouseRow> = client.query(&query).fetch_all().await?;

        let mut columns = Vec::new();
        for row in rows {
            if let (Some(name), Some(type_value)) = (row.get("name"), row.get("type"))
                && let (Some(name_str), Some(type_str)) = (name.as_str(), type_value.as_str())
            {
                match ColumnSchema::new(name_str, type_str) {
                    Ok(column_schema) => columns.push(column_schema),
                    Err(e) => eprintln!("Error parsing column {}: {}", name_str, e),
                }
            }
        }

        Ok(Self { columns })
    }

    // Convert a dynamic row according to schema
    pub fn convert_row(&self, json: &JsonValue) -> Result<ClickhouseRow, Box<dyn Error>> {
        let mut row = ClickhouseRow::default();

        if let JsonValue::Object(map) = json {
            for column in &self.columns {
                if let Some(value) = map.get(&column.name) {
                    match column.convert_value(value) {
                        Ok(converted) => row.set(column.name.clone(), converted),
                        Err(e) => {
                            return Err(format!("Error converting column {}: {}", column.name, e).into());
                        }
                    }
                }
            }
        } else {
            return Err("JSON must be an object".into());
        }

        Ok(row)
    }

    // Get column schema by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    // Get all column names
    pub fn get_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

// Additional utility functions for working with Clickhouse data

// Create a row from a map of values
pub fn create_row_from_map(values: HashMap<String, JsonValue>) -> ClickhouseRow {
    let mut row = ClickhouseRow::default();
    for (key, value) in values {
        row.set(key, value);
    }
    row
}

// Convert a Vec<ClickhouseRow> to CSV format
pub fn rows_to_csv(rows: &[ClickhouseRow]) -> Result<String, Box<dyn Error>> {
    if rows.is_empty() {
        return Ok(String::new());
    }

    // Get column names from first row
    let columns = rows[0].get_columns();

    let mut csv = Vec::new();

    // Write header
    csv.push(columns.join(","));

    // Write rows
    for row in rows {
        let values: Vec<String> = columns
            .iter()
            .map(|col| match row.get(col) {
                Some(val) => match val {
                    JsonValue::String(s) => format!("\"{}\"", s.replace("\"", "\"\"")),
                    JsonValue::Null => "NULL".to_string(),
                    _ => val.to_string(),
                },
                None => "NULL".to_string(),
            })
            .collect();

        csv.push(values.join(","));
    }

    Ok(csv.join("\n"))
}

// Parse a DateTimeMicro value (for DateTime64)
pub fn parse_datetime_micro(dt_str: &str) -> Result<i64, Box<dyn Error>> {
    let dt = DateTime::parse_from_rfc3339(dt_str).or_else(|_| {
        // Try other common formats
        chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.and_utc().into())
    })?;

    // Convert to microseconds
    let secs = dt.timestamp();
    let micros = dt.timestamp_subsec_micros() as i64;

    Ok(secs * 1_000_000 + micros)
}

// Helper to convert system time to DateTime64 format
pub fn system_time_to_datetime64(time: SystemTime) -> Result<i64, Box<dyn Error>> {
    let duration = time.duration_since(UNIX_EPOCH)?;
    let secs = duration.as_secs() as i64;
    let micros = duration.subsec_micros() as i64;

    Ok(secs * 1_000_000 + micros)
}

// Format a value for Clickhouse INSERT query
pub fn format_value_for_insert(value: &JsonValue, column_type: &ClickhouseType) -> String {
    match (value, column_type) {
        (JsonValue::Null, _) => "NULL".to_string(),

        (_, ClickhouseType::String)
        | (_, ClickhouseType::FixedString(_))
        | (_, ClickhouseType::UUID)
        | (_, ClickhouseType::IPv4)
        | (_, ClickhouseType::IPv6) => {
            if let Some(s) = value.as_str() {
                format!("'{}'", s.replace("'", "''"))
            } else {
                format!("'{}'", value.to_string().replace("'", "''"))
            }
        }

        (_, ClickhouseType::Date) | (_, ClickhouseType::Date32) => {
            if let Some(s) = value.as_str() {
                format!("'{}'", s)
            } else if value.is_number() {
                value.to_string()
            } else {
                "NULL".to_string()
            }
        }

        (_, ClickhouseType::DateTime(_)) | (_, ClickhouseType::DateTime64(_, _)) => {
            if let Some(s) = value.as_str() {
                format!("'{}'", s)
            } else if value.is_number() {
                value.to_string()
            } else {
                "NULL".to_string()
            }
        }

        (_, ClickhouseType::Array(_)) | (_, ClickhouseType::Tuple(_)) => {
            if let Some(arr) = value.as_array() {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(|v| match v {
                            JsonValue::String(s) => format!("'{}'", s.replace("'", "''")),
                            JsonValue::Null => "NULL".to_string(),
                            _ => v.to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )
            } else {
                "[]".to_string()
            }
        }

        (_, ClickhouseType::Map(_, _)) => {
            if let Some(obj) = value.as_object() {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val_str = match v {
                            JsonValue::String(s) => format!("'{}'", s.replace("'", "''")),
                            JsonValue::Null => "NULL".to_string(),
                            _ => v.to_string(),
                        };
                        format!("'{}', {}", k.replace("'", "''"), val_str)
                    })
                    .collect();
                format!("map({})", items.join(","))
            } else {
                "map()".to_string()
            }
        }

        (_, _) => {
            if value.is_string() {
                format!("'{}'", value.as_str().unwrap_or_default().replace("'", "''"))
            } else {
                value.to_string()
            }
        }
    }
}
