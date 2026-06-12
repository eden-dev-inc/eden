use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// PostgreSQL SQL type identifiers.
///
/// Used by the `query_typed` API to specify explicit parameter types
/// when the server cannot infer them.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum SqlType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Varchar,
    Bytea,
    Json,
    Jsonb,
}

impl SqlType {
    /// PG type OID for this SQL type.
    pub fn type_oid(&self) -> i32 {
        match self {
            SqlType::Bool => 16,
            SqlType::Int2 => 21,
            SqlType::Int4 => 23,
            SqlType::Int8 => 20,
            SqlType::Float4 => 700,
            SqlType::Float8 => 701,
            SqlType::Text => 25,
            SqlType::Varchar => 1043,
            SqlType::Bytea => 17,
            SqlType::Json => 114,
            SqlType::Jsonb => 3802,
        }
    }
}

/// A SQL parameter value for parameterized queries.
///
/// Values are sent to PostgreSQL in text format via the extended query protocol.
/// Each variant can be converted to its text representation with `to_pg_text()`.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum SqlParam {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl SqlParam {
    pub fn from_handlebars_type(placeholder: &str) -> Result<SqlParam, EpError> {
        if let Some(type_part) = placeholder.strip_prefix("{{").and_then(|s| s.strip_suffix("}}")) {
            let parts: Vec<&str> = type_part.split(':').collect();
            if parts.len() == 2 {
                let field_type = parts[1].trim();
                match field_type {
                    "Number" => Ok(SqlParam::Float8(0.0)), // placeholder value
                    "String" => Ok(SqlParam::Text(String::new())),
                    "Boolean" => Ok(SqlParam::Bool(false)),
                    "Object" => Ok(SqlParam::Json(serde_json::Value::Object(Default::default()))),
                    "Array" => Ok(SqlParam::Json(serde_json::Value::Array(Vec::new()))),
                    _ => Ok(SqlParam::Text(String::new())), // default fallback
                }
            } else {
                Ok(SqlParam::Text(String::new()))
            }
        } else {
            Ok(SqlParam::Text(String::new()))
        }
    }

    /// Convert this parameter to its PostgreSQL text-format representation.
    ///
    /// Returns `None` for SQL NULL (sent as -1 length in the wire protocol).
    /// Returns `Some(text)` for all other values.
    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            SqlParam::Null => None,
            SqlParam::Bool(v) => Some(if *v { "t".to_string() } else { "f".to_string() }),
            SqlParam::Int2(v) => Some(v.to_string()),
            SqlParam::Int4(v) => Some(v.to_string()),
            SqlParam::Int8(v) => Some(v.to_string()),
            SqlParam::Float4(v) => Some(v.to_string()),
            SqlParam::Float8(v) => Some(v.to_string()),
            SqlParam::Text(v) => Some(v.clone()),
            SqlParam::Bytes(v) => {
                // Bytea hex format: \x followed by hex pairs
                let mut hex = String::with_capacity(2 + v.len() * 2);
                hex.push_str("\\x");
                for byte in v {
                    hex.push_str(&format!("{byte:02x}"));
                }
                Some(hex)
            }
            SqlParam::Json(v) => Some(v.to_string()),
        }
    }
}

impl From<bool> for SqlParam {
    fn from(b: bool) -> Self {
        SqlParam::Bool(b)
    }
}

impl From<i16> for SqlParam {
    fn from(i: i16) -> Self {
        SqlParam::Int2(i)
    }
}

impl From<i32> for SqlParam {
    fn from(i: i32) -> Self {
        SqlParam::Int4(i)
    }
}

impl From<i64> for SqlParam {
    fn from(i: i64) -> Self {
        SqlParam::Int8(i)
    }
}

impl From<f32> for SqlParam {
    fn from(f: f32) -> Self {
        SqlParam::Float4(f)
    }
}

impl From<f64> for SqlParam {
    fn from(f: f64) -> Self {
        SqlParam::Float8(f)
    }
}

impl From<String> for SqlParam {
    fn from(s: String) -> Self {
        Self::from_handlebars_type(&s).unwrap_or_else(|_| SqlParam::Text(s.to_string()))
    }
}

impl From<&str> for SqlParam {
    fn from(s: &str) -> Self {
        Self::from_handlebars_type(s).unwrap_or_else(|_| SqlParam::Text(s.to_string()))
    }
}

impl From<Vec<u8>> for SqlParam {
    fn from(b: Vec<u8>) -> Self {
        SqlParam::Bytes(b)
    }
}

impl From<&[u8]> for SqlParam {
    fn from(b: &[u8]) -> Self {
        SqlParam::Bytes(b.to_vec())
    }
}

impl From<serde_json::Value> for SqlParam {
    fn from(v: serde_json::Value) -> Self {
        SqlParam::Json(v)
    }
}
