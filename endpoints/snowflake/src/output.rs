use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::Value as JsonValue;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use utoipa::ToSchema;

pub(crate) struct SnowflakeValueOutput(pub Value);

impl ToOutput for SnowflakeValueOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Snowflake, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(self.0.to_owned())
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Snowflake"))
    }
}

#[derive(Debug)]
pub enum SnowflakeError {
    TypeConversion(String),
    InvalidFormat(String),
    SerdeError(serde_json::Error),
}

impl Display for SnowflakeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SnowflakeError::TypeConversion(msg) => write!(f, "Type conversion error: {}", msg),
            SnowflakeError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            SnowflakeError::SerdeError(err) => write!(f, "Serde error: {}", err),
        }
    }
}

impl Error for SnowflakeError {}

impl From<serde_json::Error> for SnowflakeError {
    fn from(err: serde_json::Error) -> Self {
        SnowflakeError::SerdeError(err)
    }
}

impl From<&str> for SnowflakeError {
    fn from(msg: &str) -> Self {
        SnowflakeError::InvalidFormat(msg.to_string())
    }
}

/// Row type for Snowflake query results.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct SnowflakeRow(Vec<(String, JsonValue)>);

impl Deref for SnowflakeRow {
    type Target = Vec<(String, JsonValue)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SnowflakeRow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<(String, JsonValue)>> for SnowflakeRow {
    fn from(value: Vec<(String, JsonValue)>) -> Self {
        Self(value)
    }
}

impl SnowflakeRow {
    pub fn from_json(json: &JsonValue) -> Result<Self, Box<dyn Error>> {
        let values = match json {
            JsonValue::Object(map) => {
                let mut values = Vec::new();
                for (key, value) in map {
                    values.push((key.clone(), value.clone()));
                }
                values
            }
            _ => return Err(SnowflakeError::from("JSON must be an object").into()),
        };

        Ok(Self(values))
    }

    pub fn from_json_str(json_str: &str) -> Result<Self, Box<dyn Error>> {
        let json: JsonValue = serde_json::from_str(json_str)?;
        Self::from_json(&json)
    }

    pub fn get(&self, column: &str) -> Option<&JsonValue> {
        self.iter().find(|(k, _v)| k == column).map(|(_, v)| v)
    }

    pub fn set(&mut self, column: String, value: JsonValue) {
        self.iter_mut().find(|(k, _v)| k == &column).map(|(_, v)| *v = value.clone()).or_else(|| {
            self.push((column.clone(), value.clone()));
            None
        });
    }

    pub fn get_columns(&self) -> Vec<String> {
        self.iter().map(|(k, _v)| k.to_owned()).collect()
    }

    pub fn to_json(&self) -> JsonValue {
        JsonValue::Object(serde_json::Map::from_iter(self.iter().map(|(k, v)| (k.clone(), v.clone()))))
    }

    // Type-specific getters for convenience

    pub fn get_bool(&self, column: &str) -> Option<bool> {
        self.get(column).and_then(|v| v.as_bool())
    }

    pub fn get_i64(&self, column: &str) -> Option<i64> {
        self.get(column).and_then(|v| v.as_i64())
    }

    pub fn get_u64(&self, column: &str) -> Option<u64> {
        self.get(column).and_then(|v| v.as_u64())
    }

    pub fn get_f64(&self, column: &str) -> Option<f64> {
        self.get(column).and_then(|v| v.as_f64())
    }

    pub fn get_string(&self, column: &str) -> Option<String> {
        self.get(column).and_then(|v| v.as_str().map(|s| s.to_string()))
    }

    pub fn get_array(&self, column: &str) -> Option<&Vec<JsonValue>> {
        self.get(column).and_then(|v| v.as_array())
    }

    pub fn get_object(&self, column: &str) -> Option<&serde_json::Map<String, JsonValue>> {
        self.get(column).and_then(|v| v.as_object())
    }
}
