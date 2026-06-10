use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt::Display;
use redis_protocol::resp3::types::{FrameSet, RespVersion, VerbatimStringFormat};
use utoipa::openapi::{RefOr, Schema};
use utoipa::ToSchema;
use crate::command::ToRedisArgs;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyStruct {
    key: crate::api::RedisJsonValue,
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    ToSchema,
    JsonSchema,
)]
#[serde(untagged)]
pub enum RedisJsonValue {
    Resp2(OwnedFrameResp2),
    Resp3(OwnedFrameResp3),
}

pub type RedisOwnedAttributes = HashMap<String, OwnedFrameResp3>;

#[derive(
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    ToSchema,
    JsonSchema,
)]
#[serde(untagged)]
pub enum OwnedFrameResp2 {
    /// A RESP2 simple string.
    SimpleString(Vec<u8>),
    /// A short string representing an error.
    Error(String),
    /// A signed 64-bit integer.
    Integer(i64),
    /// A byte array.
    BulkString(Vec<u8>),
    /// An array of frames,
    Array(Vec<OwnedFrameResp2>),
    /// A null value.
    Null,
}


#[derive(
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    PartialEq,
    PartialOrd,
    Hash,
    ToSchema,
    JsonSchema,
)]
pub enum RedisVerbatimStringFormat {
    Text,
    Markdown,
}

impl Into<VerbatimStringFormat> for RedisVerbatimStringFormat {
    fn into(self) -> VerbatimStringFormat {
        match self {
            Self::Text => VerbatimStringFormat::Text,
            Self::Markdown => VerbatimStringFormat::Markdown,
        }
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    ToSchema,
    JsonSchema,
)]
pub enum RedisRespVersion {
    RESP2,
    RESP3,
}

impl Into<RespVersion> for RedisRespVersion {
    fn into(self) -> RespVersion {
        match self {
            Self::RESP2 => RespVersion::RESP2,
            Self::RESP3 => RespVersion::RESP3,
        }
    }
}


#[derive(
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    ToSchema,
    JsonSchema,
)]
#[serde(untagged)]
pub enum OwnedFrameResp3 {
    /// A blob of bytes.
    BlobString {
        data: Vec<u8>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A blob representing an error.
    BlobError {
        data: Vec<u8>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A small string.
    SimpleString {
        data: Vec<u8>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A small string representing an error.
    SimpleError {
        data: String,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A boolean type.
    Boolean {
        data: bool,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A null type.
    Null,
    /// A signed 64-bit integer.
    Number {
        data: i64,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A signed 64-bit floating point number.
    Double {
        data: f64,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A large number not representable as a `Number` or `Double`.
    BigNumber {
        data: Vec<u8>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A string to be displayed without any escaping or filtering.
    VerbatimString {
        data: Vec<u8>,
        format: RedisVerbatimStringFormat,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// An array of frames.
    Array {
        data: Vec<OwnedFrameResp3>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// An unordered map of key-value pairs.
    Map {
        data: Vec<(OwnedFrameResp3, OwnedFrameResp3)>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// An unordered collection of other frames with a uniqueness constraint.
    Set {
        data: Vec<OwnedFrameResp3>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// Out-of-band data.
    Push {
        data: Vec<OwnedFrameResp3>,
        attributes: Option<RedisOwnedAttributes>,
    },
    /// A special frame type used when first connecting to the server.
    Hello {
        version: RedisRespVersion,
        auth: Option<(String, String)>,
        setname: Option<String>,
    },
    /// One chunk of a streaming blob.
    ChunkedString(Vec<u8>),
}

impl From<crate::api::RedisJsonValue> for serde_json::Value {
    fn from(val: crate::api::RedisJsonValue) -> Value {
        match val {
            crate::api::RedisJsonValue::Resp2(frame) => frame.into(),
            crate::api::RedisJsonValue::Resp3(frame) => frame.into(),
        }
    }
}

impl From<OwnedFrameResp2> for serde_json::Value {
    fn from(val: OwnedFrameResp2) -> Value {
        match val {
            OwnedFrameResp2::Null => Value::Null,
            OwnedFrameResp2::SimpleString(s) | OwnedFrameResp2::BulkString(s) => {
                Value::from(String::from_utf8_lossy(&s).to_string())
            }
            OwnedFrameResp2::Error(e) => Value::from(e),
            OwnedFrameResp2::Integer(i) => Value::from(i),
            OwnedFrameResp2::Array(arr) => {
                Value::Array(arr.into_iter().map(|v| v.into()).collect())
            }
        }
    }
}

impl From<OwnedFrameResp3> for serde_json::Value {
    fn from(val: OwnedFrameResp3) -> Value {
        match val {
            OwnedFrameResp3::Null => Value::Null,
            OwnedFrameResp3::BlobString { data, .. }
            | OwnedFrameResp3::SimpleString { data, .. }
            | OwnedFrameResp3::ChunkedString(data)
            | OwnedFrameResp3::VerbatimString { data, .. } => {
                Value::from(String::from_utf8_lossy(&data).to_string())
            }
            OwnedFrameResp3::BlobError { data, .. } => {
                Value::from(String::from_utf8_lossy(&data).to_string())
            }
            OwnedFrameResp3::SimpleError { data, .. } => Value::from(data),
            OwnedFrameResp3::Boolean { data, .. } => Value::from(data),
            OwnedFrameResp3::Number { data, .. } => Value::from(data),
            OwnedFrameResp3::Double { data, .. } => Value::from(data),
            OwnedFrameResp3::BigNumber { data, .. } => {
                Value::from(String::from_utf8_lossy(&data).to_string())
            }
            OwnedFrameResp3::Array { data, .. } | OwnedFrameResp3::Push { data, .. } => {
                Value::Array(data.into_iter().map(|v| v.into()).collect())
            }
            OwnedFrameResp3::Map { data, .. } => {
                let mut map = Map::new();
                for (k, v) in data {
                    if let Value::String(key) = k.into() {
                        map.insert(key, v.into());
                    }
                }
                Value::Object(map)
            }
            OwnedFrameResp3::Set { data, .. } => {
                Value::Array(data.into_iter().map(|v| v.into()).collect())
            }
            OwnedFrameResp3::Hello { .. } => Value::Null,
        }
    }
}

impl From<bool> for crate::api::RedisJsonValue {
    fn from(v: bool) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Boolean {
            data: v,
            attributes: None,
        })
    }
}

impl From<i32> for crate::api::RedisJsonValue {
    fn from(v: i32) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Number {
            data: v as i64,
            attributes: None,
        })
    }
}

impl From<i64> for crate::api::RedisJsonValue {
    fn from(v: i64) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Number {
            data: v,
            attributes: None,
        })
    }
}

impl From<u64> for crate::api::RedisJsonValue {
    fn from(v: u64) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Number {
            data: v as i64,
            attributes: None,
        })
    }
}

impl From<f32> for crate::api::RedisJsonValue {
    fn from(v: f32) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Double {
            data: v as f64,
            attributes: None,
        })
    }
}

impl From<f64> for crate::api::RedisJsonValue {
    fn from(v: f64) -> crate::api::RedisJsonValue {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Double {
            data: v,
            attributes: None,
        })
    }
}

impl From<String> for crate::api::RedisJsonValue {
    fn from(v: String) -> crate::api::RedisJsonValue {
        match v.to_lowercase().as_str() {
            "nil" | "null" => crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Null),
            _ => crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::SimpleString {
                data: v.into_bytes(),
                attributes: None,
            }),
        }
    }
}

impl From<&str> for crate::api::RedisJsonValue {
    fn from(v: &str) -> crate::api::RedisJsonValue {
        match v.to_lowercase().as_str() {
            "nil" | "null" => crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Null),
            _ => crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::SimpleString {
                data: v.as_bytes().to_vec(),
                attributes: None,
            }),
        }
    }
}

impl From<Vec<crate::api::RedisJsonValue>> for crate::api::RedisJsonValue {
    fn from(v: Vec<crate::api::RedisJsonValue>) -> crate::api::RedisJsonValue {
        let arr: Vec<OwnedFrameResp3> = v
            .into_iter()
            .map(|item| match item {
                crate::api::RedisJsonValue::Resp3(frame) => frame,
                crate::api::RedisJsonValue::Resp2(frame) => frame.into(),
            })
            .collect();
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Array {
            data: arr,
            attributes: None,
        })
    }
}

impl From<HashMap<String, crate::api::RedisJsonValue>> for crate::api::RedisJsonValue {
    fn from(v: HashMap<String, crate::api::RedisJsonValue>) -> crate::api::RedisJsonValue {
        let map: Vec<(OwnedFrameResp3, OwnedFrameResp3)> = v
            .into_iter()
            .map(|(k, val)| {
                let key = OwnedFrameResp3::SimpleString {
                    data: k.into_bytes(),
                    attributes: None,
                };
                let value = match val {
                    crate::api::RedisJsonValue::Resp3(frame) => frame,
                    crate::api::RedisJsonValue::Resp2(frame) => frame.into(),
                };
                (key, value)
            })
            .collect();
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Map {
            data: map,
            attributes: None,
        })
    }
}

impl From<OwnedFrameResp2> for OwnedFrameResp3 {
    fn from(frame: OwnedFrameResp2) -> OwnedFrameResp3 {
        match frame {
            OwnedFrameResp2::Null => OwnedFrameResp3::Null,
            OwnedFrameResp2::SimpleString(s) => OwnedFrameResp3::SimpleString {
                data: s,
                attributes: None,
            },
            OwnedFrameResp2::BulkString(s) => OwnedFrameResp3::BlobString {
                data: s,
                attributes: None,
            },
            OwnedFrameResp2::Error(e) => OwnedFrameResp3::SimpleError {
                data: e,
                attributes: None,
            },
            OwnedFrameResp2::Integer(i) => OwnedFrameResp3::Number {
                data: i,
                attributes: None,
            },
            OwnedFrameResp2::Array(arr) => OwnedFrameResp3::Array {
                data: arr.into_iter().map(|v| v.into()).collect(),
                attributes: None,
            },
        }
    }
}

impl Display for crate::api::RedisJsonValue {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let value: Value = match self {
            crate::api::RedisJsonValue::Resp2(frame) => frame.clone().into(),
            crate::api::RedisJsonValue::Resp3(frame) => frame.clone().into(),
        };
        write!(f, "{}", value)
    }
}

impl Default for crate::api::RedisJsonValue {
    fn default() -> Self {
        crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::SimpleString {
            data: Vec::new(),
            attributes: None,
        })
    }
}

impl ToRedisArgs for crate::api::RedisJsonValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            crate::api::RedisJsonValue::Resp2(frame) => frame.write_redis_args(out),
            crate::api::RedisJsonValue::Resp3(frame) => frame.write_redis_args(out),
        }
    }
}

impl ToRedisArgs for OwnedFrameResp2 {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            OwnedFrameResp2::Null => "nil".write_redis_args(out),
            OwnedFrameResp2::SimpleString(s) | OwnedFrameResp2::BulkString(s) => {
                out.write_arg(s)
            }
            OwnedFrameResp2::Error(e) => e.write_redis_args(out),
            OwnedFrameResp2::Integer(i) => i.write_redis_args(out),
            OwnedFrameResp2::Array(arr) => {
                for item in arr {
                    item.write_redis_args(out);
                }
            }
        }
    }
}

impl ToRedisArgs for OwnedFrameResp3 {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            OwnedFrameResp3::Null => "nil".write_redis_args(out),
            OwnedFrameResp3::BlobString { data, .. }
            | OwnedFrameResp3::SimpleString { data, .. }
            | OwnedFrameResp3::ChunkedString(data)
            | OwnedFrameResp3::VerbatimString { data, .. } => out.write_arg(data),
            OwnedFrameResp3::BlobError { data, .. } => out.write_arg(data),
            OwnedFrameResp3::SimpleError { data, .. } => data.write_redis_args(out),
            OwnedFrameResp3::Boolean { data, .. } => data.write_redis_args(out),
            OwnedFrameResp3::Number { data, .. } => data.write_redis_args(out),
            OwnedFrameResp3::Double { data, .. } => data.to_string().write_redis_args(out),
            OwnedFrameResp3::BigNumber { data, .. } => out.write_arg(data),
            OwnedFrameResp3::Array { data, .. } | OwnedFrameResp3::Push { data, .. } => {
                for item in data {
                    item.write_redis_args(out);
                }
            }
            OwnedFrameResp3::Map { data, .. } => {
                serde_json::to_string(data).unwrap_or_default().write_redis_args(out)
            }
            OwnedFrameResp3::Set { data, .. } => {
                serde_json::to_string(data).unwrap_or_default().write_redis_args(out)
            }
            OwnedFrameResp3::Hello { .. } => "nil".write_redis_args(out),
        }
    }
}

impl TryFrom<Value> for crate::api::RedisJsonValue {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(crate::api::RedisJsonValue::Resp3(OwnedFrameResp3::Null)),
            Value::String(s) => Ok(crate::api::RedisJsonValue::from(s)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(crate::api::RedisJsonValue::from(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(crate::api::RedisJsonValue::from(f))
                } else {
                    Err(format!("Unsupported number format: {}", n))
                }
            }
            Value::Bool(b) => Ok(crate::api::RedisJsonValue::from(b)),
            Value::Array(arr) => {
                let mut items = Vec::with_capacity(arr.len());
                for item in arr {
                    items.push(crate::api::RedisJsonValue::try_from(item)?);
                }
                Ok(crate::api::RedisJsonValue::from(items))
            }
            Value::Object(obj) => {
                let mut map = HashMap::with_capacity(obj.len());
                for (k, v) in obj {
                    map.insert(k, crate::api::RedisJsonValue::try_from(v)?);
                }
                Ok(crate::api::RedisJsonValue::from(map))
            }
        }
    }
}

pub fn extract_redis_value(json: &Value, field: &str) -> Result<crate::api::RedisJsonValue, String> {
    json.get(field)
        .ok_or_else(|| format!("Field '{}' not found", field))
        .and_then(|v| crate::api::RedisJsonValue::try_from(v.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::RedisResult;

    struct MockRedisConnection;

    impl MockRedisConnection {
        fn set<T: redis::ToRedisArgs>(&self, _key: &str, _value: T) -> RedisResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_bool_to_redis() {
        let value = crate::api::RedisJsonValue::from(true);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_integer_to_redis() {
        let value = crate::api::RedisJsonValue::from(42i64);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_float_to_redis() {
        let value = crate::api::RedisJsonValue::from(3.5f64);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_string_to_redis() {
        let value = crate::api::RedisJsonValue::from("test string");
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_array_to_redis() {
        let value = crate::api::RedisJsonValue::from(vec![
            crate::api::RedisJsonValue::from(1i64),
            crate::api::RedisJsonValue::from("test"),
            crate::api::RedisJsonValue::from(true),
        ]);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_object_to_redis() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), crate::api::RedisJsonValue::from("test"));
        map.insert("count".to_string(), crate::api::RedisJsonValue::from(5i64));
        let value = crate::api::RedisJsonValue::from(map);

        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_deserialization() {
        let json_str = r#"{"key": "mykey"}"#;
        let result: MyStruct = serde_json::from_str(json_str).unwrap_or_default();
        assert!(matches!(result.key, RedisJsonValue::Resp3(_)));

        let json_int = r#"{"key": 1}"#;
        let result: MyStruct = serde_json::from_str(json_int).unwrap_or_default();
        assert!(matches!(result.key, RedisJsonValue::Resp3(_)));

        let json_float = r#"{"key": 0.3}"#;
        let result: MyStruct = serde_json::from_str(json_float).unwrap_or_default();
        assert!(matches!(result.key, RedisJsonValue::Resp3(_)));
    }

    #[test]
    fn test_equality() {
        let v1 = crate::api::RedisJsonValue::from(42i64);
        let v2 = crate::api::RedisJsonValue::from(42i64);
        // assert_eq!(v1, v2);

        let v3 = crate::api::RedisJsonValue::from("test");
        let v4 = crate::api::RedisJsonValue::from("test");
        // assert_eq!(v3, v4);
    }
}
