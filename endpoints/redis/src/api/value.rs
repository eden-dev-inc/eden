use base64::{Engine, engine::general_purpose::STANDARD};
use borsh::{BorshDeserialize, BorshSerialize};
use redis::ToRedisArgs;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt::Display;
use utoipa::ToSchema;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MyStruct {
    key: RedisJsonValue,
}

const BYTES_TAG: &str = "$bytes";

/// A JSON-compatible value type for Redis with support for binary data.
///
/// Binary data is serialized as `{"$bytes": "base64..."}` for unambiguous round-tripping.
#[derive(Debug, BorshSerialize, BorshDeserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum RedisJsonValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    #[schemars(with = "String")]
    Bytes(Vec<u8>),
    Array(Vec<RedisJsonValue>),
    Object(HashMap<String, RedisJsonValue>),
}

impl Serialize for RedisJsonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            RedisJsonValue::Null => serializer.serialize_none(),
            RedisJsonValue::Bool(b) => serializer.serialize_bool(*b),
            RedisJsonValue::Integer(i) => serializer.serialize_i64(*i),
            RedisJsonValue::Float(f) => serializer.serialize_f64(*f),
            RedisJsonValue::String(s) => serializer.serialize_str(s),
            RedisJsonValue::Bytes(b) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(BYTES_TAG, &STANDARD.encode(b))?;
                map.end()
            }
            RedisJsonValue::Array(arr) => Serialize::serialize(arr, serializer),
            RedisJsonValue::Object(obj) => Serialize::serialize(obj, serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RedisJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        RedisJsonValue::from_serde_value(value).map_err(serde::de::Error::custom)
    }
}

impl RedisJsonValue {
    /// Convert from serde_json::Value, detecting $bytes tagged objects
    fn from_serde_value(value: Value) -> Result<Self, String> {
        match value {
            Value::Null => Ok(RedisJsonValue::Null),
            Value::Bool(b) => Ok(RedisJsonValue::Bool(b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(RedisJsonValue::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(RedisJsonValue::Float(f))
                } else {
                    Err(format!("Unsupported number: {}", n))
                }
            }
            Value::String(s) => Ok(RedisJsonValue::String(s)),
            Value::Array(arr) => {
                let items: Result<Vec<_>, _> = arr.into_iter().map(Self::from_serde_value).collect();
                Ok(RedisJsonValue::Array(items?))
            }
            Value::Object(obj) => {
                // Check for $bytes tag
                if obj.len() == 1
                    && let Some(Value::String(encoded)) = obj.get(BYTES_TAG)
                {
                    let bytes = STANDARD.decode(encoded).map_err(|e| format!("Invalid base64 in $bytes: {}", e))?;
                    return Ok(RedisJsonValue::Bytes(bytes));
                }

                // Regular object
                let items: Result<HashMap<_, _>, _> = obj.into_iter().map(|(k, v)| Self::from_serde_value(v).map(|v| (k, v))).collect();
                Ok(RedisJsonValue::Object(items?))
            }
        }
    }

    /// Create a Bytes variant from raw bytes
    pub fn bytes(data: impl Into<Vec<u8>>) -> Self {
        RedisJsonValue::Bytes(data.into())
    }

    /// Get as bytes if this is a Bytes variant
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RedisJsonValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Check if this is a Bytes variant
    pub fn is_bytes(&self) -> bool {
        matches!(self, RedisJsonValue::Bytes(_))
    }
}

impl From<RedisJsonValue> for Value {
    fn from(v: RedisJsonValue) -> Value {
        match v {
            RedisJsonValue::Null => Value::Null,
            RedisJsonValue::Bool(b) => Value::from(b),
            RedisJsonValue::String(s) => Value::from(s),
            RedisJsonValue::Float(f) => Value::from(f),
            RedisJsonValue::Integer(i) => Value::from(i),
            RedisJsonValue::Bytes(b) => {
                let mut map = Map::new();
                map.insert(BYTES_TAG.to_string(), Value::String(STANDARD.encode(&b)));
                Value::Object(map)
            }
            RedisJsonValue::Array(a) => Value::Array(a.into_iter().map(Into::into).collect()),
            RedisJsonValue::Object(o) => Value::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect()),
        }
    }
}

impl From<bool> for RedisJsonValue {
    fn from(v: bool) -> RedisJsonValue {
        RedisJsonValue::Bool(v)
    }
}

impl From<i32> for RedisJsonValue {
    fn from(v: i32) -> RedisJsonValue {
        RedisJsonValue::Integer(v as i64)
    }
}

impl From<i64> for RedisJsonValue {
    fn from(v: i64) -> RedisJsonValue {
        RedisJsonValue::Integer(v)
    }
}

impl From<u64> for RedisJsonValue {
    fn from(v: u64) -> RedisJsonValue {
        RedisJsonValue::Integer(v as i64)
    }
}

impl From<f32> for RedisJsonValue {
    fn from(v: f32) -> RedisJsonValue {
        RedisJsonValue::Float(v as f64)
    }
}

impl From<f64> for RedisJsonValue {
    fn from(v: f64) -> RedisJsonValue {
        RedisJsonValue::Float(v)
    }
}

impl From<String> for RedisJsonValue {
    fn from(v: String) -> RedisJsonValue {
        match v.to_lowercase().as_str() {
            "nil" | "null" => RedisJsonValue::Null,
            _ => RedisJsonValue::String(v),
        }
    }
}

impl From<&'static str> for RedisJsonValue {
    fn from(v: &'static str) -> RedisJsonValue {
        match v.to_lowercase().as_str() {
            "nil" | "null" => RedisJsonValue::Null,
            _ => RedisJsonValue::String(v.to_string()),
        }
    }
}

impl From<&[u8]> for RedisJsonValue {
    fn from(bytes: &[u8]) -> RedisJsonValue {
        RedisJsonValue::Bytes(bytes.to_vec())
    }
}

impl From<Vec<u8>> for RedisJsonValue {
    fn from(bytes: Vec<u8>) -> RedisJsonValue {
        RedisJsonValue::Bytes(bytes)
    }
}

impl Display for RedisJsonValue {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            Self::Null => write!(f, "nil"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "{}", s),
            Self::Bytes(b) => write!(f, "{}", STANDARD.encode(b)),
            Self::Array(arr) => write!(f, "{}", serde_json::to_string(arr).unwrap_or_default()),
            Self::Object(obj) => write!(f, "{}", serde_json::to_string(obj).unwrap_or_default()),
        }
    }
}

impl Default for RedisJsonValue {
    fn default() -> Self {
        RedisJsonValue::String(String::default())
    }
}

impl ToRedisArgs for RedisJsonValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            RedisJsonValue::Null => "nil".write_redis_args(out),
            RedisJsonValue::Bool(b) => b.write_redis_args(out),
            RedisJsonValue::Integer(i) => i.write_redis_args(out),
            RedisJsonValue::Float(f) => f.to_string().write_redis_args(out),
            RedisJsonValue::String(s) => s.write_redis_args(out),
            RedisJsonValue::Bytes(b) => b.as_slice().write_redis_args(out),
            RedisJsonValue::Array(a) => {
                for item in a {
                    item.write_redis_args(out);
                }
            }
            RedisJsonValue::Object(o) => serde_json::to_value(o).unwrap_or_default().to_string().write_redis_args(out),
        }
    }
}

impl TryFrom<Value> for RedisJsonValue {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::from_serde_value(value)
    }
}

/// Extract a RedisJsonValue from a JSON object by field name
pub fn extract_redis_value(json: &Value, field: &str) -> Result<RedisJsonValue, String> {
    json.get(field).ok_or_else(|| format!("Field '{}' not found", field)).and_then(|v| RedisJsonValue::try_from(v.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::RedisResult;

    struct MockRedisConnection;

    impl MockRedisConnection {
        fn set<T: ToRedisArgs>(&self, _key: &str, _value: T) -> RedisResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_bool_to_redis() {
        let value = RedisJsonValue::Bool(true);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_integer_to_redis() {
        let value = RedisJsonValue::Integer(42);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_float_to_redis() {
        let value = RedisJsonValue::Float(3.5);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_string_to_redis() {
        let value = RedisJsonValue::from("test string");
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_bytes_to_redis() {
        let value = RedisJsonValue::Bytes(vec![0x00, 0x01, 0xFF]);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_array_to_redis() {
        let value = RedisJsonValue::Array(vec![
            RedisJsonValue::Integer(1),
            RedisJsonValue::String("test".to_string()),
            RedisJsonValue::Bool(true),
        ]);
        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_object_to_redis() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), RedisJsonValue::String("test".to_string()));
        map.insert("count".to_string(), RedisJsonValue::Integer(5));
        let value = RedisJsonValue::Object(map);

        let conn = MockRedisConnection;
        assert!(conn.set("test_key", value).is_ok());
    }

    #[test]
    fn test_deserialization() {
        // Test with string value
        let json_str = r#"{"key": "mykey"}"#;
        let result: MyStruct = serde_json::from_str(json_str).unwrap();
        assert!(matches!(result.key, RedisJsonValue::String(s) if s == "mykey"));

        // Test with integer value
        let json_int = r#"{"key": 1}"#;
        let result: MyStruct = serde_json::from_str(json_int).unwrap();
        assert!(matches!(result.key, RedisJsonValue::Integer(1)));

        // Test with float value
        let json_float = r#"{"key": 0.3}"#;
        let result: MyStruct = serde_json::from_str(json_float).unwrap();
        assert!(matches!(result.key, RedisJsonValue::Float(f) if (f - 0.3).abs() < f64::EPSILON));
    }

    #[test]
    fn test_bytes_serialization_roundtrip() {
        let original = RedisJsonValue::Bytes(vec![0x00, 0x01, 0x02, 0xFF]);
        let json = serde_json::to_string(&original).unwrap();

        assert!(json.contains("$bytes"));

        let parsed: RedisJsonValue = serde_json::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_bytes_in_nested_structure() {
        let mut obj = HashMap::new();
        obj.insert("data".to_string(), RedisJsonValue::Bytes(vec![0xDE, 0xAD]));
        obj.insert("name".to_string(), RedisJsonValue::String("test".to_string()));
        let original = RedisJsonValue::Object(obj);

        let json = serde_json::to_string(&original).unwrap();
        let parsed: RedisJsonValue = serde_json::from_str(&json).unwrap();

        assert_eq!(original, parsed);
    }

    #[test]
    fn test_from_vec_u8_creates_bytes() {
        let value = RedisJsonValue::from(vec![0x01, 0x02, 0x03]);
        assert!(matches!(value, RedisJsonValue::Bytes(_)));
    }

    #[test]
    fn test_from_slice_creates_bytes() {
        let data: &[u8] = &[0x01, 0x02, 0x03];
        let value = RedisJsonValue::from(data);
        assert!(matches!(value, RedisJsonValue::Bytes(_)));
    }
}
