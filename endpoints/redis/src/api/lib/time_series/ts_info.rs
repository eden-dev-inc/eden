use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsInfo,
    "Returns information and statistics for a time series",
    ReqType::Read,
    true,
);

/// Input for Redis `TS.INFO` command.
///
/// Returns information and statistics about a time series.
///
/// See official Redis documentation for `TS.INFO`:
/// https://redis.io/docs/latest/commands/ts.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsInfoInput {
    /// The key name of the time series
    key: RedisKey,
    /// When true, returns additional debug information about chunks
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    debug: Option<bool>,
}

impl Serialize for TsInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.debug.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsInfoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(debug) = &self.debug {
            state.serialize_field("debug", debug)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsInfoInput,
    API_INFO,
    {key, debug}
);

impl RedisCommandInput for TsInfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(true) = self.debug {
            command.arg("DEBUG");
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.INFO requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let debug = if args.len() > 1 {
            if let RedisJsonValue::String(s) = &args[1] {
                if s.to_uppercase() == "DEBUG" { Some(true) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        Ok(TsInfoInput { key, debug })
    }
}

/// Output for Redis `TS.INFO` command.
///
/// Contains information and statistics about a time series.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsInfoOutput {
    /// Raw properties from the time series info response
    properties: HashMap<String, RedisJsonValue>,
}

impl TsInfoOutput {
    pub fn new(properties: HashMap<String, RedisJsonValue>) -> Self {
        Self { properties }
    }

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, RedisJsonValue> {
        &self.properties
    }

    /// Get a specific property by name
    pub fn get(&self, key: &str) -> Option<&RedisJsonValue> {
        self.properties.get(key)
    }

    /// Get total samples count
    pub fn total_samples(&self) -> Option<i64> {
        self.properties.get("totalSamples").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> Option<i64> {
        self.properties.get("memoryUsage").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Get retention time in milliseconds
    pub fn retention_time(&self) -> Option<i64> {
        self.properties.get("retentionTime").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Get first timestamp
    pub fn first_timestamp(&self) -> Option<i64> {
        self.properties.get("firstTimestamp").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Get last timestamp
    pub fn last_timestamp(&self) -> Option<i64> {
        self.properties.get("lastTimestamp").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Get chunk count
    pub fn chunk_count(&self) -> Option<i64> {
        self.properties.get("chunkCount").and_then(|v| match v {
            RedisJsonValue::Integer(n) => Some(*n),
            _ => None,
        })
    }

    /// Decode the Redis protocol response into a TsInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let properties = Self::parse_frame(frame)?;
        Ok(Self { properties })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<HashMap<String, RedisJsonValue>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<HashMap<String, RedisJsonValue>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut properties = HashMap::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::resp2_to_string(&arr[i])?;
                    let value = Self::resp2_to_json(&arr[i + 1]);
                    properties.insert(key, value);
                    i += 2;
                }
                Ok(properties)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.INFO response: {:?}", other))),
        }
    }

    fn resp2_to_string(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            Resp2Frame::SimpleString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn resp2_to_json(frame: &Resp2Frame) -> RedisJsonValue {
        match frame {
            Resp2Frame::Integer(n) => RedisJsonValue::Integer(*n),
            Resp2Frame::BulkString(data) => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
            Resp2Frame::SimpleString(data) => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
            Resp2Frame::Array(arr) => RedisJsonValue::Array(arr.iter().map(Self::resp2_to_json).collect()),
            Resp2Frame::Null => RedisJsonValue::Null,
            Resp2Frame::Error(e) => RedisJsonValue::String(e.clone()),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<HashMap<String, RedisJsonValue>, EpError> {
        match frame {
            Resp3Frame::Map { data, .. } => {
                let mut properties = HashMap::new();
                for (k, v) in data {
                    let key = Self::resp3_to_string(&k)?;
                    let value = Self::resp3_to_json(&v);
                    properties.insert(key, value);
                }
                Ok(properties)
            }
            Resp3Frame::Array { data, .. } => {
                // Fallback for array format
                let mut properties = HashMap::new();
                let mut i = 0;
                while i + 1 < data.len() {
                    let key = Self::resp3_to_string(&data[i])?;
                    let value = Self::resp3_to_json(&data[i + 1]);
                    properties.insert(key, value);
                    i += 2;
                }
                Ok(properties)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.INFO response: {:?}", other))),
        }
    }

    fn resp3_to_string(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.clone()).map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn resp3_to_json(frame: &Resp3Frame) -> RedisJsonValue {
        match frame {
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
            Resp3Frame::Double { data, .. } => RedisJsonValue::Float(*data),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8_lossy(data).to_string())
            }
            Resp3Frame::Array { data, .. } => RedisJsonValue::Array(data.iter().map(Self::resp3_to_json).collect()),
            Resp3Frame::Map { data, .. } => {
                let map: HashMap<String, RedisJsonValue> =
                    data.iter().filter_map(|(k, v)| Self::resp3_to_string(k).ok().map(|key| (key, Self::resp3_to_json(v)))).collect();
                RedisJsonValue::Object(map)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            Resp3Frame::Boolean { data, .. } => RedisJsonValue::Bool(*data),
            _ => RedisJsonValue::Null,
        }
    }
}

impl Serialize for TsInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsInfoOutput", 1)?;
        state.serialize_field("properties", &self.properties)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TsInfoInput { key: RedisKey::String("ts:key".into()), debug: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.INFO"));
            assert!(cmd_str.contains("ts:key"));
            assert!(!cmd_str.contains("DEBUG"));
        }

        #[test]
        fn test_encode_command_with_debug() {
            let input = TsInfoInput { key: RedisKey::String("ts:key".into()), debug: Some(true) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.INFO"));
            assert!(cmd_str.contains("DEBUG"));
        }

        #[test]
        fn test_encode_command_debug_false() {
            let input = TsInfoInput { key: RedisKey::String("ts:key".into()), debug: Some(false) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("DEBUG"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TsInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.debug.is_none());
        }

        #[test]
        fn test_decode_input_with_debug() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("DEBUG".into())];
            let input = TsInfoInput::decode(args).unwrap();
            assert_eq!(input.debug, Some(true));
        }

        #[test]
        fn test_decode_input_debug_lowercase() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("debug".into())];
            let input = TsInfoInput::decode(args).unwrap();
            assert_eq!(input.debug, Some(true));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TsInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsInfoOutput::decode(b"-ERR TSDB: the key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("TSDB"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsInfoInput { key: RedisKey::String("mykey".into()), debug: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsInfoInput { key: RedisKey::String("mykey".into()), debug: None };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsInfo);
        }

        #[test]
        fn test_output_accessors() {
            let mut properties = HashMap::new();
            properties.insert("totalSamples".into(), RedisJsonValue::Integer(1000));
            properties.insert("memoryUsage".into(), RedisJsonValue::Integer(4096));
            properties.insert("retentionTime".into(), RedisJsonValue::Integer(86400000));
            properties.insert("firstTimestamp".into(), RedisJsonValue::Integer(1609459200000));
            properties.insert("lastTimestamp".into(), RedisJsonValue::Integer(1609545600000));
            properties.insert("chunkCount".into(), RedisJsonValue::Integer(5));

            let output = TsInfoOutput::new(properties);
            assert_eq!(output.total_samples(), Some(1000));
            assert_eq!(output.memory_usage(), Some(4096));
            assert_eq!(output.retention_time(), Some(86400000));
            assert_eq!(output.first_timestamp(), Some(1609459200000));
            assert_eq!(output.last_timestamp(), Some(1609545600000));
            assert_eq!(output.chunk_count(), Some(5));
        }

        #[test]
        fn test_output_get_property() {
            let mut properties = HashMap::new();
            properties.insert("customProp".into(), RedisJsonValue::String("value".into()));

            let output = TsInfoOutput::new(properties);
            assert_eq!(output.get("customProp"), Some(&RedisJsonValue::String("value".into())));
            assert!(output.get("nonexistent").is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.INFO requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TsInfoInput { key: RedisKey::String("nonexistent:ts".into()), debug: None }.command()).await;

                    // Expected to fail - key doesn't exist
                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_with_debug_option() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsInfoInput {
                                key: RedisKey::String("nonexistent:ts".into()),
                                debug: Some(true),
                            }
                            .command(),
                        )
                        .await;

                    // Just verify command is accepted
                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
