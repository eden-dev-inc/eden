use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, MemoryStatsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::MemoryStats, "Returns details about memory usage", ReqType::Read, true);

/// See official Redis documentation for `MEMORY STATS`
/// https://redis.io/docs/latest/commands/memory-stats/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MemoryStatsInput {}

impl Serialize for MemoryStatsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryStatsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MemoryStatsInput, API_INFO);

impl RedisCommandInput for MemoryStatsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "MEMORY STATS expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis MEMORY STATS command
///
/// Returns detailed memory statistics as key-value pairs.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MemoryStatsOutput {
    /// Raw stats as key-value pairs
    stats: HashMap<String, RedisJsonValue>,
}

impl MemoryStatsOutput {
    pub fn new(stats: HashMap<String, RedisJsonValue>) -> Self {
        Self { stats }
    }

    /// Get all stats
    pub fn stats(&self) -> &HashMap<String, RedisJsonValue> {
        &self.stats
    }

    /// Get a specific stat by name
    pub fn get(&self, key: &str) -> Option<&RedisJsonValue> {
        self.stats.get(key)
    }

    /// Get peak.allocated memory in bytes
    pub fn peak_allocated(&self) -> Option<i64> {
        self.get_int("peak.allocated")
    }

    /// Get total.allocated memory in bytes
    pub fn total_allocated(&self) -> Option<i64> {
        self.get_int("total.allocated")
    }

    /// Get fragmentation ratio
    pub fn fragmentation_ratio(&self) -> Option<f64> {
        match self.stats.get("fragmentation") {
            Some(RedisJsonValue::Float(f)) => Some(*f),
            Some(RedisJsonValue::Integer(i)) => Some(*i as f64),
            Some(RedisJsonValue::String(s)) => s.parse().ok(),
            _ => None,
        }
    }

    fn get_int(&self, key: &str) -> Option<i64> {
        match self.stats.get(key) {
            Some(RedisJsonValue::Integer(i)) => Some(*i),
            Some(RedisJsonValue::String(s)) => s.parse().ok(),
            _ => None,
        }
    }

    /// Decode the Redis protocol response into a MemoryStatsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let stats = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2_array(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3_frame(resp3_frame)?,
        };

        Ok(Self { stats })
    }

    fn parse_resp2_array(frame: Resp2Frame) -> Result<HashMap<String, RedisJsonValue>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut stats = HashMap::new();
                let mut iter = items.into_iter();

                while let Some(key_frame) = iter.next() {
                    let key = match key_frame {
                        Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                        Resp2Frame::SimpleString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                        _ => continue,
                    };

                    if let Some(value_frame) = iter.next() {
                        let value = Self::resp2_to_json_value(value_frame)?;
                        stats.insert(key, value);
                    }
                }

                Ok(stats)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected MEMORY STATS response: {:?}", other))),
        }
    }

    fn resp2_to_json_value(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(RedisJsonValue::Integer(i)),
            Resp2Frame::BulkString(bytes) => {
                let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                if let Ok(i) = s.parse::<i64>() {
                    Ok(RedisJsonValue::Integer(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(RedisJsonValue::Float(f))
                } else {
                    Ok(RedisJsonValue::String(s))
                }
            }
            Resp2Frame::SimpleString(bytes) => Ok(RedisJsonValue::String(String::from_utf8(bytes).map_err(EpError::parse)?)),
            Resp2Frame::Array(items) => {
                // Nested stats (like db0, db1, etc.)
                let mut nested = HashMap::new();
                let mut iter = items.into_iter();
                while let Some(k) = iter.next() {
                    if let Resp2Frame::BulkString(key_bytes) = k {
                        let key = String::from_utf8(key_bytes).map_err(EpError::parse)?;
                        if let Some(v) = iter.next() {
                            nested.insert(key, Self::resp2_to_json_value(v)?);
                        }
                    }
                }
                Ok(RedisJsonValue::Object(nested))
            }
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
        }
    }

    fn parse_resp3_frame(frame: Resp3Frame) -> Result<HashMap<String, RedisJsonValue>, EpError> {
        match frame {
            Resp3Frame::Map { data, .. } => {
                let mut stats = HashMap::new();
                for (key_frame, value_frame) in data {
                    let key = match key_frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    let value = Self::resp3_to_json_value(value_frame)?;
                    stats.insert(key, value);
                }
                Ok(stats)
            }
            Resp3Frame::Array { data, .. } => {
                // Fallback for array format
                let mut stats = HashMap::new();
                let mut iter = data.into_iter();
                while let Some(key_frame) = iter.next() {
                    let key = match key_frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    if let Some(value_frame) = iter.next() {
                        let value = Self::resp3_to_json_value(value_frame)?;
                        stats.insert(key, value);
                    }
                }
                Ok(stats)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected MEMORY STATS response: {:?}", other))),
        }
    }

    fn resp3_to_json_value(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
            Resp3Frame::Double { data, .. } => Ok(RedisJsonValue::Float(data)),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Ok(RedisJsonValue::String(s))
            }
            Resp3Frame::SimpleString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::Map { data, .. } => {
                let mut nested = HashMap::new();
                for (k, v) in data {
                    if let Resp3Frame::BlobString { data: key_bytes, .. } = k {
                        let key = String::from_utf8(key_bytes).map_err(EpError::parse)?;
                        nested.insert(key, Self::resp3_to_json_value(v)?);
                    }
                }
                Ok(RedisJsonValue::Object(nested))
            }
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
        }
    }
}

impl Serialize for MemoryStatsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryStatsOutput", 1)?;
        state.serialize_field("stats", &self.stats)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = MemoryStatsInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nMEMORY\r\n$5\r\nSTATS\r\n");
        }

        #[test]
        fn test_decode_simple_array() {
            // Simple RESP2 array with key-value pairs
            // "peak.allocated" = 14 chars, "total.allocated" = 15 chars
            let response = b"*4\r\n$14\r\npeak.allocated\r\n:1048576\r\n$15\r\ntotal.allocated\r\n:524288\r\n";
            let output = MemoryStatsOutput::decode(response).unwrap();
            assert_eq!(output.peak_allocated(), Some(1048576));
            assert_eq!(output.total_allocated(), Some(524288));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MemoryStatsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = MemoryStatsInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns() {
            let input = MemoryStatsInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MemoryStatsInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = MemoryStatsInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::MemoryStats);
        }

        #[test]
        fn test_get_stat() {
            let mut stats = HashMap::new();
            stats.insert("peak.allocated".to_string(), RedisJsonValue::Integer(1000));
            let output = MemoryStatsOutput::new(stats);
            assert_eq!(output.get("peak.allocated"), Some(&RedisJsonValue::Integer(1000)));
            assert_eq!(output.get("nonexistent"), None);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_stats_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryStatsInput {}.command()).await.expect("raw failed");

                    let output = MemoryStatsOutput::decode(&result).expect("decode failed");
                    // Should have some stats
                    assert!(!output.stats().is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_stats_has_expected_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryStatsInput {}.command()).await.expect("raw failed");

                    let output = MemoryStatsOutput::decode(&result).expect("decode failed");
                    // These keys should always be present
                    assert!(output.get("peak.allocated").is_some() || output.stats().keys().any(|k| k.contains("allocated")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_stats_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&MemoryStatsInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = MemoryStatsOutput::decode(&result).expect("decode failed");
            assert!(!output.stats().is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_stats_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&MemoryStatsInput {}.command()).await.expect("raw failed");

            // RESP3 returns map (%) or array (*)
            assert!(result.starts_with(b"%") || result.starts_with(b"*"), "RESP3 should return map or array");
            let output = MemoryStatsOutput::decode(&result).expect("decode failed");
            assert!(!output.stats().is_empty());

            ctx.stop().await;
        }
    }
}
