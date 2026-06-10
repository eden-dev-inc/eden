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
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SlowlogGetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::SlowlogGet, "Returns the slow log's entries", ReqType::Read, true);

/// See official Redis documentation for `SLOWLOG GET`
/// https://redis.io/docs/latest/commands/slowlog-get/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SlowlogGetInput {
    /// Optional count of entries to retrieve (default is 10 if not specified)
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
}

impl Serialize for SlowlogGetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;

        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("SlowlogGetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(SlowlogGetInput, API_INFO, { count });

impl RedisCommandInput for SlowlogGetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(count) = &self.count {
            command.arg(count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SLOWLOG GET expects 0 or 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let count = if args.is_empty() {
            None
        } else {
            // Validate that count is non-negative if provided
            let count_val = &args[0];
            if let RedisJsonValue::Integer(n) = count_val {
                if *n < 0 {
                    return Err(EpError::request("SLOWLOG GET count must be non-negative"));
                }
            } else if let RedisJsonValue::String(s) = count_val
                && let Ok(n) = s.parse::<i64>()
                && n < 0
            {
                return Err(EpError::request("SLOWLOG GET count must be non-negative"));
            }
            Some(args[0].clone())
        };

        Ok(Self { count })
    }
}

/// A single entry in the slow log
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SlowlogEntry {
    /// Unique ID of this entry
    pub id: i64,
    /// Unix timestamp when the command was logged
    pub timestamp: i64,
    /// Execution time in microseconds
    pub duration_micros: i64,
    /// The command arguments
    pub command: Vec<String>,
    /// Client IP:port (Redis 4.0+)
    pub client_address: Option<String>,
    /// Client name (Redis 4.0+)
    pub client_name: Option<String>,
}

impl Serialize for SlowlogEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogEntry", 6)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("duration_micros", &self.duration_micros)?;
        state.serialize_field("command", &self.command)?;
        state.serialize_field("client_address", &self.client_address)?;
        state.serialize_field("client_name", &self.client_name)?;
        state.end()
    }
}

/// Output for Redis SLOWLOG GET command
///
/// Returns an array of slow log entries.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SlowlogGetOutput {
    /// The slow log entries
    entries: Vec<SlowlogEntry>,
}

impl SlowlogGetOutput {
    pub fn new(entries: Vec<SlowlogEntry>) -> Self {
        Self { entries }
    }

    /// Get the slow log entries
    pub fn entries(&self) -> &[SlowlogEntry] {
        &self.entries
    }

    /// Get the number of entries returned
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if no entries were returned
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Decode the Redis protocol response into a SlowlogGetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let entries = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => Self::parse_entries_resp2(arr)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SLOWLOG GET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::parse_entries_resp3(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SLOWLOG GET response: {:?}", other)));
                }
            },
        };

        Ok(Self { entries })
    }

    fn parse_entries_resp2(arr: Vec<Resp2Frame>) -> Result<Vec<SlowlogEntry>, EpError> {
        let mut entries = Vec::with_capacity(arr.len());

        for entry_frame in arr {
            if let Resp2Frame::Array(entry_arr) = entry_frame {
                if entry_arr.len() < 4 {
                    return Err(EpError::parse("SLOWLOG entry must have at least 4 elements"));
                }

                let id = Self::extract_integer_resp2(&entry_arr[0])?;
                let timestamp = Self::extract_integer_resp2(&entry_arr[1])?;
                let duration_micros = Self::extract_integer_resp2(&entry_arr[2])?;
                let command = Self::extract_string_array_resp2(&entry_arr[3])?;

                // Optional fields (Redis 4.0+)
                let client_address = entry_arr.get(4).and_then(|f| Self::extract_string_resp2(f).ok());
                let client_name = entry_arr.get(5).and_then(|f| Self::extract_string_resp2(f).ok());

                entries.push(SlowlogEntry {
                    id,
                    timestamp,
                    duration_micros,
                    command,
                    client_address,
                    client_name,
                });
            } else {
                return Err(EpError::parse("SLOWLOG entry must be an array"));
            }
        }

        Ok(entries)
    }

    fn parse_entries_resp3(arr: Vec<Resp3Frame>) -> Result<Vec<SlowlogEntry>, EpError> {
        let mut entries = Vec::with_capacity(arr.len());

        for entry_frame in arr {
            if let Resp3Frame::Array { data: entry_arr, .. } = entry_frame {
                if entry_arr.len() < 4 {
                    return Err(EpError::parse("SLOWLOG entry must have at least 4 elements"));
                }

                let id = Self::extract_integer_resp3(&entry_arr[0])?;
                let timestamp = Self::extract_integer_resp3(&entry_arr[1])?;
                let duration_micros = Self::extract_integer_resp3(&entry_arr[2])?;
                let command = Self::extract_string_array_resp3(&entry_arr[3])?;

                // Optional fields (Redis 4.0+)
                let client_address = entry_arr.get(4).and_then(|f| Self::extract_string_resp3(f).ok());
                let client_name = entry_arr.get(5).and_then(|f| Self::extract_string_resp3(f).ok());

                entries.push(SlowlogEntry {
                    id,
                    timestamp,
                    duration_micros,
                    command,
                    client_address,
                    client_name,
                });
            } else {
                return Err(EpError::parse("SLOWLOG entry must be an array"));
            }
        }

        Ok(entries)
    }

    fn extract_integer_resp2(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(*i),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<i64>().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_integer_resp3(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<i64>().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_string_resp2(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(String::from_utf8(data.clone()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(s) => Ok(String::from_utf8(s.clone()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_string_resp3(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.clone()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.clone()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_string_array_resp2(frame: &Resp2Frame) -> Result<Vec<String>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for item in arr {
                    result.push(Self::extract_string_resp2(item)?);
                }
                Ok(result)
            }
            other => Err(EpError::parse(format!("expected string array, got {:?}", other))),
        }
    }

    fn extract_string_array_resp3(frame: &Resp3Frame) -> Result<Vec<String>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut result = Vec::with_capacity(data.len());
                for item in data {
                    result.push(Self::extract_string_resp3(item)?);
                }
                Ok(result)
            }
            other => Err(EpError::parse(format!("expected string array, got {:?}", other))),
        }
    }
}

impl Serialize for SlowlogGetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogGetOutput", 1)?;
        state.serialize_field("entries", &self.entries)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_count() {
            let input = SlowlogGetInput { count: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SLOWLOG"));
            assert!(cmd_str.contains("GET"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = SlowlogGetInput { count: Some(RedisJsonValue::Integer(5)) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SLOWLOG"));
            assert!(cmd_str.contains("GET"));
            assert!(cmd_str.contains("5"));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = SlowlogGetOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SlowlogGetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = SlowlogGetInput::decode(vec![]).unwrap();
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let input = SlowlogGetInput::decode(vec![RedisJsonValue::Integer(10)]).unwrap();
            assert_eq!(input.count, Some(RedisJsonValue::Integer(10)));
        }

        #[test]
        fn test_decode_input_negative_count_fails() {
            let err = SlowlogGetInput::decode(vec![RedisJsonValue::Integer(-1)]).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_negative_string_count_fails() {
            let err = SlowlogGetInput::decode(vec![RedisJsonValue::String("-5".into())]).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SlowlogGetInput { count: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SlowlogGetInput { count: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::SlowlogGet);
        }

        #[test]
        fn test_serialize_entry() {
            let entry = SlowlogEntry {
                id: 1,
                timestamp: 1234567890,
                duration_micros: 1000,
                command: vec!["GET".into(), "key".into()],
                client_address: Some("127.0.0.1:12345".into()),
                client_name: Some("test".into()),
            };
            let json = serde_json::to_string(&entry).unwrap();
            assert!(json.contains("\"id\":1"));
            assert!(json.contains("\"timestamp\":1234567890"));
            assert!(json.contains("\"duration_micros\":1000"));
            assert!(json.contains("GET"));
        }

        #[test]
        fn test_serialize_output() {
            let output = SlowlogGetOutput::new(vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"entries\":[]"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::server_management::slowlog::slowlog_reset::SlowlogResetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogGetInput { count: None }.command()).await.expect("raw failed");

                    let output = SlowlogGetOutput::decode(&result).expect("decode failed");
                    // Just verify it decodes without error
                    // Entries may or may not be present depending on server state
                    assert!(output.entries().len() <= 128); // Default max entries
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogGetInput { count: Some(RedisJsonValue::Integer(5)) }.command()).await.expect("raw failed");

                    let output = SlowlogGetOutput::decode(&result).expect("decode failed");
                    assert!(output.len() <= 5, "should return at most 5 entries");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_after_reset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset the slow log
                    ctx.raw(&SlowlogResetInput {}.command()).await.expect("reset failed");

                    let result = ctx.raw(&SlowlogGetInput { count: None }.command()).await.expect("raw failed");

                    let output = SlowlogGetOutput::decode(&result).expect("decode failed");
                    // After reset, should be empty or nearly empty
                    assert!(output.len() <= 2, "after reset, should have very few entries");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_entry_structure() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogGetInput { count: None }.command()).await.expect("raw failed");

                    let output = SlowlogGetOutput::decode(&result).expect("decode failed");

                    // If there are entries, verify their structure
                    for entry in output.entries() {
                        assert!(entry.id >= 0, "ID should be non-negative");
                        assert!(entry.timestamp > 0, "timestamp should be positive");
                        assert!(entry.duration_micros >= 0, "duration should be non-negative");
                        assert!(!entry.command.is_empty(), "command should not be empty");
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_zero_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogGetInput { count: Some(RedisJsonValue::Integer(0)) }.command()).await.expect("raw failed");

                    let output = SlowlogGetOutput::decode(&result).expect("decode failed");
                    // With count=0, should return empty array
                    assert!(output.is_empty(), "count=0 should return empty array");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&SlowlogGetInput { count: None }.command());
                    pipeline.extend_from_slice(&SlowlogGetInput { count: Some(RedisJsonValue::Integer(1)) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = SlowlogGetOutput::decode(responses[0]).expect("decode first");
                    let out2 = SlowlogGetOutput::decode(responses[1]).expect("decode second");

                    // Second should have at most 1 entry
                    assert!(out2.len() <= 1);
                    // First should have >= second's entries
                    assert!(out1.len() >= out2.len());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&SlowlogGetInput { count: None }.command()).await.expect("raw failed");

            // RESP2 should return array
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = SlowlogGetOutput::decode(&result).expect("decode failed");
            // Just verify it decodes
            let _ = output.entries();

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_get_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&SlowlogGetInput { count: None }.command()).await.expect("raw failed");

            // RESP3 should return array
            assert!(result.starts_with(b"*"), "RESP3 should return array");
            let output = SlowlogGetOutput::decode(&result).expect("decode failed");
            // Just verify it decodes
            let _ = output.entries();

            ctx.stop().await;
        }
    }
}
