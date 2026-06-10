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
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, TimeInput> = ApiInfo::new(EpKind::Redis, RedisApi::Time, "Returns the server time", ReqType::Read, true);

/// See official Redis documentation for `TIME`
/// https://redis.io/docs/latest/commands/time/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TimeInput {}

impl Serialize for TimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TimeInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(TimeInput, API_INFO);

impl RedisCommandInput for TimeInput {
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
            log_warn!(_ctx, "TIME expects no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }
        Ok(Self::default())
    }
}

/// Output for Redis TIME command
///
/// Returns the current server time as a two-element array:
/// - Unix timestamp in seconds
/// - Microseconds already elapsed in the current second
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TimeOutput {
    /// Unix timestamp in seconds
    unix_seconds: i64,
    /// Microseconds elapsed in the current second
    microseconds: i64,
}

impl TimeOutput {
    pub fn new(unix_seconds: i64, microseconds: i64) -> Self {
        Self { unix_seconds, microseconds }
    }

    /// Get the Unix timestamp in seconds
    pub fn unix_seconds(&self) -> i64 {
        self.unix_seconds
    }

    /// Get the microseconds elapsed in the current second
    pub fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Get the full timestamp as a tuple (seconds, microseconds)
    pub fn as_tuple(&self) -> (i64, i64) {
        (self.unix_seconds, self.microseconds)
    }

    /// Convert to total microseconds since epoch
    pub fn total_microseconds(&self) -> i128 {
        (self.unix_seconds as i128) * 1_000_000 + (self.microseconds as i128)
    }

    /// Decode the Redis protocol response into a TimeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let (unix_seconds, microseconds) = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    if arr.len() != 2 {
                        return Err(EpError::parse(format!("TIME expects 2-element array, got {}", arr.len())));
                    }
                    let secs = Self::extract_integer_resp2(&arr[0])?;
                    let micros = Self::extract_integer_resp2(&arr[1])?;
                    (secs, micros)
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected TIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    if data.len() != 2 {
                        return Err(EpError::parse(format!("TIME expects 2-element array, got {}", data.len())));
                    }
                    let secs = Self::extract_integer_resp3(&data[0])?;
                    let micros = Self::extract_integer_resp3(&data[1])?;
                    (secs, micros)
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected TIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { unix_seconds, microseconds })
    }

    fn extract_integer_resp2(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(*i),
            Resp2Frame::BulkString(data) => String::from_utf8_lossy(data).parse::<i64>().map_err(EpError::parse),
            Resp2Frame::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>().map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected integer or string, got {:?}", other))),
        }
    }

    fn extract_integer_resp3(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<i64>().map_err(EpError::parse)
            }
            Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).parse::<i64>().map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected integer or string, got {:?}", other))),
        }
    }
}

impl Serialize for TimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TimeOutput", 2)?;
        state.serialize_field("unix_seconds", &self.unix_seconds)?;
        state.serialize_field("microseconds", &self.microseconds)?;
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
            let input = TimeInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nTIME\r\n");
        }

        #[test]
        fn test_decode_array_bulk_strings() {
            // Redis TIME returns array of bulk strings
            let output = TimeOutput::decode(b"*2\r\n$10\r\n1234567890\r\n$6\r\n123456\r\n").unwrap();
            assert_eq!(output.unix_seconds(), 1234567890);
            assert_eq!(output.microseconds(), 123456);
        }

        #[test]
        fn test_as_tuple() {
            let output = TimeOutput::new(1000, 500);
            assert_eq!(output.as_tuple(), (1000, 500));
        }

        #[test]
        fn test_total_microseconds() {
            let output = TimeOutput::new(1, 500000);
            assert_eq!(output.total_microseconds(), 1_500_000);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = TimeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_wrong_array_length() {
            let err = TimeOutput::decode(b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").unwrap_err();
            assert!(err.to_string().contains("2-element"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = TimeInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = TimeInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = TimeInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Time);
        }

        #[test]
        fn test_serialize_output() {
            let output = TimeOutput::new(1234567890, 123456);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"unix_seconds\":1234567890"));
            assert!(json.contains("\"microseconds\":123456"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_time_returns_valid_timestamp() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TimeInput {}.command()).await.expect("raw failed");

                    let output = TimeOutput::decode(&result).expect("decode failed");

                    // Verify timestamp is reasonable (after 2020-01-01)
                    assert!(output.unix_seconds() > 1577836800, "timestamp should be after 2020");
                    // Verify microseconds are in valid range
                    assert!(output.microseconds() >= 0 && output.microseconds() < 1_000_000, "microseconds should be 0-999999");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_time_monotonic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result1 = ctx.raw(&TimeInput {}.command()).await.expect("raw failed");
                    let output1 = TimeOutput::decode(&result1).expect("decode failed");

                    // Small delay
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                    let result2 = ctx.raw(&TimeInput {}.command()).await.expect("raw failed");
                    let output2 = TimeOutput::decode(&result2).expect("decode failed");

                    // Second timestamp should be >= first
                    assert!(
                        output2.total_microseconds() >= output1.total_microseconds(),
                        "time should be monotonically increasing"
                    );
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_time_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&TimeInput {}.command());
                    pipeline.extend_from_slice(&TimeInput {}.command());
                    pipeline.extend_from_slice(&TimeInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    for resp in responses {
                        let output = TimeOutput::decode(resp).expect("decode failed");
                        assert!(output.unix_seconds() > 0);
                        assert!(output.microseconds() >= 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_time_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&TimeInput {}.command()).await.expect("raw failed");

            // RESP2 returns array of bulk strings
            assert!(result.starts_with(b"*2\r\n"), "RESP2 should return array");
            let output = TimeOutput::decode(&result).expect("decode failed");
            assert!(output.unix_seconds() > 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_time_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&TimeInput {}.command()).await.expect("raw failed");

            // RESP3 also returns array
            assert!(result.starts_with(b"*2\r\n"), "RESP3 should return array");
            let output = TimeOutput::decode(&result).expect("decode failed");
            assert!(output.unix_seconds() > 0);

            ctx.stop().await;
        }
    }
}
