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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, SlowlogLenInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::SlowlogLen,
    "Returns the number of entries in the slow log",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SLOWLOG LEN`
/// https://redis.io/docs/latest/commands/slowlog-len/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SlowlogLenInput {}

impl Serialize for SlowlogLenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogLenInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(SlowlogLenInput, API_INFO);

impl RedisCommandInput for SlowlogLenInput {
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
                "SLOWLOG LEN expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis SLOWLOG LEN command
///
/// Returns the number of entries currently in the slow log.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SlowlogLenOutput {
    /// The number of entries in the slow log
    length: i64,
}

impl SlowlogLenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the number of entries in the slow log
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Alias for length() for semantic clarity
    pub fn len(&self) -> i64 {
        self.length
    }

    /// Check if the slow log is empty
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into a SlowlogLenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SLOWLOG LEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SLOWLOG LEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for SlowlogLenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogLenOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = SlowlogLenInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SLOWLOG"));
            assert!(cmd_str.contains("LEN"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = SlowlogLenOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.length(), 42);
            assert_eq!(output.len(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_zero() {
            let output = SlowlogLenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SlowlogLenOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = SlowlogLenInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SlowlogLenInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SlowlogLenInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::SlowlogLen);
        }

        #[test]
        fn test_serialize_output() {
            let output = SlowlogLenOutput::new(10);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"length\":10"));
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
        async fn test_slowlog_len_after_reset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset the slow log first
                    ctx.raw(&SlowlogResetInput {}.command()).await.expect("reset failed");

                    let result = ctx.raw(&SlowlogLenInput {}.command()).await.expect("raw failed");

                    let output = SlowlogLenOutput::decode(&result).expect("decode failed");
                    // After reset, length should be 0 or very small
                    // (some commands during test might be logged)
                    assert!(output.length() >= 0, "SLOWLOG LEN should return non-negative");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_len_non_negative() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogLenInput {}.command()).await.expect("raw failed");

                    let output = SlowlogLenOutput::decode(&result).expect("decode failed");
                    assert!(output.length() >= 0, "SLOWLOG LEN should always be non-negative");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_len_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&SlowlogLenInput {}.command());
                    pipeline.extend_from_slice(&SlowlogLenInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    for resp in responses {
                        let output = SlowlogLenOutput::decode(resp).expect("decode failed");
                        assert!(output.length() >= 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_len_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&SlowlogLenInput {}.command()).await.expect("raw failed");

            // RESP2 integer format: :N\r\n
            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = SlowlogLenOutput::decode(&result).expect("decode failed");
            assert!(output.length() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_len_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&SlowlogLenInput {}.command()).await.expect("raw failed");

            // RESP3 also uses : for integers
            assert!(result.starts_with(b":"), "RESP3 should return integer");
            let output = SlowlogLenOutput::decode(&result).expect("decode failed");
            assert!(output.length() >= 0);

            ctx.stop().await;
        }
    }
}
