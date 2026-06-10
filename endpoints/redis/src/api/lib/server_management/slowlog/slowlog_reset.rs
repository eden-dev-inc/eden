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

const API_INFO: ApiInfo<RedisApi, SlowlogResetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::SlowlogReset, "Clears all entries from the slow log", ReqType::Write, true);

/// See official Redis documentation for `SLOWLOG RESET`
/// https://redis.io/docs/latest/commands/slowlog-reset/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SlowlogResetInput {}

impl Serialize for SlowlogResetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogResetInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(SlowlogResetInput, API_INFO);

impl RedisCommandInput for SlowlogResetInput {
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
                "SLOWLOG RESET expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis SLOWLOG RESET command
///
/// Returns OK on success. Clears all entries from the slow log.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SlowlogResetOutput {
    /// Whether the reset was successful
    success: bool,
}

impl SlowlogResetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the reset was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a SlowlogResetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self::new(true)),
                Resp2Frame::SimpleString(s) => {
                    Err(EpError::parse(format!("unexpected SLOWLOG RESET response: {}", String::from_utf8_lossy(&s))))
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SLOWLOG RESET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self::new(true)),
                Resp3Frame::SimpleString { data, .. } => {
                    Err(EpError::parse(format!("unexpected SLOWLOG RESET response: {}", String::from_utf8_lossy(&data))))
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SLOWLOG RESET response: {:?}", other))),
            },
        }
    }
}

impl Serialize for SlowlogResetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SlowlogResetOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = SlowlogResetInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SLOWLOG"));
            assert!(cmd_str.contains("RESET"));
        }

        #[test]
        fn test_decode_ok() {
            let output = SlowlogResetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SlowlogResetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = SlowlogResetInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SlowlogResetInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SlowlogResetInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::SlowlogReset);
        }

        #[test]
        fn test_serialize_output() {
            let output = SlowlogResetOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::server_management::slowlog::slowlog_len::{SlowlogLenInput, SlowlogLenOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SlowlogResetInput {}.command()).await.expect("raw failed");

                    let output = SlowlogResetOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SLOWLOG RESET should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_clears_log() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset the slow log
                    let result = ctx.raw(&SlowlogResetInput {}.command()).await.expect("raw failed");

                    let output = SlowlogResetOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Check that slow log length is 0 (or very small due to test commands)
                    let len_result = ctx.raw(&SlowlogLenInput {}.command()).await.expect("raw failed");

                    let len_output = SlowlogLenOutput::decode(&len_result).expect("decode failed");
                    // After reset, we expect 0 entries, but the SLOWLOG LEN command
                    // itself might be slow enough to be logged, so we just verify
                    // it's a small number
                    assert!(len_output.length() <= 2, "slow log should be empty or nearly empty after reset");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_multiple_times() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset multiple times should always succeed
                    for _ in 0..3 {
                        let result = ctx.raw(&SlowlogResetInput {}.command()).await.expect("raw failed");

                        let output = SlowlogResetOutput::decode(&result).expect("decode failed");
                        assert!(output.success());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&SlowlogResetInput {}.command());
                    pipeline.extend_from_slice(&SlowlogLenInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let reset_output = SlowlogResetOutput::decode(responses[0]).expect("decode reset");
                    assert!(reset_output.success());

                    let len_output = SlowlogLenOutput::decode(responses[1]).expect("decode len");
                    assert!(len_output.length() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&SlowlogResetInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 should return simple string OK");
            let output = SlowlogResetOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_slowlog_reset_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&SlowlogResetInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 should return simple string OK");
            let output = SlowlogResetOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }
    }
}
