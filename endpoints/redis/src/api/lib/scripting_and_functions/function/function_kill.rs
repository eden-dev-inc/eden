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

const API_INFO: ApiInfo<RedisApi, FunctionKillInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FunctionKill,
    "Terminates a function during execution",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `FUNCTION KILL`
/// https://redis.io/docs/latest/commands/function-kill/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionKillInput {}

impl FunctionKillInput {
    /// Create a new FUNCTION KILL input
    pub fn new() -> Self {
        Self::default()
    }
}

impl Serialize for FunctionKillInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionKillInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(FunctionKillInput, API_INFO);

impl RedisCommandInput for FunctionKillInput {
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
                "FUNCTION KILL expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis FUNCTION KILL command
///
/// Returns OK if a function was successfully terminated, or an error if no
/// function is currently executing or the function has performed writes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FunctionKillOutput {
    /// Whether the kill operation succeeded
    success: bool,
}

impl FunctionKillOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the function was successfully killed
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FunctionKillOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION KILL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION KILL response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for FunctionKillOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FunctionKillOutput", 1)?;
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
            let input = FunctionKillInput::new();
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nFUNCTION\r\n$4\r\nKILL\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = FunctionKillOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_no_script_running() {
            let err = FunctionKillOutput::decode(b"-NOTBUSY No scripts in execution right now.\r\n").unwrap_err();
            assert!(err.to_string().contains("NOTBUSY"));
        }

        #[test]
        fn test_decode_error_script_wrote() {
            let err = FunctionKillOutput::decode(b"-UNKILLABLE The script already performed write commands.\r\n").unwrap_err();
            assert!(err.to_string().contains("UNKILLABLE"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = FunctionKillInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionKill);
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            // Should succeed but log a warning
            let input = FunctionKillInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionKill);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FunctionKillInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FunctionKillInput::new();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = FunctionKillOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_kind() {
            let input = FunctionKillInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionKill);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // FUNCTION commands require Redis 7.0+
        const MIN_VERSION: &str = "7";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_kill_no_function_running() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // FUNCTION KILL when nothing is running returns NOTBUSY error
                    let result = ctx.raw(&FunctionKillInput::new().command()).await.expect("raw failed");

                    // Should get an error since no function is running
                    let err = FunctionKillOutput::decode(&result);
                    assert!(err.is_err(), "FUNCTION KILL should error when nothing running");
                    assert!(err.unwrap_err().to_string().contains("NOTBUSY"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_kill_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&FunctionKillInput::new().command()).await.expect("raw failed");

            // Should return NOTBUSY error
            assert!(result.starts_with(b"-NOTBUSY"), "RESP2 error format");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_kill_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&FunctionKillInput::new().command()).await.expect("raw failed");

            // Should return NOTBUSY error
            assert!(result.starts_with(b"-NOTBUSY"), "RESP3 error format");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_kill_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Pipeline: FUNCTION KILL + FUNCTION KILL
                    // Both should fail with NOTBUSY
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&FunctionKillInput::new().command());
                    pipeline.extend_from_slice(&FunctionKillInput::new().command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    // Both should be errors
                    let err1 = FunctionKillOutput::decode(responses[0]);
                    assert!(err1.is_err());

                    let err2 = FunctionKillOutput::decode(responses[1]);
                    assert!(err2.is_err());
                })
            })
            .await;
        }
    }
}
