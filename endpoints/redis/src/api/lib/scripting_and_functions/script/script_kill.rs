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

const API_INFO: ApiInfo<RedisApi, ScriptKillInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ScriptKill,
    "Terminates a server-side Lua script during execution",
    ReqType::Write,
    true,
);

/// Input for Redis `SCRIPT KILL` command.
///
/// Kills the currently executing Lua script, assuming no write operation was yet performed
/// by the script. This command is mainly useful to kill a script that is running for too
/// much time.
///
/// See official Redis documentation for `SCRIPT KILL`:
/// https://redis.io/docs/latest/commands/script-kill/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScriptKillInput {}

impl Serialize for ScriptKillInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptKillInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ScriptKillInput, API_INFO);

impl RedisCommandInput for ScriptKillInput {
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
                "SCRIPT KILL expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis `SCRIPT KILL` command.
///
/// Returns OK if the script was killed, or an error if no script was running
/// or the script had already performed write operations.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScriptKillOutput {
    /// Whether the script was killed successfully
    success: bool,
    /// Error message if the operation failed
    error: Option<String>,
}

impl ScriptKillOutput {
    pub fn new(success: bool, error: Option<String>) -> Self {
        Self { success, error }
    }

    /// Check if the script was killed successfully
    pub fn success(&self) -> bool {
        self.success
    }

    /// Get the error message if the operation failed
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Check if no script was running
    pub fn no_script_running(&self) -> bool {
        self.error.as_ref().map(|e| e.contains("NOTBUSY")).unwrap_or(false)
    }

    /// Check if the script had already performed writes
    pub fn script_has_writes(&self) -> bool {
        self.error.as_ref().map(|e| e.contains("UNKILLABLE")).unwrap_or(false)
    }

    /// Decode the Redis protocol response into a ScriptKillOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK", error: None })
                }
                Resp2Frame::Error(e) => Ok(Self { success: false, error: Some(e) }),
                other => Err(EpError::parse(format!("unexpected SCRIPT KILL response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK", error: None })
                }
                Resp3Frame::SimpleError { data, .. } => Ok(Self { success: false, error: Some(data) }),
                Resp3Frame::BlobError { data, .. } => Ok(Self {
                    success: false,
                    error: Some(String::from_utf8_lossy(&data).to_string()),
                }),
                other => Err(EpError::parse(format!("unexpected SCRIPT KILL response: {:?}", other))),
            },
        }
    }
}

impl Serialize for ScriptKillOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptKillOutput", 2)?;
        state.serialize_field("success", &self.success)?;
        state.serialize_field("error", &self.error)?;
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
            let input = ScriptKillInput {};
            let cmd = input.command();
            // SCRIPT KILL command format
            assert!(
                cmd.windows(11).any(|w| w == b"SCRIPT KILL")
                    || (cmd.windows(6).any(|w| w == b"SCRIPT") && cmd.windows(4).any(|w| w == b"KILL"))
            );
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ScriptKillOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
            assert!(output.error().is_none());
            assert!(!output.no_script_running());
            assert!(!output.script_has_writes());
        }

        #[test]
        fn test_decode_notbusy_error() {
            let output = ScriptKillOutput::decode(b"-NOTBUSY No scripts in execution right now.\r\n").unwrap();
            assert!(!output.success());
            assert!(output.error().is_some());
            assert!(output.no_script_running());
            assert!(!output.script_has_writes());
        }

        #[test]
        fn test_decode_unkillable_error() {
            let output = ScriptKillOutput::decode(b"-UNKILLABLE Sorry the script already executed write commands.\r\n").unwrap();
            assert!(!output.success());
            assert!(output.error().is_some());
            assert!(!output.no_script_running());
            assert!(output.script_has_writes());
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = ScriptKillInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            // Should succeed but log a warning
            let input = ScriptKillInput::decode(vec![RedisJsonValue::String("extra".into())]);
            assert!(input.is_ok());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScriptKillInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ScriptKillInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ScriptKill);
        }

        #[test]
        fn test_serialize_input() {
            let input = ScriptKillInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"SCRIPT KILL\"") || json.contains("\"type\":\"ScriptKill\""));
        }

        #[test]
        fn test_serialize_output_success() {
            let output = ScriptKillOutput::new(true, None);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
            assert!(json.contains("\"error\":null"));
        }

        #[test]
        fn test_serialize_output_error() {
            let output = ScriptKillOutput::new(false, Some("NOTBUSY".into()));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":false"));
            assert!(json.contains("NOTBUSY"));
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
        async fn test_script_kill_no_script_running() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Try to kill when no script is running
                    let result = ctx.raw(&ScriptKillInput {}.command()).await.expect("raw failed");

                    let output = ScriptKillOutput::decode(&result).expect("decode failed");
                    // Should fail with NOTBUSY error
                    assert!(!output.success());
                    assert!(output.no_script_running(), "should report no script running");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_kill_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Pipeline multiple SCRIPT KILL calls (all should fail with NOTBUSY)
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ScriptKillInput {}.command());
                    pipeline.extend_from_slice(&ScriptKillInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = ScriptKillOutput::decode(responses[0]).expect("decode first");
                    assert!(!output1.success());
                    assert!(output1.no_script_running());

                    let output2 = ScriptKillOutput::decode(responses[1]).expect("decode second");
                    assert!(!output2.success());
                    assert!(output2.no_script_running());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_kill_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ScriptKillInput {}.command()).await.expect("raw failed");

            // Should be an error response (starts with -)
            assert!(result.starts_with(b"-"), "RESP2 error should start with -");
            let output = ScriptKillOutput::decode(&result).expect("decode failed");
            assert!(!output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_kill_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ScriptKillInput {}.command()).await.expect("raw failed");

            // Should be an error response
            let output = ScriptKillOutput::decode(&result).expect("decode failed");
            assert!(!output.success());
            assert!(output.no_script_running());
            ctx.stop().await;
        }
    }
}
