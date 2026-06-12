use crate::api::lib::scripting_and_functions::script::DebugMode;
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
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ScriptDebugInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ScriptDebug,
    "Sets the debug mode of server-side Lua scripts",
    ReqType::Write,
    false,
);

/// Input for Redis `SCRIPT DEBUG` command.
///
/// Sets the debug mode for later scripts executed with EVAL.
///
/// See official Redis documentation for `SCRIPT DEBUG`:
/// https://redis.io/docs/latest/commands/script-debug/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScriptDebugInput {
    /// The debug mode to set
    mode: DebugMode,
}

impl Serialize for ScriptDebugInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptDebugInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("mode", &self.mode)?;
        state.end()
    }
}

impl_redis_operation!(ScriptDebugInput, API_INFO, { mode });

impl RedisCommandInput for ScriptDebugInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        match &self.mode {
            DebugMode::YES => command.arg("YES"),
            DebugMode::SYNC => command.arg("SYNC"),
            DebugMode::NO => command.arg("NO"),
        };

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SCRIPT DEBUG requires 1 argument".to_string()));
        }

        let mode = if let RedisJsonValue::String(s) = &args[0] {
            match s.to_uppercase().as_str() {
                "YES" => DebugMode::YES,
                "SYNC" => DebugMode::SYNC,
                "NO" => DebugMode::NO,
                other => {
                    return Err(EpError::request(format!("Invalid SCRIPT DEBUG mode: {}. Must be YES, SYNC, or NO", other)));
                }
            }
        } else {
            return Err(EpError::request("SCRIPT DEBUG mode must be a string".to_string()));
        };

        Ok(Self { mode })
    }
}

/// Output for Redis `SCRIPT DEBUG` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScriptDebugOutput {
    /// Whether the debug mode was set successfully
    success: bool,
}

impl ScriptDebugOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a ScriptDebugOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SCRIPT DEBUG response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SCRIPT DEBUG response: {:?}", other))),
            },
        }
    }
}

impl Serialize for ScriptDebugOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptDebugOutput", 1)?;
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
        fn test_encode_command_yes() {
            let input = ScriptDebugInput { mode: DebugMode::YES };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"YES"));
        }

        #[test]
        fn test_encode_command_sync() {
            let input = ScriptDebugInput { mode: DebugMode::SYNC };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"SYNC"));
        }

        #[test]
        fn test_encode_command_no() {
            let input = ScriptDebugInput { mode: DebugMode::NO };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"NO"));
        }

        #[test]
        fn test_encode_command_default() {
            let input = ScriptDebugInput::default();
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"NO"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ScriptDebugOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ScriptDebugOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let result = ScriptDebugInput::decode(vec![]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_yes() {
            let input = ScriptDebugInput::decode(vec![RedisJsonValue::String("YES".into())]).unwrap();
            assert_eq!(input.mode, DebugMode::YES);
        }

        #[test]
        fn test_decode_input_sync() {
            let input = ScriptDebugInput::decode(vec![RedisJsonValue::String("SYNC".into())]).unwrap();
            assert_eq!(input.mode, DebugMode::SYNC);
        }

        #[test]
        fn test_decode_input_no() {
            let input = ScriptDebugInput::decode(vec![RedisJsonValue::String("NO".into())]).unwrap();
            assert_eq!(input.mode, DebugMode::NO);
        }

        #[test]
        fn test_decode_input_case_insensitive() {
            let input = ScriptDebugInput::decode(vec![RedisJsonValue::String("yes".into())]).unwrap();
            assert_eq!(input.mode, DebugMode::YES);
        }

        #[test]
        fn test_decode_input_invalid_mode() {
            let result = ScriptDebugInput::decode(vec![RedisJsonValue::String("INVALID".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_non_string() {
            let result = ScriptDebugInput::decode(vec![RedisJsonValue::Integer(1)]);
            assert!(result.is_err());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScriptDebugInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ScriptDebugInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ScriptDebug);
        }

        #[test]
        fn test_serialize_input() {
            let input = ScriptDebugInput { mode: DebugMode::YES };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"mode\":\"YES\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = ScriptDebugOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: SCRIPT DEBUG requires a connected debugger client.
        // These tests verify the command syntax and response format,
        // but actual debugging functionality requires a debugger client.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_debug_no() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ScriptDebugInput { mode: DebugMode::NO }.command()).await.expect("raw failed");

                    let output = ScriptDebugOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SCRIPT DEBUG NO should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_debug_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ScriptDebugInput { mode: DebugMode::NO }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = ScriptDebugOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_debug_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ScriptDebugInput { mode: DebugMode::NO }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = ScriptDebugOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_debug_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Pipeline: DEBUG NO + DEBUG NO
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ScriptDebugInput { mode: DebugMode::NO }.command());
                    pipeline.extend_from_slice(&ScriptDebugInput { mode: DebugMode::NO }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = ScriptDebugOutput::decode(responses[0]).expect("decode first");
                    assert!(output1.success());

                    // Second response may fail with pipeline error in some Redis versions
                    // Redis limitation: "ERR SCRIPT DEBUG must be called outside a pipeline"
                    match ScriptDebugOutput::decode(responses[1]) {
                        Ok(output) => assert!(output.success()),
                        Err(e) => {
                            // Expected error in Redis versions that don't support SCRIPT DEBUG in pipelines
                            assert!(e.to_string().contains("must be called outside a pipeline"), "Unexpected error: {}", e);
                        }
                    }
                })
            })
            .await;
        }
    }
}
