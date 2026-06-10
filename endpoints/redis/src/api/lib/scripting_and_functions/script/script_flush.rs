use crate::api::lib::scripting_and_functions::script::FlushMode;
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

const API_INFO: ApiInfo<RedisApi, ScriptFlushInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ScriptFlush,
    "Removes all server-side Lua scripts from the script cache",
    ReqType::Write,
    true,
);

/// Input for Redis `SCRIPT FLUSH` command.
///
/// Flushes the Lua scripts cache. By default, the operation is performed asynchronously.
///
/// See official Redis documentation for `SCRIPT FLUSH`:
/// https://redis.io/docs/latest/commands/script-flush/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScriptFlushInput {
    /// The flush mode: ASYNC (default) or SYNC
    #[serde(default)]
    mode: FlushMode,
}

impl Serialize for ScriptFlushInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptFlushInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("mode", &self.mode)?;
        state.end()
    }
}

impl_redis_operation!(ScriptFlushInput, API_INFO, { mode });

impl RedisCommandInput for ScriptFlushInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        match &self.mode {
            FlushMode::Default => {} // No argument for compatibility with Redis < 6.2
            FlushMode::SYNC => {
                command.arg("SYNC");
            }
            FlushMode::ASYNC => {
                command.arg("ASYNC");
            }
        };

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mode = if args.is_empty() {
            FlushMode::Default
        } else if let RedisJsonValue::String(s) = &args[0] {
            match s.to_uppercase().as_str() {
                "SYNC" => FlushMode::SYNC,
                "ASYNC" => FlushMode::ASYNC,
                _ => FlushMode::Default,
            }
        } else {
            FlushMode::Default
        };

        Ok(Self { mode })
    }
}

/// Output for Redis `SCRIPT FLUSH` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScriptFlushOutput {
    /// Whether the flush was successful
    success: bool,
}

impl ScriptFlushOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the flush was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a ScriptFlushOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SCRIPT FLUSH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SCRIPT FLUSH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for ScriptFlushOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptFlushOutput", 1)?;
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
        fn test_encode_command_async() {
            let input = ScriptFlushInput { mode: FlushMode::ASYNC };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"ASYNC"));
        }

        #[test]
        fn test_encode_command_sync() {
            let input = ScriptFlushInput { mode: FlushMode::SYNC };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"SYNC"));
        }

        #[test]
        fn test_encode_command_default() {
            let input = ScriptFlushInput::default();
            let cmd = input.command();
            // Default mode should NOT include ASYNC or SYNC for Redis < 6.2 compatibility
            assert!(!cmd.windows(5).any(|w| w == b"ASYNC"));
            assert!(!cmd.windows(4).any(|w| w == b"SYNC"));
            // Should just be SCRIPT FLUSH with no additional arguments
            assert!(cmd.windows(6).any(|w| w == b"SCRIPT"));
            assert!(cmd.windows(5).any(|w| w == b"FLUSH"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ScriptFlushOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ScriptFlushOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = ScriptFlushInput::decode(vec![]).unwrap();
            assert_eq!(input.mode, FlushMode::Default);
        }

        #[test]
        fn test_decode_input_async() {
            let input = ScriptFlushInput::decode(vec![RedisJsonValue::String("ASYNC".into())]).unwrap();
            assert_eq!(input.mode, FlushMode::ASYNC);
        }

        #[test]
        fn test_decode_input_sync() {
            let input = ScriptFlushInput::decode(vec![RedisJsonValue::String("SYNC".into())]).unwrap();
            assert_eq!(input.mode, FlushMode::SYNC);
        }

        #[test]
        fn test_decode_input_case_insensitive() {
            let input = ScriptFlushInput::decode(vec![RedisJsonValue::String("sync".into())]).unwrap();
            assert_eq!(input.mode, FlushMode::SYNC);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScriptFlushInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ScriptFlushInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ScriptFlush);
        }

        #[test]
        fn test_serialize_input() {
            let input = ScriptFlushInput { mode: FlushMode::SYNC };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"mode\":\"SYNC\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = ScriptFlushOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{ScriptExistsInput, ScriptExistsOutput, ScriptLoadInput, ScriptLoadOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_default() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ScriptFlushInput::default().command()).await.expect("raw failed");

                    let output = ScriptFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SCRIPT FLUSH (default) should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_sync() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ScriptFlushInput { mode: FlushMode::SYNC }.command()).await.expect("raw failed");

                    let output = ScriptFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SCRIPT FLUSH SYNC should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_async() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ScriptFlushInput { mode: FlushMode::ASYNC }.command()).await.expect("raw failed");

                    let output = ScriptFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SCRIPT FLUSH ASYNC should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_clears_cache() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load a script first
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command())
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha").to_string();

                    // Verify script exists
                    let exists_result =
                        ctx.raw(&ScriptExistsInput { sha: vec![RedisJsonValue::String(sha.clone())] }.command()).await.expect("raw failed");

                    let exists_output = ScriptExistsOutput::decode(&exists_result).expect("decode failed");
                    assert!(exists_output.exists(0), "script should exist before flush");

                    // Flush scripts
                    let flush_result = ctx.raw(&ScriptFlushInput::default().command()).await.expect("raw failed");

                    let flush_output = ScriptFlushOutput::decode(&flush_result).expect("decode failed");
                    assert!(flush_output.success());

                    // Verify script no longer exists
                    let exists_result =
                        ctx.raw(&ScriptExistsInput { sha: vec![RedisJsonValue::String(sha)] }.command()).await.expect("raw failed");

                    let exists_output = ScriptExistsOutput::decode(&exists_result).expect("decode failed");
                    assert!(!exists_output.exists(0), "script should not exist after flush");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ScriptFlushInput::default().command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = ScriptFlushOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_flush_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ScriptFlushInput::default().command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = ScriptFlushOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }
    }
}
