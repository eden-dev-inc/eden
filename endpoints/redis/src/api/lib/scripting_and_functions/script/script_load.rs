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

const API_INFO: ApiInfo<RedisApi, ScriptLoadInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ScriptLoad,
    "Loads a server-side Lua script to the script cache",
    ReqType::Write,
    true,
);

/// Input for Redis `SCRIPT LOAD` command.
///
/// Loads a script into the scripts cache without executing it. After the script is loaded,
/// it can be called using EVALSHA with its SHA1 digest.
///
/// See official Redis documentation for `SCRIPT LOAD`:
/// https://redis.io/docs/latest/commands/script-load/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScriptLoadInput {
    /// The Lua script to load
    pub(crate) script: RedisJsonValue,
}

impl Default for ScriptLoadInput {
    fn default() -> Self {
        Self { script: RedisJsonValue::String(String::new()) }
    }
}

impl Serialize for ScriptLoadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptLoadInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("script", &self.script.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ScriptLoadInput, API_INFO, { script });

impl RedisCommandInput for ScriptLoadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.script);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("SCRIPT LOAD requires 1 argument, given {}", args.len())));
        }

        Ok(Self { script: args[0].clone() })
    }
}

/// Output for Redis `SCRIPT LOAD` command.
///
/// Returns the SHA1 digest of the script added into the script cache.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScriptLoadOutput {
    /// The SHA1 digest of the loaded script
    sha: Option<String>,
}

impl ScriptLoadOutput {
    pub fn new(sha: Option<String>) -> Self {
        Self { sha }
    }

    /// Get the SHA1 digest of the loaded script
    pub fn sha(&self) -> Option<&str> {
        self.sha.as_deref()
    }

    /// Check if the script was loaded successfully
    pub fn success(&self) -> bool {
        self.sha.is_some()
    }

    /// Decode the Redis protocol response into a ScriptLoadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let sha = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(s) => Some(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SCRIPT LOAD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SCRIPT LOAD response: {:?}", other)));
                }
            },
        };

        Ok(Self { sha })
    }
}

impl Serialize for ScriptLoadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptLoadOutput", 1)?;
        state.serialize_field("sha", &self.sha)?;
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
            let input = ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"return 1"));
        }

        #[test]
        fn test_decode_sha_response() {
            // RESP2 bulk string with SHA1
            let output = ScriptLoadOutput::decode(b"$40\r\na42059b356c875f0717db19a51f6aaa9161e77a2\r\n").unwrap();
            assert!(output.success());
            assert_eq!(output.sha(), Some("a42059b356c875f0717db19a51f6aaa9161e77a2"));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ScriptLoadOutput::decode(b"-ERR Error compiling script\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let result = ScriptLoadInput::decode(vec![]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_too_many_args_fails() {
            let result = ScriptLoadInput::decode(vec![RedisJsonValue::String("script1".into()), RedisJsonValue::String("script2".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_valid() {
            let result = ScriptLoadInput::decode(vec![RedisJsonValue::String("return 42".into())]);
            assert!(result.is_ok());
            let input = result.unwrap();
            assert_eq!(input.script, RedisJsonValue::String("return 42".into()));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ScriptLoadInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ScriptLoad);
        }

        #[test]
        fn test_serialize_input() {
            let input = ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("return 1"));
        }

        #[test]
        fn test_serialize_output() {
            let output = ScriptLoadOutput::new(Some("abc123".into()));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"sha\":\"abc123\""));
        }

        #[test]
        fn test_default() {
            let input = ScriptLoadInput::default();
            assert_eq!(input.script, RedisJsonValue::String(String::new()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::scripting_and_functions::script::script_exists::{ScriptExistsInput, ScriptExistsOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_simple() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = ScriptLoadOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SCRIPT LOAD should succeed");
                    assert!(output.sha().is_some(), "should return SHA1");
                    assert_eq!(output.sha().unwrap().len(), 40, "SHA1 should be 40 characters");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_deterministic_sha() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load the same script twice
                    let script = "return 'hello'";

                    let result1 =
                        ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String(script.into()) }.command()).await.expect("raw failed");

                    let result2 =
                        ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String(script.into()) }.command()).await.expect("raw failed");

                    let output1 = ScriptLoadOutput::decode(&result1).expect("decode failed");
                    let output2 = ScriptLoadOutput::decode(&result2).expect("decode failed");

                    assert_eq!(output1.sha(), output2.sha(), "same script should produce same SHA1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_different_scripts() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result1 = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command())
                        .await
                        .expect("raw failed");

                    let result2 = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 2".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output1 = ScriptLoadOutput::decode(&result1).expect("decode failed");
                    let output2 = ScriptLoadOutput::decode(&result2).expect("decode failed");

                    assert_ne!(output1.sha(), output2.sha(), "different scripts should produce different SHA1s");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_then_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load a script
                    let result = ctx
                        .raw(
                            &ScriptLoadInput {
                                script: RedisJsonValue::String("return redis.call('PING')".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha");

                    // Verify it exists
                    let exists_result =
                        ctx.raw(&ScriptExistsInput { sha: vec![RedisJsonValue::String(sha.into())] }.command()).await.expect("raw failed");

                    let exists_output = ScriptExistsOutput::decode(&exists_result).expect("decode failed");
                    assert!(exists_output.exists(0), "loaded script should exist");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Pipeline multiple SCRIPT LOAD calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command());
                    pipeline.extend_from_slice(&ScriptLoadInput { script: RedisJsonValue::String("return 2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = ScriptLoadOutput::decode(responses[0]).expect("decode first");
                    assert!(output1.success());

                    let output2 = ScriptLoadOutput::decode(responses[1]).expect("decode second");
                    assert!(output2.success());

                    assert_ne!(output1.sha(), output2.sha());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result =
                ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = ScriptLoadOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            assert_eq!(output.sha().unwrap().len(), 40);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result =
                ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command()).await.expect("raw failed");

            let output = ScriptLoadOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            assert_eq!(output.sha().unwrap().len(), 40);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_load_complex_script() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let complex_script = r#"
                        local key = KEYS[1]
                        local value = ARGV[1]
                        redis.call('SET', key, value)
                        return redis.call('GET', key)
                    "#;

                    let result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String(complex_script.into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = ScriptLoadOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "complex script should load successfully");
                })
            })
            .await;
        }
    }
}
