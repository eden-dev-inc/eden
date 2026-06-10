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

const API_INFO: ApiInfo<RedisApi, ScriptExistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ScriptExists,
    "Determines whether server-side Lua scripts exist in the script cache",
    ReqType::Read,
    true,
);

/// Input for Redis `SCRIPT EXISTS` command.
///
/// Returns information about the existence of the scripts in the script cache.
///
/// See official Redis documentation for `SCRIPT EXISTS`:
/// https://redis.io/docs/latest/commands/script-exists/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScriptExistsInput {
    /// One or more SHA1 digests of scripts to check
    pub(crate) sha: Vec<RedisJsonValue>,
}

impl Serialize for ScriptExistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptExistsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("sha", &self.sha)?;
        state.end()
    }
}

impl_redis_operation!(ScriptExistsInput, API_INFO, { sha });

impl RedisCommandInput for ScriptExistsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for sha in &self.sha {
            command.arg(sha);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SCRIPT EXISTS requires at least 1 argument".to_string()));
        }

        Ok(Self { sha: args })
    }
}

/// Output for Redis `SCRIPT EXISTS` command.
///
/// Returns an array of integers corresponding to the specified SHA1 digests.
/// For each SHA1, the return value is 1 if the script exists, 0 otherwise.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScriptExistsOutput {
    /// Results for each SHA1 in the same order as requested (true = exists, false = missing)
    results: Vec<bool>,
}

impl ScriptExistsOutput {
    pub fn new(results: Vec<bool>) -> Self {
        Self { results }
    }

    /// Get the results array
    pub fn results(&self) -> &[bool] {
        &self.results
    }

    /// Check if a script exists at the given index
    pub fn exists(&self, index: usize) -> bool {
        self.results.get(index).copied().unwrap_or(false)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Check if all scripts exist
    pub fn all_exist(&self) -> bool {
        self.results.iter().all(|&exists| exists)
    }

    /// Check if any script exists
    pub fn any_exist(&self) -> bool {
        self.results.iter().any(|&exists| exists)
    }

    /// Decode the Redis protocol response into a ScriptExistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(i == 1),
                        other => Err(EpError::parse(format!("unexpected value in SCRIPT EXISTS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SCRIPT EXISTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(data == 1),
                        other => Err(EpError::parse(format!("unexpected value in SCRIPT EXISTS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SCRIPT EXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for ScriptExistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScriptExistsOutput", 1)?;
        state.serialize_field("results", &self.results)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_sha() {
            let input = ScriptExistsInput {
                sha: vec![RedisJsonValue::String("a42059b356c875f0717db19a51f6aaa9161e77a2".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(40).any(|w| w == b"a42059b356c875f0717db19a51f6aaa9161e77a2"));
        }

        #[test]
        fn test_encode_command_multiple_shas() {
            let input = ScriptExistsInput {
                sha: vec![RedisJsonValue::String("sha1".into()), RedisJsonValue::String("sha2".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"sha1"));
            assert!(cmd.windows(4).any(|w| w == b"sha2"));
        }

        #[test]
        fn test_decode_all_exist() {
            // RESP2 array with all 1s
            let output = ScriptExistsOutput::decode(b"*2\r\n:1\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!(output.exists(0));
            assert!(output.exists(1));
            assert!(output.all_exist());
        }

        #[test]
        fn test_decode_none_exist() {
            // RESP2 array with all 0s
            let output = ScriptExistsOutput::decode(b"*2\r\n:0\r\n:0\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!(!output.exists(0));
            assert!(!output.exists(1));
            assert!(!output.any_exist());
        }

        #[test]
        fn test_decode_mixed() {
            // RESP2 array with mixed results
            let output = ScriptExistsOutput::decode(b"*3\r\n:1\r\n:0\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.exists(0));
            assert!(!output.exists(1));
            assert!(output.exists(2));
            assert!(!output.all_exist());
            assert!(output.any_exist());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ScriptExistsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let result = ScriptExistsInput::decode(vec![]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_valid() {
            let result = ScriptExistsInput::decode(vec![RedisJsonValue::String("sha1".into())]);
            assert!(result.is_ok());
            let input = result.unwrap();
            assert_eq!(input.sha.len(), 1);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScriptExistsInput { sha: vec![RedisJsonValue::String("sha1".into())] };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ScriptExistsInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ScriptExists);
        }

        #[test]
        fn test_serialize_input() {
            let input = ScriptExistsInput { sha: vec![RedisJsonValue::String("testsha".into())] };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("testsha"));
        }

        #[test]
        fn test_serialize_output() {
            let output = ScriptExistsOutput::new(vec![true, false]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"results\":[true,false]"));
        }

        #[test]
        fn test_exists_out_of_bounds() {
            let output = ScriptExistsOutput::new(vec![true]);
            assert!(!output.exists(5)); // Out of bounds returns false
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::scripting_and_functions::script::script_load::{ScriptLoadInput, ScriptLoadOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_not_found() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ScriptExistsInput {
                                sha: vec![RedisJsonValue::String("0000000000000000000000000000000000000000".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ScriptExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert!(!output.exists(0), "non-existent script should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_after_load() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First load a script
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command())
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha").to_string();

                    // Now check if it exists
                    let result =
                        ctx.raw(&ScriptExistsInput { sha: vec![RedisJsonValue::String(sha)] }.command()).await.expect("raw failed");

                    let output = ScriptExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert!(output.exists(0), "loaded script should exist");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_multiple() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load a script
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 42".into()) }.command())
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha").to_string();

                    // Check multiple SHAs (one valid, one invalid)
                    let result = ctx
                        .raw(
                            &ScriptExistsInput {
                                sha: vec![
                                    RedisJsonValue::String(sha),
                                    RedisJsonValue::String("0000000000000000000000000000000000000000".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ScriptExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert!(output.exists(0), "loaded script should exist");
                    assert!(!output.exists(1), "non-existent script should not exist");
                    assert!(!output.all_exist());
                    assert!(output.any_exist());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load a script
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 'test'".into()) }.command())
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha").to_string();

                    // Pipeline multiple SCRIPT EXISTS calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ScriptExistsInput { sha: vec![RedisJsonValue::String(sha)] }.command());
                    pipeline.extend_from_slice(
                        &ScriptExistsInput {
                            sha: vec![RedisJsonValue::String("0000000000000000000000000000000000000000".into())],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = ScriptExistsOutput::decode(responses[0]).expect("decode first");
                    assert!(output1.exists(0));

                    let output2 = ScriptExistsOutput::decode(responses[1]).expect("decode second");
                    assert!(!output2.exists(0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &ScriptExistsInput {
                        sha: vec![RedisJsonValue::String("0000000000000000000000000000000000000000".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ScriptExistsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_exists_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &ScriptExistsInput {
                        sha: vec![RedisJsonValue::String("0000000000000000000000000000000000000000".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = ScriptExistsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            ctx.stop().await;
        }
    }
}
