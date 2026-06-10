use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, JsonMergeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonMerge,
    "Merges a given JSON value into matching paths",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.MERGE`
/// https://redis.io/docs/latest/commands/json.merge/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonMergeInput {
    key: RedisKey,
    path: RedisJsonValue,
    value: RedisJsonValue,
}

impl Serialize for JsonMergeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMergeInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    JsonMergeInput,
    API_INFO,
    {key, path, value}
);

impl RedisCommandInput for JsonMergeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.path).arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("JSON.MERGE requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            path: args[1].clone(),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis JSON.MERGE command
///
/// Returns OK if the merge was successful.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonMergeOutput {
    success: bool,
}

impl JsonMergeOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if the merge was successful
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a JsonMergeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.as_slice() == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected JSON.MERGE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.as_slice() == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected JSON.MERGE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for JsonMergeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMergeOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = JsonMergeInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String(r#"{"new":"value"}"#.into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$10\r\nJSON.MERGE\r\n"));
        }

        #[test]
        fn test_encode_command_nested_path() {
            let input = JsonMergeInput {
                key: RedisKey::String("doc".into()),
                path: RedisJsonValue::String("$.config".into()),
                value: RedisJsonValue::String(r#"{"setting":true}"#.into()),
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"$.config"));
        }

        #[test]
        fn test_decode_output_ok_resp2() {
            let output = JsonMergeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_ok_resp3() {
            let output = JsonMergeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonMergeOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String(r#"{"merged":true}"#.into()),
            ];
            let input = JsonMergeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$".into())];
            let err = JsonMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = JsonMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonMergeInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String("{}".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::json::json_set::JsonSetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("mergekey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":1,"b":2}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Merge new data
                    let result = ctx
                        .raw(
                            &JsonMergeInput {
                                key: RedisKey::String("mergekey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"c":3}"#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = JsonMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_overwrite() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("overwritekey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"value":"old"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Merge to overwrite existing field
                    let result = ctx
                        .raw(
                            &JsonMergeInput {
                                key: RedisKey::String("overwritekey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"value":"new"}"#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = JsonMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_nested() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial document with nested structure
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("nestedmerge".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"outer":{"inner":1}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Merge at nested path
                    let result = ctx
                        .raw(
                            &JsonMergeInput {
                                key: RedisKey::String("nestedmerge".into()),
                                path: RedisJsonValue::String("$.outer".into()),
                                value: RedisJsonValue::String(r#"{"added":true}"#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = JsonMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_delete_with_null() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("deletefield".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"keep":1,"remove":2}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Merge null to delete field (RFC 7396 behavior)
                    let result = ctx
                        .raw(
                            &JsonMergeInput {
                                key: RedisKey::String("deletefield".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"remove":null}"#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("merge failed");

                    let output = JsonMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_nonexistent_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Attempt to merge into nonexistent key
                    let result = ctx
                        .raw(
                            &JsonMergeInput {
                                key: RedisKey::String("noexist".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{}"#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should still return OK per Redis behavior
                    let output = JsonMergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2merge".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonMergeInput {
                        key: RedisKey::String("r2merge".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{"test":true}"#.into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return simple string");
            let output = JsonMergeOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_merge_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3merge".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonMergeInput {
                        key: RedisKey::String("r3merge".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{"test":true}"#.into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = JsonMergeOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
