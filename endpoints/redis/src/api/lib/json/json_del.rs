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

const API_INFO: ApiInfo<RedisApi, JsonDelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonDel, "Deletes a value at the specified path", ReqType::Write, true);

/// See official Redis documentation for `JSON.DEL`
/// https://redis.io/docs/latest/commands/json.del/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonDelInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonDelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;

        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonDelInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonDelInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonDelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);
        if let Some(path) = &self.path {
            command.arg(path);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("JSON.DEL requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.DEL command
///
/// Returns the number of paths deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonDelOutput {
    /// The number of paths that were deleted
    deleted: i64,
}

impl JsonDelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of deleted paths
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Returns true if at least one path was deleted
    pub fn was_deleted(&self) -> bool {
        self.deleted > 0
    }

    /// Decode the Redis protocol response into a JsonDelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.DEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.DEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for JsonDelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonDelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_key_only() {
            let input = JsonDelInput { key: RedisKey::String("mykey".into()), path: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$8\r\nJSON.DEL\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonDelInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.field".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$8\r\nJSON.DEL\r\n"));
            assert!(cmd.windows(7).any(|w| w == b"$.field"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = JsonDelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
            assert!(!output.was_deleted());
        }

        #[test]
        fn test_decode_output_one() {
            let output = JsonDelOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.deleted(), 1);
            assert!(output.was_deleted());
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = JsonDelOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.deleted(), 5);
            assert!(output.was_deleted());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonDelOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonDelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_with_path() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.path".into())];
            let input = JsonDelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonDelInput { key: RedisKey::String("testkey".into()), path: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::json::json_get::{JsonGetInput, JsonGetOutput};
        use crate::api::lib::json::json_set::JsonSetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_entire_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set a JSON document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("delkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"foo":"bar"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Delete the entire document
                    let result =
                        ctx.raw(&JsonDelInput { key: RedisKey::String("delkey".into()), path: None }.command()).await.expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);
                    assert!(output.was_deleted());

                    // Verify key is gone
                    let get_result = ctx
                        .raw(
                            &JsonGetInput {
                                key: RedisKey::String("delkey".into()),
                                path: None,
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("get failed");

                    let get_output = JsonGetOutput::decode(&get_result).expect("decode get");
                    assert!(get_output.is_nil(), "key should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_specific_path() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set a JSON document with multiple fields
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("pathdelkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"keep":"this","remove":"that"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Delete only the "remove" field
                    let result = ctx
                        .raw(
                            &JsonDelInput {
                                key: RedisKey::String("pathdelkey".into()),
                                path: Some(RedisJsonValue::String("$.remove".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_nonexistent_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&JsonDelInput { key: RedisKey::String("noexist".into()), path: None }.command()).await.expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                    assert!(!output.was_deleted());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_nonexistent_path() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set a document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("pathnoexist".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"existing":"field"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Try to delete nonexistent path
                    let result = ctx
                        .raw(
                            &JsonDelInput {
                                key: RedisKey::String("pathnoexist".into()),
                                path: Some(RedisJsonValue::String("$.nonexistent".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_array_elements() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set a document with array
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("arraydelkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"items":[1,2,3,4,5]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Delete array elements matching a pattern
                    let result = ctx
                        .raw(
                            &JsonDelInput {
                                key: RedisKey::String("arraydelkey".into()),
                                path: Some(RedisJsonValue::String("$.items[2]".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_wildcard_path() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set a document with nested objects
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("wildcardkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":{"x":1},"b":{"x":2},"c":{"x":3}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Delete all "x" fields using wildcard
                    let result = ctx
                        .raw(
                            &JsonDelInput {
                                key: RedisKey::String("wildcardkey".into()),
                                path: Some(RedisJsonValue::String("$..x".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("del failed");

                    let output = JsonDelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2delkey".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result =
                ctx.raw(&JsonDelInput { key: RedisKey::String("r2delkey".into()), path: None }.command()).await.expect("del failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = JsonDelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_del_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3delkey".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result =
                ctx.raw(&JsonDelInput { key: RedisKey::String("r3delkey".into()), path: None }.command()).await.expect("del failed");

            let output = JsonDelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);

            ctx.stop().await;
        }
    }
}
