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

const API_INFO: ApiInfo<RedisApi, JsonClearInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonClear,
    "Clears all values from an array or an object and sets numeric values to '0'",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.CLEAR`
/// https://redis.io/docs/latest/commands/json.clear/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonClearInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonClearInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;

        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonClearInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonClearInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonClearInput {
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
            return Err(EpError::request("JSON.CLEAR requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.CLEAR command
///
/// Returns the number of values cleared.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonClearOutput {
    cleared: i64,
}

impl JsonClearOutput {
    pub fn new(cleared: i64) -> Self {
        Self { cleared }
    }

    pub fn cleared(&self) -> i64 {
        self.cleared
    }

    pub fn was_cleared(&self) -> bool {
        self.cleared > 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let cleared = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.CLEAR response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.CLEAR response: {:?}", other)));
                }
            },
        };

        Ok(Self { cleared })
    }
}

impl Serialize for JsonClearOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonClearOutput", 1)?;
        state.serialize_field("cleared", &self.cleared)?;
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
            let input = JsonClearInput { key: RedisKey::String("mykey".into()), path: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$10\r\nJSON.CLEAR\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonClearInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.arr".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$10\r\nJSON.CLEAR\r\n"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = JsonClearOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.cleared(), 0);
            assert!(!output.was_cleared());
        }

        #[test]
        fn test_decode_output_one() {
            let output = JsonClearOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.cleared(), 1);
            assert!(output.was_cleared());
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = JsonClearOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.cleared(), 3);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonClearOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonClearInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_with_path() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.path".into())];
            let input = JsonClearInput::decode(args).unwrap();
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonClearInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonClearInput { key: RedisKey::String("testkey".into()), path: None };
            assert_eq!(input.keys().len(), 1);
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
        async fn test_json_clear_array() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("cleararray".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[1,2,3,4,5]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonClearInput {
                                key: RedisKey::String("cleararray".into()),
                                path: Some(RedisJsonValue::String("$.arr".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("clear failed");

                    let output = JsonClearOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cleared(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_clear_object() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("clearobj".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"nested":{"a":1,"b":2}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonClearInput {
                                key: RedisKey::String("clearobj".into()),
                                path: Some(RedisJsonValue::String("$.nested".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("clear failed");

                    let output = JsonClearOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cleared(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_clear_number() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("clearnum".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"count":42}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonClearInput {
                                key: RedisKey::String("clearnum".into()),
                                path: Some(RedisJsonValue::String("$.count".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("clear failed");

                    let output = JsonClearOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cleared(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_clear_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonClearInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("clear failed");

                    // Redis Stack returns error for nonexistent key
                    let output = JsonClearOutput::decode(&result);
                    assert!(output.is_err() || output.as_ref().is_ok_and(|o| o.cleared() == 0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_clear_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2clear".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"arr":[1,2,3]}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonClearInput {
                        key: RedisKey::String("r2clear".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                    }
                    .command(),
                )
                .await
                .expect("clear failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_clear_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3clear".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"arr":[1,2,3]}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonClearInput {
                        key: RedisKey::String("r3clear".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                    }
                    .command(),
                )
                .await
                .expect("clear failed");

            let output = JsonClearOutput::decode(&result).expect("decode failed");
            assert_eq!(output.cleared(), 1);
            ctx.stop().await;
        }
    }
}
