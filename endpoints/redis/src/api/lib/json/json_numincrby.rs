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

const API_INFO: ApiInfo<RedisApi, JsonNumincrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonNumincrby,
    "Increments the numeric value at path by a value",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.NUMINCRBY`
/// https://redis.io/docs/latest/commands/json.numincrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonNumincrbyInput {
    key: RedisKey,
    path: RedisJsonValue,
    value: RedisJsonValue,
}

impl Serialize for JsonNumincrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonNumincrbyInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    JsonNumincrbyInput,
    API_INFO,
    {key, path, value}
);

impl RedisCommandInput for JsonNumincrbyInput {
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
            return Err(EpError::request(format!("JSON.NUMINCRBY requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            path: args[1].clone(),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis JSON.NUMINCRBY command
///
/// Returns a bulk string containing a JSON array of the new values.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonNumincrbyOutput {
    /// JSON string containing the result array
    value: Option<String>,
}

impl JsonNumincrbyOutput {
    pub fn new(value: Option<String>) -> Self {
        Self { value }
    }

    /// Get the raw JSON result string
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Check if the operation returned a value
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Some(String::from_utf8_lossy(&data).to_string()),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.NUMINCRBY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8_lossy(&data).to_string()),
                Resp3Frame::Array { data, .. } => {
                    // Redis Stack RESP3 returns array of numbers
                    let values: Vec<String> = data
                        .iter()
                        .map(|f| match f {
                            Resp3Frame::Number { data, .. } => data.to_string(),
                            Resp3Frame::Double { data, .. } => data.to_string(),
                            Resp3Frame::Null => "null".to_string(),
                            _ => format!("{:?}", f),
                        })
                        .collect();
                    Some(format!("[{}]", values.join(",")))
                }
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.NUMINCRBY response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for JsonNumincrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonNumincrbyOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = JsonNumincrbyInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.count".into()),
                value: RedisJsonValue::Integer(5),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$14\r\nJSON.NUMINCRBY\r\n"));
        }

        #[test]
        fn test_encode_command_negative() {
            let input = JsonNumincrbyInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.count".into()),
                value: RedisJsonValue::Integer(-10),
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"-10"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = JsonNumincrbyOutput::decode(b"$3\r\n[5]\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some("[5]"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonNumincrbyOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonNumincrbyOutput::decode(b"-ERR not a number\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.val".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = JsonNumincrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_wrong_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.val".into())];
            let err = JsonNumincrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonNumincrbyInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::Integer(1),
            };
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
        async fn test_json_numincrby_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("incrkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"count":10}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonNumincrbyInput {
                                key: RedisKey::String("incrkey".into()),
                                path: RedisJsonValue::String("$.count".into()),
                                value: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("numincrby failed");

                    let output = JsonNumincrbyOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.value().unwrap().contains("15"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_numincrby_negative() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("decrkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"count":100}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonNumincrbyInput {
                                key: RedisKey::String("decrkey".into()),
                                path: RedisJsonValue::String("$.count".into()),
                                value: RedisJsonValue::Integer(-30),
                            }
                            .command(),
                        )
                        .await
                        .expect("numincrby failed");

                    let output = JsonNumincrbyOutput::decode(&result).expect("decode failed");
                    assert!(output.value().unwrap().contains("70"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_numincrby_float() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("floatkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"val":1.5}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonNumincrbyInput {
                                key: RedisKey::String("floatkey".into()),
                                path: RedisJsonValue::String("$.val".into()),
                                value: RedisJsonValue::String("0.5".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("numincrby failed");

                    let output = JsonNumincrbyOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_numincrby_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("nopath".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"other":1}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonNumincrbyInput {
                                key: RedisKey::String("nopath".into()),
                                path: RedisJsonValue::String("$.missing".into()),
                                value: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("numincrby failed");

                    let output = JsonNumincrbyOutput::decode(&result).expect("decode failed");
                    // Returns array with empty or null for missing path
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_numincrby_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2incr".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"n":0}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonNumincrbyInput {
                        key: RedisKey::String("r2incr".into()),
                        path: RedisJsonValue::String("$.n".into()),
                        value: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("numincrby failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_numincrby_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3incr".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"n":0}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonNumincrbyInput {
                        key: RedisKey::String("r3incr".into()),
                        path: RedisJsonValue::String("$.n".into()),
                        value: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("numincrby failed");

            let output = JsonNumincrbyOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }
    }
}
