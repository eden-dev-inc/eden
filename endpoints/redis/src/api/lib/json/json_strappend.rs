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

const API_INFO: ApiInfo<RedisApi, JsonStrappendInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonStrappend,
    "Appends a string to a JSON string value at path",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.STRAPPEND`
/// https://redis.io/docs/latest/commands/json.strappend/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonStrappendInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
    value: RedisJsonValue,
}

impl Serialize for JsonStrappendInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonStrappendInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    JsonStrappendInput,
    API_INFO,
    {key, path, value}
);

impl RedisCommandInput for JsonStrappendInput {
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
        command.arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("JSON.STRAPPEND requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;

        let (path, value) = if args.len() == 2 {
            (None, args[1].clone())
        } else {
            // 3+ args: key, path, value
            (Some(args[1].clone()), args[2].clone())
        };

        Ok(Self { key, path, value })
    }
}

/// Output for Redis JSON.STRAPPEND command
///
/// Returns an array of integer replies for each path,
/// representing the new length of the string after appending.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonStrappendOutput {
    lengths: Vec<Option<i64>>,
}

impl JsonStrappendOutput {
    pub fn new(lengths: Vec<Option<i64>>) -> Self {
        Self { lengths }
    }

    pub fn lengths(&self) -> &[Option<i64>] {
        &self.lengths
    }

    pub fn first(&self) -> Option<Option<i64>> {
        self.lengths.first().copied()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let lengths = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut lengths = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::Integer(n) => lengths.push(Some(n)),
                            Resp2Frame::Null => lengths.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    lengths
                }
                Resp2Frame::Integer(n) => vec![Some(n)],
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.STRAPPEND response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut lengths = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::Number { data, .. } => lengths.push(Some(data)),
                            Resp3Frame::Null => lengths.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    lengths
                }
                Resp3Frame::Number { data, .. } => vec![Some(data)],
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.STRAPPEND response: {:?}", other)));
                }
            },
        };

        Ok(Self { lengths })
    }
}

impl Serialize for JsonStrappendOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonStrappendOutput", 1)?;
        state.serialize_field("lengths", &self.lengths)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonStrappendInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.str".into())),
                value: RedisJsonValue::String(r#""appended""#.into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$14\r\nJSON.STRAPPEND\r\n"));
        }

        #[test]
        fn test_encode_command_without_path() {
            let input = JsonStrappendInput {
                key: RedisKey::String("mykey".into()),
                path: None,
                value: RedisJsonValue::String(r#""appended""#.into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$14\r\nJSON.STRAPPEND\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonStrappendOutput::decode(b"*1\r\n:10\r\n").unwrap();
            assert_eq!(output.lengths().len(), 1);
            assert_eq!(output.lengths()[0], Some(10));
        }

        #[test]
        fn test_decode_output_array_multiple() {
            let output = JsonStrappendOutput::decode(b"*3\r\n:5\r\n:10\r\n$-1\r\n").unwrap();
            assert_eq!(output.lengths().len(), 3);
            assert_eq!(output.lengths()[0], Some(5));
            assert_eq!(output.lengths()[1], Some(10));
            assert_eq!(output.lengths()[2], None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonStrappendOutput::decode(b"-ERR not a string\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_two_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String(r#""value""#.into())];
            let input = JsonStrappendInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_three_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.path".into()),
                RedisJsonValue::String(r#""value""#.into()),
            ];
            let input = JsonStrappendInput::decode(args).unwrap();
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = JsonStrappendInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonStrappendInput {
                key: RedisKey::String("testkey".into()),
                path: None,
                value: RedisJsonValue::String(r#""x""#.into()),
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
        async fn test_json_strappend_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("strkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"msg":"hello"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonStrappendInput {
                                key: RedisKey::String("strkey".into()),
                                path: Some(RedisJsonValue::String("$.msg".into())),
                                value: RedisJsonValue::String(r#"" world""#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("strappend failed");

                    let output = JsonStrappendOutput::decode(&result).expect("decode failed");
                    // "hello world" = 11 chars
                    assert_eq!(output.first(), Some(Some(11)));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strappend_multiple_paths() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("multistr".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":{"s":"x"},"b":{"s":"y"}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonStrappendInput {
                                key: RedisKey::String("multistr".into()),
                                path: Some(RedisJsonValue::String("$..s".into())),
                                value: RedisJsonValue::String(r#""z""#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("strappend failed");

                    let output = JsonStrappendOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.lengths().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strappend_non_string() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("numkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"val":123}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonStrappendInput {
                                key: RedisKey::String("numkey".into()),
                                path: Some(RedisJsonValue::String("$.val".into())),
                                value: RedisJsonValue::String(r#""text""#.into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("strappend failed");

                    let output = JsonStrappendOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(None));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strappend_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2str".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"s":"a"}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonStrappendInput {
                        key: RedisKey::String("r2str".into()),
                        path: Some(RedisJsonValue::String("$.s".into())),
                        value: RedisJsonValue::String(r#""b""#.into()),
                    }
                    .command(),
                )
                .await
                .expect("strappend failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strappend_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3str".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"s":"a"}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonStrappendInput {
                        key: RedisKey::String("r3str".into()),
                        path: Some(RedisJsonValue::String("$.s".into())),
                        value: RedisJsonValue::String(r#""b""#.into()),
                    }
                    .command(),
                )
                .await
                .expect("strappend failed");

            let output = JsonStrappendOutput::decode(&result).expect("decode failed");
            assert_eq!(output.first(), Some(Some(2)));

            ctx.stop().await;
        }
    }
}
