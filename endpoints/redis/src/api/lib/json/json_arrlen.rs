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

const API_INFO: ApiInfo<RedisApi, JsonArrlenInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonArrlen, "Returns the length of the array at path", ReqType::Read, true);

/// See official Redis documentation for `JSON.ARRLEN`
/// https://redis.io/docs/latest/commands/json.arrlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonArrlenInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonArrlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;

        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonArrlenInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonArrlenInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonArrlenInput {
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
            return Err(EpError::request("JSON.ARRLEN requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.ARRLEN command
///
/// Returns an array of integer replies for each path,
/// or null if the path doesn't contain an array.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonArrlenOutput {
    lengths: Vec<Option<i64>>,
}

impl JsonArrlenOutput {
    pub fn new(lengths: Vec<Option<i64>>) -> Self {
        Self { lengths }
    }

    pub fn lengths(&self) -> &[Option<i64>] {
        &self.lengths
    }

    pub fn first(&self) -> Option<i64> {
        self.lengths.first().and_then(|l| *l)
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
                    return Err(EpError::parse(format!("unexpected JSON.ARRLEN response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected JSON.ARRLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { lengths })
    }
}

impl Serialize for JsonArrlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrlenOutput", 1)?;
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
        fn test_encode_command_key_only() {
            let input = JsonArrlenInput { key: RedisKey::String("mykey".into()), path: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$11\r\nJSON.ARRLEN\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonArrlenInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.arr".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$11\r\nJSON.ARRLEN\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonArrlenOutput::decode(b"*1\r\n:5\r\n").unwrap();
            assert_eq!(output.first(), Some(5));
        }

        #[test]
        fn test_decode_output_single() {
            let output = JsonArrlenOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.first(), Some(3));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonArrlenOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonArrlenOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonArrlenInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_with_path() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.arr".into())];
            let input = JsonArrlenInput::decode(args).unwrap();
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonArrlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonArrlenInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_arrlen_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("lenkey".into()),
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
                            &JsonArrlenInput {
                                key: RedisKey::String("lenkey".into()),
                                path: Some(RedisJsonValue::String("$.arr".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrlen failed");

                    let output = JsonArrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(5));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrlen_empty() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("emptyarr".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonArrlenInput {
                                key: RedisKey::String("emptyarr".into()),
                                path: Some(RedisJsonValue::String("$.arr".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrlen failed");

                    let output = JsonArrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrlen_non_array() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("notarr".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"val":"string"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonArrlenInput {
                                key: RedisKey::String("notarr".into()),
                                path: Some(RedisJsonValue::String("$.val".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrlen failed");

                    let output = JsonArrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None); // Not an array
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrlen_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonArrlenInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("arrlen failed");

                    let output = JsonArrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrlen_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2len".into()),
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
                    &JsonArrlenInput {
                        key: RedisKey::String("r2len".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                    }
                    .command(),
                )
                .await
                .expect("arrlen failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrlen_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3len".into()),
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
                    &JsonArrlenInput {
                        key: RedisKey::String("r3len".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                    }
                    .command(),
                )
                .await
                .expect("arrlen failed");

            let output = JsonArrlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.first(), Some(3));

            ctx.stop().await;
        }
    }
}
