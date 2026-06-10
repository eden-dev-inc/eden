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

const API_INFO: ApiInfo<RedisApi, JsonArrtrimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonArrtrim,
    "Trims the array at path to contain only the specified inclusive range of indices from start to stop",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.ARRTRIM`
/// https://redis.io/docs/latest/commands/json.arrtrim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonArrtrimInput {
    key: RedisKey,
    path: RedisJsonValue,
    start: RedisJsonValue,
    stop: RedisJsonValue,
}

impl Serialize for JsonArrtrimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrtrimInput", 5)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;
        state.end()
    }
}

impl_redis_operation!(JsonArrtrimInput, API_INFO, {key, path, start, stop});

impl RedisCommandInput for JsonArrtrimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.path).arg(&self.start).arg(&self.stop);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 4 {
            return Err(EpError::request(format!("JSON.ARRTRIM requires 4 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            path: args[1].clone(),
            start: args[2].clone(),
            stop: args[3].clone(),
        })
    }
}

/// Output for Redis JSON.ARRTRIM command
///
/// Returns an array of integer replies for each path,
/// representing the new length of the array after trimming.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonArrtrimOutput {
    lengths: Vec<Option<i64>>,
}

impl JsonArrtrimOutput {
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
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
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
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { lengths })
    }
}

impl Serialize for JsonArrtrimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrtrimOutput", 1)?;
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
        fn test_encode_command() {
            let input = JsonArrtrimInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.arr".into()),
                start: RedisJsonValue::Integer(1),
                stop: RedisJsonValue::Integer(3),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$12\r\nJSON.ARRTRIM\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonArrtrimOutput::decode(b"*1\r\n:3\r\n").unwrap();
            assert_eq!(output.first(), Some(3));
        }

        #[test]
        fn test_decode_output_single() {
            let output = JsonArrtrimOutput::decode(b":2\r\n").unwrap();
            assert_eq!(output.first(), Some(2));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonArrtrimOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonArrtrimOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.arr".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(2),
            ];
            let input = JsonArrtrimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_wrong_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.arr".into()),
                RedisJsonValue::Integer(0),
            ];
            let err = JsonArrtrimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("4 arguments"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonArrtrimInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
                start: RedisJsonValue::Integer(0),
                stop: RedisJsonValue::Integer(1),
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
        async fn test_json_arrtrim_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("trimkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[0,1,2,3,4,5]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Keep elements 1-3 (indices 1, 2, 3)
                    let result = ctx
                        .raw(
                            &JsonArrtrimInput {
                                key: RedisKey::String("trimkey".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                start: RedisJsonValue::Integer(1),
                                stop: RedisJsonValue::Integer(3),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrtrim failed");

                    let output = JsonArrtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(3)); // [1,2,3] - 3 elements
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrtrim_negative_indices() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("trimneg".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[0,1,2,3,4]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Keep last 3 elements using negative indices
                    let result = ctx
                        .raw(
                            &JsonArrtrimInput {
                                key: RedisKey::String("trimneg".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                start: RedisJsonValue::Integer(-3),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrtrim failed");

                    let output = JsonArrtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(3)); // [2,3,4] - 3 elements
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrtrim_empty_result() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("trimempty".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[1,2,3]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Trim with out-of-bounds indices results in empty array
                    let result = ctx
                        .raw(
                            &JsonArrtrimInput {
                                key: RedisKey::String("trimempty".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                start: RedisJsonValue::Integer(10),
                                stop: RedisJsonValue::Integer(20),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrtrim failed");

                    let output = JsonArrtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(0)); // Empty array
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrtrim_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2trim".into()),
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
                    &JsonArrtrimInput {
                        key: RedisKey::String("r2trim".into()),
                        path: RedisJsonValue::String("$.arr".into()),
                        start: RedisJsonValue::Integer(0),
                        stop: RedisJsonValue::Integer(2),
                    }
                    .command(),
                )
                .await
                .expect("arrtrim failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrtrim_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3trim".into()),
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
                    &JsonArrtrimInput {
                        key: RedisKey::String("r3trim".into()),
                        path: RedisJsonValue::String("$.arr".into()),
                        start: RedisJsonValue::Integer(0),
                        stop: RedisJsonValue::Integer(2),
                    }
                    .command(),
                )
                .await
                .expect("arrtrim failed");

            let output = JsonArrtrimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.first(), Some(3)); // [1,2,3] - 3 elements

            ctx.stop().await;
        }
    }
}
