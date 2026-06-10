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

const API_INFO: ApiInfo<RedisApi, JsonArrappendInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonArrappend,
    "Append one or more json values into the array at path after the last element in it",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.ARRAPPEND`
/// https://redis.io/docs/latest/commands/json.arrappend/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonArrappendInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
    value: Vec<RedisJsonValue>,
}

impl Serialize for JsonArrappendInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonArrappendInput", fields)?;
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
    JsonArrappendInput,
    API_INFO,
    {key, path, value}
);

impl RedisCommandInput for JsonArrappendInput {
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
        for v in &self.value {
            command.arg(v);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("JSON.ARRAPPEND requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;

        // Check if second argument looks like a path (string starting with $ or .) or a value
        let (path, value_start) = if args.len() > 2 {
            if let RedisJsonValue::String(s) = &args[1] {
                if s.starts_with('$') || s.starts_with('.') {
                    (Some(args[1].clone()), 2)
                } else {
                    (None, 1)
                }
            } else {
                (None, 1)
            }
        } else {
            (None, 1)
        };

        let value = args[value_start..].to_vec();

        Ok(Self { key, path, value })
    }
}

/// Output for Redis JSON.ARRAPPEND command
///
/// Returns an array of integer replies for each path,
/// representing the new length of the array after appending.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonArrappendOutput {
    lengths: Vec<Option<i64>>,
}

impl JsonArrappendOutput {
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
                    return Err(EpError::parse(format!("unexpected JSON.ARRAPPEND response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected JSON.ARRAPPEND response: {:?}", other)));
                }
            },
        };

        Ok(Self { lengths })
    }
}

impl Serialize for JsonArrappendOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrappendOutput", 1)?;
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
            let input = JsonArrappendInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.arr".into())),
                value: vec![RedisJsonValue::String("1".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$14\r\nJSON.ARRAPPEND\r\n"));
        }

        #[test]
        fn test_encode_command_multiple_values() {
            let input = JsonArrappendInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.arr".into())),
                value: vec![
                    RedisJsonValue::String("1".into()),
                    RedisJsonValue::String("2".into()),
                    RedisJsonValue::String("3".into()),
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n$14\r\nJSON.ARRAPPEND\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonArrappendOutput::decode(b"*1\r\n:5\r\n").unwrap();
            assert_eq!(output.first(), Some(5));
        }

        #[test]
        fn test_decode_output_single() {
            let output = JsonArrappendOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.first(), Some(3));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonArrappendOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonArrappendOutput::decode(b"-ERR not an array\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_with_path() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.arr".into()),
                RedisJsonValue::String("1".into()),
            ];
            let input = JsonArrappendInput::decode(args).unwrap();
            assert!(input.path.is_some());
            assert_eq!(input.value.len(), 1);
        }

        #[test]
        fn test_decode_input_without_path() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("1".into())];
            let input = JsonArrappendInput::decode(args).unwrap();
            assert!(input.path.is_none());
            assert_eq!(input.value.len(), 1);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = JsonArrappendInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonArrappendInput {
                key: RedisKey::String("testkey".into()),
                path: None,
                value: vec![],
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
        async fn test_json_arrappend_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("arrappkey".into()),
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
                            &JsonArrappendInput {
                                key: RedisKey::String("arrappkey".into()),
                                path: Some(RedisJsonValue::String("$.arr".into())),
                                value: vec![RedisJsonValue::String("4".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("arrappend failed");

                    let output = JsonArrappendOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(4)); // Array now has 4 elements
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrappend_multiple() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("arrmulti".into()),
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
                            &JsonArrappendInput {
                                key: RedisKey::String("arrmulti".into()),
                                path: Some(RedisJsonValue::String("$.arr".into())),
                                value: vec![
                                    RedisJsonValue::String("1".into()),
                                    RedisJsonValue::String("2".into()),
                                    RedisJsonValue::String("3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("arrappend failed");

                    let output = JsonArrappendOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(3));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrappend_non_array() {
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
                            &JsonArrappendInput {
                                key: RedisKey::String("notarr".into()),
                                path: Some(RedisJsonValue::String("$.val".into())),
                                value: vec![RedisJsonValue::String("1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("arrappend failed");

                    let output = JsonArrappendOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None); // Not an array
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrappend_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2arrapp".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"arr":[1]}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonArrappendInput {
                        key: RedisKey::String("r2arrapp".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                        value: vec![RedisJsonValue::String("2".into())],
                    }
                    .command(),
                )
                .await
                .expect("arrappend failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrappend_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3arrapp".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"arr":[1]}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonArrappendInput {
                        key: RedisKey::String("r3arrapp".into()),
                        path: Some(RedisJsonValue::String("$.arr".into())),
                        value: vec![RedisJsonValue::String("2".into())],
                    }
                    .command(),
                )
                .await
                .expect("arrappend failed");

            let output = JsonArrappendOutput::decode(&result).expect("decode failed");
            assert_eq!(output.first(), Some(2));

            ctx.stop().await;
        }
    }
}
