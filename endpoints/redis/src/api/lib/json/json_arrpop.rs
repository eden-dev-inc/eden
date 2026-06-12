use crate::api::lib::json::PathWithIndex;
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

const API_INFO: ApiInfo<RedisApi, JsonArrpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonArrpop,
    "Removes and returns the element at the specified index in the array at path",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.ARRPOP`
/// https://redis.io/docs/latest/commands/json.arrpop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonArrpopInput {
    key: RedisKey,
    path: Option<PathWithIndex>,
}

impl Serialize for JsonArrpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;

        if let Some(path) = &self.path {
            fields += 1;
            if path.index.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("JsonArrpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", &path.path)?;
            if let Some(index) = &path.index {
                state.serialize_field("index", index)?;
            }
        }
        state.end()
    }
}

impl_redis_operation!(JsonArrpopInput, API_INFO, {key, path});

impl RedisCommandInput for JsonArrpopInput {
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
            path.cmd(&mut command);
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("JSON.ARRPOP requires at least 1 argument"));
        }
        let key = args[0].clone().try_into()?;
        let path = if args.len() > 1 {
            let path_val = args[1].clone();
            let index = args.get(2).cloned();
            Some(PathWithIndex { path: path_val, index })
        } else {
            None
        };
        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.ARRPOP command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonArrpopOutput {
    values: Vec<Option<String>>,
}

impl JsonArrpopOutput {
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Option<String>] {
        &self.values
    }

    pub fn first(&self) -> Option<&str> {
        self.values.first().and_then(|v| v.as_deref())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut values = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(data) => values.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp2Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp2Frame::BulkString(data) => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut values = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => values.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp3Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp3Frame::BlobString { data, .. } => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for JsonArrpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrpopOutput", 1)?;
        state.serialize_field("values", &self.values)?;
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
            let input = JsonArrpopInput { key: RedisKey::String("mykey".into()), path: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$11\r\nJSON.ARRPOP\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonArrpopInput {
                key: RedisKey::String("mykey".into()),
                path: Some(PathWithIndex { path: RedisJsonValue::String("$.arr".into()), index: None }),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$11\r\nJSON.ARRPOP\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonArrpopOutput::decode(b"*1\r\n$1\r\n5\r\n").unwrap();
            assert_eq!(output.first(), Some("5"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonArrpopOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonArrpopInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonArrpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonArrpopInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_arrpop_last() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("popkey".into()),
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
                            &JsonArrpopInput {
                                key: RedisKey::String("popkey".into()),
                                path: Some(PathWithIndex { path: RedisJsonValue::String("$.arr".into()), index: None }),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrpop failed");

                    let output = JsonArrpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some("3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrpop_first() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("popfirst".into()),
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
                            &JsonArrpopInput {
                                key: RedisKey::String("popfirst".into()),
                                path: Some(PathWithIndex {
                                    path: RedisJsonValue::String("$.arr".into()),
                                    index: Some(RedisJsonValue::Integer(0)),
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrpop failed");

                    let output = JsonArrpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some("1"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrpop_empty() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("popempty".into()),
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
                            &JsonArrpopInput {
                                key: RedisKey::String("popempty".into()),
                                path: Some(PathWithIndex { path: RedisJsonValue::String("$.arr".into()), index: None }),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrpop failed");

                    let output = JsonArrpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None);
                })
            })
            .await;
        }
    }
}
