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

const API_INFO: ApiInfo<RedisApi, JsonStrlenInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonStrlen,
    "Returns the length of the JSON String at path in key",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.STRLEN`
/// https://redis.io/docs/latest/commands/json.strlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonStrlenInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonStrlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonStrlenInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonStrlenInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonStrlenInput {
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
            return Err(EpError::request("JSON.STRLEN requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.STRLEN command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonStrlenOutput {
    lengths: Vec<Option<i64>>,
}

impl JsonStrlenOutput {
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
                    return Err(EpError::parse(format!("unexpected JSON.STRLEN response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected JSON.STRLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { lengths })
    }
}

impl Serialize for JsonStrlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonStrlenOutput", 1)?;
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
            let input = JsonStrlenInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.str".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$11\r\nJSON.STRLEN\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonStrlenOutput::decode(b"*1\r\n:5\r\n").unwrap();
            assert_eq!(output.first(), Some(5));
        }

        #[test]
        fn test_decode_output_single() {
            let output = JsonStrlenOutput::decode(b":10\r\n").unwrap();
            assert_eq!(output.first(), Some(10));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonStrlenOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonStrlenInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonStrlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonStrlenInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_strlen_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("strlenkey".into()),
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
                            &JsonStrlenInput {
                                key: RedisKey::String("strlenkey".into()),
                                path: Some(RedisJsonValue::String("$.msg".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("strlen failed");

                    let output = JsonStrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(5)); // "hello" = 5 chars
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strlen_non_string() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("numkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"num":123}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonStrlenInput {
                                key: RedisKey::String("numkey".into()),
                                path: Some(RedisJsonValue::String("$.num".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("strlen failed");

                    let output = JsonStrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_strlen_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonStrlenInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("strlen failed");

                    let output = JsonStrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None);
                })
            })
            .await;
        }
    }
}
