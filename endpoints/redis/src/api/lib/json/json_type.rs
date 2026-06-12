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

const API_INFO: ApiInfo<RedisApi, JsonTypeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonType, "Returns the type of the JSON value at path", ReqType::Read, true);

/// See official Redis documentation for `JSON.TYPE`
/// https://redis.io/docs/latest/commands/json.type/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonTypeInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonTypeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonTypeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonTypeInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonTypeInput {
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
            return Err(EpError::request("JSON.TYPE requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.TYPE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonTypeOutput {
    types: Vec<Option<String>>,
}

impl JsonTypeOutput {
    pub fn new(types: Vec<Option<String>>) -> Self {
        Self { types }
    }

    pub fn types(&self) -> &[Option<String>] {
        &self.types
    }

    pub fn first(&self) -> Option<&str> {
        self.types.first().and_then(|t| t.as_deref())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let types = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut types = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(data) => types.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp2Frame::SimpleString(data) => types.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp2Frame::Null => types.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    types
                }
                Resp2Frame::BulkString(data) => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp2Frame::SimpleString(data) => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.TYPE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut types = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => types.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp3Frame::SimpleString { data, .. } => types.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp3Frame::Array { data: inner, .. } => {
                                // Redis Stack RESP3 may return nested arrays
                                for inner_item in inner {
                                    match inner_item {
                                        Resp3Frame::BlobString { data, .. } => types.push(Some(String::from_utf8_lossy(&data).to_string())),
                                        Resp3Frame::SimpleString { data, .. } => {
                                            types.push(Some(String::from_utf8_lossy(&data).to_string()))
                                        }
                                        Resp3Frame::Null => types.push(None),
                                        _ => {}
                                    }
                                }
                            }
                            Resp3Frame::Null => types.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    types
                }
                Resp3Frame::BlobString { data, .. } => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp3Frame::SimpleString { data, .. } => {
                    vec![Some(String::from_utf8_lossy(&data).to_string())]
                }
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.TYPE response: {:?}", other)));
                }
            },
        };

        Ok(Self { types })
    }
}

impl Serialize for JsonTypeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonTypeOutput", 1)?;
        state.serialize_field("types", &self.types)?;
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
            let input = JsonTypeInput { key: RedisKey::String("mykey".into()), path: None };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$9\r\nJSON.TYPE\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonTypeInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$.field".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$9\r\nJSON.TYPE\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonTypeOutput::decode(b"*1\r\n$6\r\nobject\r\n").unwrap();
            assert_eq!(output.first(), Some("object"));
        }

        #[test]
        fn test_decode_output_single() {
            let output = JsonTypeOutput::decode(b"$6\r\nstring\r\n").unwrap();
            assert_eq!(output.first(), Some("string"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonTypeOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.path".into())];
            let input = JsonTypeInput::decode(args).unwrap();
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonTypeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonTypeInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_type_object() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("typekey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"foo":"bar"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonTypeInput {
                                key: RedisKey::String("typekey".into()),
                                path: Some(RedisJsonValue::String("$".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("type failed");

                    let output = JsonTypeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some("object"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_type_various() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("types".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"s":"str","n":123,"b":true,"a":[1,2],"o":{}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let test_cases = vec![
                        ("$.s", "string"),
                        ("$.n", "integer"),
                        ("$.b", "boolean"),
                        ("$.a", "array"),
                        ("$.o", "object"),
                    ];

                    for (path, expected_type) in test_cases {
                        let result = ctx
                            .raw(
                                &JsonTypeInput {
                                    key: RedisKey::String("types".into()),
                                    path: Some(RedisJsonValue::String(path.into())),
                                }
                                .command(),
                            )
                            .await
                            .expect("type failed");

                        let output = JsonTypeOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.first(), Some(expected_type), "Type mismatch for path {}", path);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_type_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonTypeInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("type failed");

                    let output = JsonTypeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), None);
                })
            })
            .await;
        }
    }
}
