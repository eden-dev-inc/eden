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

const API_INFO: ApiInfo<RedisApi, JsonObjkeysInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonObjkeys,
    "Returns the JSON keys of the object at path",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.OBJKEYS`
/// https://redis.io/docs/latest/commands/json.objkeys/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonObjkeysInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonObjkeysInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonObjkeysInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(JsonObjkeysInput, API_INFO, {key, path});

impl RedisCommandInput for JsonObjkeysInput {
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
            return Err(EpError::request("JSON.OBJKEYS requires at least 1 argument"));
        }
        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();
        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.OBJKEYS command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonObjkeysOutput {
    keys: Vec<Option<Vec<String>>>,
}

impl JsonObjkeysOutput {
    pub fn new(keys: Vec<Option<Vec<String>>>) -> Self {
        Self { keys }
    }

    pub fn keys(&self) -> &[Option<Vec<String>>] {
        &self.keys
    }

    pub fn first(&self) -> Option<&Vec<String>> {
        self.keys.first().and_then(|k| k.as_ref())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    if arr.is_empty() {
                        vec![Some(vec![])]
                    } else if matches!(arr[0], Resp2Frame::Array(_) | Resp2Frame::Null) {
                        let mut keys = Vec::new();
                        for item in arr {
                            match item {
                                Resp2Frame::Array(inner) => {
                                    let mut strs = Vec::new();
                                    for s in inner {
                                        if let Resp2Frame::BulkString(data) = s {
                                            strs.push(String::from_utf8_lossy(&data).to_string());
                                        }
                                    }
                                    keys.push(Some(strs))
                                }
                                Resp2Frame::Null => keys.push(None),
                                _ => {}
                            }
                        }
                        keys
                    } else {
                        let mut strs = Vec::new();
                        for s in arr {
                            if let Resp2Frame::BulkString(data) = s {
                                strs.push(String::from_utf8_lossy(&data).to_string());
                            }
                        }
                        vec![Some(strs)]
                    }
                }
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    if data.is_empty() {
                        vec![Some(vec![])]
                    } else if matches!(data[0], Resp3Frame::Array { .. } | Resp3Frame::Null) {
                        let mut keys = Vec::new();
                        for item in data {
                            match item {
                                Resp3Frame::Array { data: inner, .. } => {
                                    let mut strs = Vec::new();
                                    for s in inner {
                                        if let Resp3Frame::BlobString { data, .. } = s {
                                            strs.push(String::from_utf8_lossy(&data).to_string());
                                        }
                                    }
                                    keys.push(Some(strs))
                                }
                                Resp3Frame::Null => keys.push(None),
                                _ => {}
                            }
                        }
                        keys
                    } else {
                        let mut strs = Vec::new();
                        for s in data {
                            if let Resp3Frame::BlobString { data, .. } = s {
                                strs.push(String::from_utf8_lossy(&data).to_string());
                            }
                        }
                        vec![Some(strs)]
                    }
                }
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { keys })
    }
}

impl Serialize for JsonObjkeysOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonObjkeysOutput", 1)?;
        state.serialize_field("keys", &self.keys)?;
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
            let input = JsonObjkeysInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$12\r\nJSON.OBJKEYS\r\n"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonObjkeysOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.first().is_none());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonObjkeysInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonObjkeysInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonObjkeysInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_objkeys_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("objkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":1,"b":2,"c":3}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonObjkeysInput {
                                key: RedisKey::String("objkey".into()),
                                path: Some(RedisJsonValue::String("$".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("objkeys failed");

                    let output = JsonObjkeysOutput::decode(&result).expect("decode failed");
                    let keys = output.first().expect("should have keys");
                    assert_eq!(keys.len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_objkeys_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonObjkeysInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("objkeys failed");

                    let output = JsonObjkeysOutput::decode(&result).expect("decode failed");
                    assert!(output.first().is_none());
                })
            })
            .await;
        }
    }
}
