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

const API_INFO: ApiInfo<RedisApi, JsonRespInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonResp,
    "Returns the JSON value at path in Redis Serialization Protocol (RESP)",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.RESP`
/// https://redis.io/docs/latest/commands/json.resp/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonRespInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonRespInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonRespInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(JsonRespInput, API_INFO, {key, path});

impl RedisCommandInput for JsonRespInput {
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
            return Err(EpError::request("JSON.RESP requires at least 1 argument"));
        }
        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();
        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.RESP command
///
/// The response is a nested RESP structure representing the JSON value.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonRespOutput {
    /// Raw response data - nested arrays representing JSON structure
    exists: bool,
}

impl JsonRespOutput {
    pub fn new(exists: bool) -> Self {
        Self { exists }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let exists = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(_) => true,
                Resp2Frame::Null => false,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                _ => true,
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { .. } => true,
                Resp3Frame::Null => false,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                _ => true,
            },
        };

        Ok(Self { exists })
    }
}

impl Serialize for JsonRespOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonRespOutput", 1)?;
        state.serialize_field("exists", &self.exists)?;
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
            let input = JsonRespInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$9\r\nJSON.RESP\r\n"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonRespOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonRespInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonRespInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonRespInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_resp_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("respkey".into()),
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
                            &JsonRespInput {
                                key: RedisKey::String("respkey".into()),
                                path: Some(RedisJsonValue::String("$".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("resp failed");

                    let output = JsonRespOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_resp_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonRespInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("resp failed");

                    let output = JsonRespOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }
    }
}
