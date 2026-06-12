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

const API_INFO: ApiInfo<RedisApi, JsonDebugMemoryInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonDebugMemory, "Reports the size in bytes of a key", ReqType::Read, true);

/// See official Redis documentation for `JSON.DEBUG MEMORY`
/// https://redis.io/docs/latest/commands/json.debug-memory/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonDebugMemoryInput {
    key: RedisKey,
    path: Option<RedisJsonValue>,
}

impl Serialize for JsonDebugMemoryInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonDebugMemoryInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(JsonDebugMemoryInput, API_INFO, {key, path});

impl RedisCommandInput for JsonDebugMemoryInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        // TODO(#774): Once PR #774 is merged, add "JSON.DEBUG MEMORY" to MULTI_WORD_COMMANDS
        // in command.rs and simplify this to just: crate::command::cmd(&API_INFO.api.to_string())
        let cmd_str = API_INFO.api.to_string();
        let mut parts = cmd_str.split_whitespace();
        let base_cmd = parts.next().unwrap_or_default();
        let mut command = crate::command::cmd(if base_cmd.is_empty() { &cmd_str } else { base_cmd });
        for part in parts {
            command.arg(part);
        }
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
            return Err(EpError::request("JSON.DEBUG MEMORY requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let path = args.get(1).cloned();

        Ok(Self { key, path })
    }
}

/// Output for Redis JSON.DEBUG MEMORY command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonDebugMemoryOutput {
    bytes: Option<i64>,
}

impl JsonDebugMemoryOutput {
    pub fn new(bytes: Option<i64>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> Option<i64> {
        self.bytes
    }

    pub fn exists(&self) -> bool {
        self.bytes.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let size = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Some(n),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Some(data),
                Resp3Frame::Array { data, .. } => {
                    // Redis Stack RESP3 returns array of numbers for JSONPath queries
                    // Sum all memory values for total memory usage
                    let total: i64 = data
                        .iter()
                        .filter_map(|item| {
                            if let Resp3Frame::Number { data, .. } = item {
                                Some(*data)
                            } else {
                                None
                            }
                        })
                        .sum();
                    if total > 0 { Some(total) } else { None }
                }
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { bytes: size })
    }
}

impl Serialize for JsonDebugMemoryOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonDebugMemoryOutput", 1)?;
        state.serialize_field("bytes", &self.bytes)?;
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
            let input = JsonDebugMemoryInput {
                key: RedisKey::String("mykey".into()),
                path: Some(RedisJsonValue::String("$".into())),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$10\r\nJSON.DEBUG\r\n$6\r\nMEMORY\r\n"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = JsonDebugMemoryOutput::decode(b":256\r\n").unwrap();
            assert_eq!(output.bytes(), Some(256));
            assert!(output.exists());
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonDebugMemoryOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.bytes(), None);
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonDebugMemoryInput::decode(args).unwrap();
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonDebugMemoryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonDebugMemoryInput { key: RedisKey::String("testkey".into()), path: None };
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
        async fn test_json_debug_memory_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("memkey".into()),
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
                            &JsonDebugMemoryInput {
                                key: RedisKey::String("memkey".into()),
                                path: Some(RedisJsonValue::String("$".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("debug memory failed");

                    let output = JsonDebugMemoryOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.bytes().unwrap() > 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_debug_memory_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&JsonDebugMemoryInput { key: RedisKey::String("noexist".into()), path: None }.command())
                        .await
                        .expect("debug memory failed");

                    let output = JsonDebugMemoryOutput::decode(&result);
                    // For nonexistent key, Redis may return:
                    // - Null (RESP2)
                    // - 0 bytes (Redis Stack RESP3 for empty key)
                    if let Ok(out) = output {
                        // Either doesn't exist or has 0 bytes
                        if out.exists() {
                            assert_eq!(out.bytes(), Some(0));
                        }
                    }
                })
            })
            .await;
        }
    }
}
