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

const API_INFO: ApiInfo<RedisApi, JsonGetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonGet,
    "Gets the value at one or more paths in JSON serialized form",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.GET`
/// https://redis.io/docs/latest/commands/json.get/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonGetInput {
    pub(crate) key: RedisKey,
    pub(crate) indent: Option<RedisJsonValue>,
    pub(crate) newline: Option<RedisJsonValue>,
    pub(crate) space: Option<RedisJsonValue>,
    pub(crate) path: Option<Vec<RedisJsonValue>>,
}

impl Serialize for JsonGetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.indent.is_some() {
            fields += 1;
        }
        if self.newline.is_some() {
            fields += 1;
        }
        if self.space.is_some() {
            fields += 1;
        }
        if self.path.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonGetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(indent) = &self.indent {
            state.serialize_field("indent", indent)?;
        }
        if let Some(newline) = &self.newline {
            state.serialize_field("newline", newline)?;
        }
        if let Some(space) = &self.space {
            state.serialize_field("space", space)?;
        }
        if let Some(path) = &self.path {
            state.serialize_field("path", path)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonGetInput,
    API_INFO,
    {key, indent, newline, space, path}
);

impl RedisCommandInput for JsonGetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(indent) = &self.indent {
            command.arg("INDENT").arg(indent);
        }

        if let Some(newline) = &self.newline {
            command.arg("NEWLINE").arg(newline);
        }

        if let Some(space) = &self.space {
            command.arg("SPACE").arg(space);
        }

        if let Some(path) = &self.path {
            for p in path {
                command.arg(p);
            }
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("JSON.GET requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let mut indent = None;
        let mut newline = None;
        let mut space = None;
        let mut path = None;

        let mut i = 1;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "INDENT" if i + 1 < args.len() => {
                        indent = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "NEWLINE" if i + 1 < args.len() => {
                        newline = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SPACE" if i + 1 < args.len() => {
                        space = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        path = Some(args[i..].to_vec());
                        break;
                    }
                }
            } else {
                path = Some(args[i..].to_vec());
                break;
            }
        }

        Ok(Self { key, indent, newline, space, path })
    }
}

/// Output for Redis JSON.GET command
///
/// Returns the JSON value at the specified path(s), or nil if the key doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonGetOutput {
    value: Option<String>,
}

impl JsonGetOutput {
    pub fn new(value: Option<String>) -> Self {
        Self { value }
    }

    /// Get the raw JSON string result
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns true if a value exists
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Returns true if key doesn't exist or path not found
    pub fn is_nil(&self) -> bool {
        self.value.is_none()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Some(String::from_utf8_lossy(&data).to_string()),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.GET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8_lossy(&data).to_string()),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.GET response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for JsonGetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonGetOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = JsonGetInput { key: RedisKey::String("mykey".into()), ..Default::default() };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$8\r\nJSON.GET\r\n"));
        }

        #[test]
        fn test_encode_command_with_path() {
            let input = JsonGetInput {
                key: RedisKey::String("mykey".into()),
                path: Some(vec![RedisJsonValue::String("$".into())]),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$8\r\nJSON.GET\r\n"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = JsonGetInput {
                key: RedisKey::String("mykey".into()),
                indent: Some(RedisJsonValue::String("  ".into())),
                newline: Some(RedisJsonValue::String("\n".into())),
                path: Some(vec![RedisJsonValue::String("$".into())]),
                ..Default::default()
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"INDENT"));
            assert!(cmd.windows(7).any(|w| w == b"NEWLINE"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = JsonGetOutput::decode(b"$13\r\n{\"foo\":\"bar\"}\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(r#"{"foo":"bar"}"#));
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonGetOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_output_resp3_null() {
            let output = JsonGetOutput::decode(b"_\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonGetOutput::decode(b"-ERR unknown key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = JsonGetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.path.is_none());
        }

        #[test]
        fn test_decode_input_with_path() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.field".into())];
            let input = JsonGetInput::decode(args).unwrap();
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("INDENT".into()),
                RedisJsonValue::String("  ".into()),
                RedisJsonValue::String("$".into()),
            ];
            let input = JsonGetInput::decode(args).unwrap();
            assert!(input.indent.is_some());
            assert!(input.path.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonGetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonGetInput {
                key: RedisKey::String("testkey".into()),
                ..Default::default()
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
        async fn test_json_get_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("getkey".into()),
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
                            &JsonGetInput {
                                key: RedisKey::String("getkey".into()),
                                path: Some(vec![RedisJsonValue::String("$".into())]),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("get failed");

                    let output = JsonGetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_get_nested_path() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("nested".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":{"b":{"c":123}}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonGetInput {
                                key: RedisKey::String("nested".into()),
                                path: Some(vec![RedisJsonValue::String("$.a.b.c".into())]),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("get failed");

                    let output = JsonGetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.value().unwrap().contains("123"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_get_nonexistent_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonGetInput {
                                key: RedisKey::String("noexist".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("get failed");

                    let output = JsonGetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_nil());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_get_multiple_paths() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("multipath".into()),
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
                            &JsonGetInput {
                                key: RedisKey::String("multipath".into()),
                                path: Some(vec![RedisJsonValue::String("$.a".into()), RedisJsonValue::String("$.b".into())]),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("get failed");

                    let output = JsonGetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_get_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2get".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"x":1}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonGetInput {
                        key: RedisKey::String("r2get".into()),
                        path: Some(vec![RedisJsonValue::String("$".into())]),
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("get failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_get_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3get".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"x":1}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonGetInput {
                        key: RedisKey::String("r3get".into()),
                        path: Some(vec![RedisJsonValue::String("$".into())]),
                        ..Default::default()
                    }
                    .command(),
                )
                .await
                .expect("get failed");

            let output = JsonGetOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }
    }
}
