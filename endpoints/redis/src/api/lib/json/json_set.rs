use crate::api::lib::json::{JsonSetResult, Options};
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

const API_INFO: ApiInfo<RedisApi, JsonSetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonSet, "Sets or updates the JSON value at path", ReqType::Write, true);

/// See official Redis documentation for `JSON.SET`
/// https://redis.io/docs/latest/commands/json.set/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonSetInput {
    pub(crate) key: RedisKey,
    pub(crate) path: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
    pub(crate) options: Option<Options>,
}

impl Serialize for JsonSetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.options.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonSetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("value", &self.value)?;
        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonSetInput,
    API_INFO,
    {key, path, value, options}
);

impl RedisCommandInput for JsonSetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.path).arg(&self.value);

        if let Some(options) = &self.options {
            match options {
                Options::NX => command.arg("NX"),
                Options::XX => command.arg("XX"),
            };
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("JSON.SET requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let path = args[1].clone();
        let value = args[2].clone();
        let options = if args.len() > 3 {
            Some(Options::try_from(args[3].clone())?)
        } else {
            None
        };

        Ok(Self { key, path, value, options })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonSetOutput {
    result: JsonSetResult,
}

impl JsonSetOutput {
    pub fn new(result: JsonSetResult) -> Self {
        Self { result }
    }

    /// Returns true if the value was set successfully
    pub fn was_set(&self) -> bool {
        matches!(self.result, JsonSetResult::Ok)
    }

    /// Returns true if the operation was aborted (NX/XX condition not met)
    pub fn is_nil(&self) -> bool {
        matches!(self.result, JsonSetResult::Nil)
    }

    /// Decode the Redis protocol response into a JsonSetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.as_slice() == b"OK" => JsonSetResult::Ok,
                Resp2Frame::Null => JsonSetResult::Nil,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.SET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.as_slice() == b"OK" => JsonSetResult::Ok,
                Resp3Frame::Null => JsonSetResult::Nil,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.SET response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for JsonSetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonSetOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

impl Serialize for JsonSetResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            JsonSetResult::Ok => serializer.serialize_str("OK"),
            JsonSetResult::Nil => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = JsonSetInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String(r#"{"foo":"bar"}"#.into()),
                options: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$8\r\nJSON.SET\r\n"));
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = JsonSetInput {
                key: RedisKey::String("key".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String("{}".into()),
                options: Some(Options::NX),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"NX"));
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = JsonSetInput {
                key: RedisKey::String("key".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String("{}".into()),
                options: Some(Options::XX),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"XX"));
        }

        #[test]
        fn test_decode_output_ok_resp2() {
            let output = JsonSetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.was_set());
            assert!(!output.is_nil());
        }

        #[test]
        fn test_decode_output_nil_resp2() {
            let output = JsonSetOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.was_set());
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_output_ok_resp3() {
            let output = JsonSetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.was_set());
        }

        #[test]
        fn test_decode_output_nil_resp3() {
            let output = JsonSetOutput::decode(b"_\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonSetOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String(r#"{"foo":"bar"}"#.into()),
            ];
            let input = JsonSetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.options.is_none());
        }

        #[test]
        fn test_decode_input_with_nx() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
                RedisJsonValue::String("NX".into()),
            ];
            let input = JsonSetInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(Options::NX)));
        }

        #[test]
        fn test_decode_input_with_xx() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
                RedisJsonValue::String("XX".into()),
            ];
            let input = JsonSetInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(Options::XX)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$".into())];
            let err = JsonSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
                RedisJsonValue::String("INVALID".into()),
            ];
            let err = JsonSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Invalid option"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonSetInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String("{}".into()),
                options: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_options_try_from_lowercase() {
            let nx = Options::try_from(RedisJsonValue::String("nx".into())).unwrap();
            assert!(matches!(nx, Options::NX));

            let xx = Options::try_from(RedisJsonValue::String("xx".into())).unwrap();
            assert!(matches!(xx, Options::XX));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("jsonkey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"name":"test"}"#.into()),
                                options: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_nested_path() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // First set root document
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("nested".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"outer":{}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Then set nested path
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("nested".into()),
                                path: RedisJsonValue::String("$.outer.inner".into()),
                                value: RedisJsonValue::String(r#""value""#.into()),
                                options: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_nx_new_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("nxkey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"new":true}"#.into()),
                                options: Some(Options::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "NX should succeed for new key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_nx_existing_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("existkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"exists":true}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try NX on existing key
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("existkey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"new":true}"#.into()),
                                options: Some(Options::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_nil(), "NX should fail for existing key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_xx_existing_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("xxkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"old":true}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // XX on existing key should work
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("xxkey".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"updated":true}"#.into()),
                                options: Some(Options::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "XX should succeed for existing key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_xx_nonexistent_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonSetInput {
                                key: RedisKey::String("nonexistent".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{}"#.into()),
                                options: Some(Options::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonSetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_nil(), "XX should fail for nonexistent key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_various_types() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Test setting various JSON types
                    let test_cases = [
                        (r#""string""#, "string value"),
                        ("123", "integer"),
                        ("45.67", "float"),
                        ("true", "boolean"),
                        ("null", "null"),
                        (r#"["a","b","c"]"#, "array"),
                        (r#"{"nested":{"deep":true}}"#, "nested object"),
                    ];

                    for (i, (value, desc)) in test_cases.iter().enumerate() {
                        let result = ctx
                            .raw(
                                &JsonSetInput {
                                    key: RedisKey::String(format!("typekey{}", i)),
                                    path: RedisJsonValue::String("$".into()),
                                    value: RedisJsonValue::String(value.to_string()),
                                    options: None,
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = JsonSetOutput::decode(&result).expect("decode failed");
                        assert!(output.was_set(), "Failed to set {}", desc);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &JsonSetInput {
                        key: RedisKey::String("r2key".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{}"#.into()),
                        options: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return simple string");
            let output = JsonSetOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_set_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &JsonSetInput {
                        key: RedisKey::String("r3key".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{}"#.into()),
                        options: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = JsonSetOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());

            ctx.stop().await;
        }
    }
}
