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

const API_INFO: ApiInfo<RedisApi, JsonToggleInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonToggle, "Toggles a boolean value", ReqType::Write, true);

/// See official Redis documentation for `JSON.TOGGLE`
/// https://redis.io/docs/latest/commands/json.toggle/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonToggleInput {
    key: RedisKey,
    path: RedisJsonValue,
}

impl Serialize for JsonToggleInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonToggleInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.end()
    }
}

impl_redis_operation!(
    JsonToggleInput,
    API_INFO,
    {key, path}
);

impl RedisCommandInput for JsonToggleInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.path);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("JSON.TOGGLE requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, path: args[1].clone() })
    }
}

/// Output for Redis JSON.TOGGLE command
///
/// Returns an array of integer replies for each path:
/// - 0 if the new value is false
/// - 1 if the new value is true
/// - null if the path doesn't exist or isn't a boolean
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonToggleOutput {
    /// Results for each matched path (0=false, 1=true, None=not a boolean)
    values: Vec<Option<i64>>,
}

impl JsonToggleOutput {
    pub fn new(values: Vec<Option<i64>>) -> Self {
        Self { values }
    }

    /// Get the toggle results
    pub fn values(&self) -> &[Option<i64>] {
        &self.values
    }

    /// Get the first value (for single-path queries)
    pub fn first(&self) -> Option<Option<i64>> {
        self.values.first().copied()
    }

    /// Returns true if the first toggled value is now true
    pub fn is_true(&self) -> bool {
        matches!(self.first(), Some(Some(1)))
    }

    /// Returns true if the first toggled value is now false
    pub fn is_false(&self) -> bool {
        matches!(self.first(), Some(Some(0)))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut values = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::Integer(n) => values.push(Some(n)),
                            Resp2Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp2Frame::Integer(n) => vec![Some(n)],
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.TOGGLE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut values = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::Number { data, .. } => values.push(Some(data)),
                            Resp3Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp3Frame::Number { data, .. } => vec![Some(data)],
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.TOGGLE response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for JsonToggleOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonToggleOutput", 1)?;
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
        fn test_encode_command() {
            let input = JsonToggleInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.active".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$11\r\nJSON.TOGGLE\r\n"));
        }

        #[test]
        fn test_decode_output_array_true() {
            // Array with single 1 (toggled to true)
            let output = JsonToggleOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.values().len(), 1);
            assert_eq!(output.values()[0], Some(1));
            assert!(output.is_true());
        }

        #[test]
        fn test_decode_output_array_false() {
            // Array with single 0 (toggled to false)
            let output = JsonToggleOutput::decode(b"*1\r\n:0\r\n").unwrap();
            assert_eq!(output.values()[0], Some(0));
            assert!(output.is_false());
        }

        #[test]
        fn test_decode_output_array_null() {
            // Array with null (path not a boolean)
            let output = JsonToggleOutput::decode(b"*1\r\n$-1\r\n").unwrap();
            assert_eq!(output.values()[0], None);
        }

        #[test]
        fn test_decode_output_multiple() {
            // Array with multiple results
            let output = JsonToggleOutput::decode(b"*3\r\n:1\r\n:0\r\n$-1\r\n").unwrap();
            assert_eq!(output.values().len(), 3);
            assert_eq!(output.values()[0], Some(1));
            assert_eq!(output.values()[1], Some(0));
            assert_eq!(output.values()[2], None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonToggleOutput::decode(b"-ERR not a boolean\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.flag".into())];
            let input = JsonToggleInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_wrong_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = JsonToggleInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonToggleInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
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
        async fn test_json_toggle_true_to_false() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("togglekey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"active":true}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonToggleInput {
                                key: RedisKey::String("togglekey".into()),
                                path: RedisJsonValue::String("$.active".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("toggle failed");

                    let output = JsonToggleOutput::decode(&result).expect("decode failed");
                    assert!(output.is_false(), "true should toggle to false");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_toggle_false_to_true() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("togglekey2".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"active":false}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonToggleInput {
                                key: RedisKey::String("togglekey2".into()),
                                path: RedisJsonValue::String("$.active".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("toggle failed");

                    let output = JsonToggleOutput::decode(&result).expect("decode failed");
                    assert!(output.is_true(), "false should toggle to true");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_toggle_multiple_paths() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("multitogg".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"a":{"flag":true},"b":{"flag":false}}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonToggleInput {
                                key: RedisKey::String("multitogg".into()),
                                path: RedisJsonValue::String("$..flag".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("toggle failed");

                    let output = JsonToggleOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_toggle_non_boolean() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("nonbool".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"value":"string"}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonToggleInput {
                                key: RedisKey::String("nonbool".into()),
                                path: RedisJsonValue::String("$.value".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("toggle failed");

                    let output = JsonToggleOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values()[0], None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_toggle_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2toggle".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"flag":true}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonToggleInput {
                        key: RedisKey::String("r2toggle".into()),
                        path: RedisJsonValue::String("$.flag".into()),
                    }
                    .command(),
                )
                .await
                .expect("toggle failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = JsonToggleOutput::decode(&result).expect("decode failed");
            assert!(output.is_false());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_toggle_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r3toggle".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"flag":false}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonToggleInput {
                        key: RedisKey::String("r3toggle".into()),
                        path: RedisJsonValue::String("$.flag".into()),
                    }
                    .command(),
                )
                .await
                .expect("toggle failed");

            let output = JsonToggleOutput::decode(&result).expect("decode failed");
            assert!(output.is_true());

            ctx.stop().await;
        }
    }
}
