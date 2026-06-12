use crate::api::lib::json::JsonMsetEntry;
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

const API_INFO: ApiInfo<RedisApi, JsonMsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonMset,
    "Sets or updates the JSON value of one or more keys",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `JSON.MSET`
/// https://redis.io/docs/latest/commands/json.mset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonMsetInput {
    set: Vec<JsonMsetEntry>,
}

impl Serialize for JsonMsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMsetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("set", &self.set)?;
        state.end()
    }
}

impl_redis_operation!(JsonMsetInput, API_INFO, { set });

impl RedisCommandInput for JsonMsetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.set.iter().map(|v| v.key.clone()).collect()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for entry in &self.set {
            command.arg(&entry.key).arg(&entry.path).arg(&entry.value);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("JSON.MSET requires at least one triplet of key, path, value"));
        }

        if !args.len().is_multiple_of(3) {
            return Err(EpError::request("JSON.MSET requires arguments in triplets of key, path, value"));
        }

        let mut set = Vec::new();
        for chunk in args.chunks(3) {
            set.push(JsonMsetEntry {
                key: chunk[0].clone().try_into()?,
                path: chunk[1].clone(),
                value: chunk[2].clone(),
            });
        }

        Ok(Self { set })
    }
}

/// Output for Redis JSON.MSET command
///
/// Returns OK if all values were set successfully.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonMsetOutput {
    success: bool,
}

impl JsonMsetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if all values were set successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a JsonMsetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.as_slice() == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected JSON.MSET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.as_slice() == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected JSON.MSET response: {:?}", other))),
            },
        }
    }
}

impl Serialize for JsonMsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMsetOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_entry() {
            let input = JsonMsetInput {
                set: vec![JsonMsetEntry {
                    key: RedisKey::String("key1".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"foo":"bar"}"#.into()),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$9\r\nJSON.MSET\r\n"));
        }

        #[test]
        fn test_encode_command_multiple_entries() {
            let input = JsonMsetInput {
                set: vec![
                    JsonMsetEntry {
                        key: RedisKey::String("key1".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{"a":1}"#.into()),
                    },
                    JsonMsetEntry {
                        key: RedisKey::String("key2".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String(r#"{"b":2}"#.into()),
                    },
                ],
            };
            let cmd = input.command();
            // Should have 7 args: command + 2 * (key, path, value)
            assert!(cmd.starts_with(b"*7\r\n$9\r\nJSON.MSET\r\n"));
        }

        #[test]
        fn test_decode_output_ok_resp2() {
            let output = JsonMsetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_ok_resp3() {
            let output = JsonMsetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonMsetOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_single_triplet() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
            ];
            let input = JsonMsetInput::decode(args).unwrap();
            assert_eq!(input.set.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_triplets() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("{}".into()),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("$.path".into()),
                RedisJsonValue::String(r#""value""#.into()),
            ];
            let input = JsonMsetInput::decode(args).unwrap();
            assert_eq!(input.set.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = JsonMsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one triplet"));
        }

        #[test]
        fn test_decode_input_incomplete_triplet_fails() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("$".into())];
            let err = JsonMsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("triplets"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonMsetInput {
                set: vec![
                    JsonMsetEntry {
                        key: RedisKey::String("key1".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String("{}".into()),
                    },
                    JsonMsetEntry {
                        key: RedisKey::String("key2".into()),
                        path: RedisJsonValue::String("$".into()),
                        value: RedisJsonValue::String("{}".into()),
                    },
                ],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("key1".into()));
            assert_eq!(keys[1], RedisKey::String("key2".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::JsonGetOutput;
        use crate::api::lib::json::json_get::JsonGetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mset_single_key() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonMsetInput {
                                set: vec![JsonMsetEntry {
                                    key: RedisKey::String("mset1".into()),
                                    path: RedisJsonValue::String("$".into()),
                                    value: RedisJsonValue::String(r#"{"single":true}"#.into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonMsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mset_multiple_keys() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &JsonMsetInput {
                                set: vec![
                                    JsonMsetEntry {
                                        key: RedisKey::String("msetA".into()),
                                        path: RedisJsonValue::String("$".into()),
                                        value: RedisJsonValue::String(r#"{"keyA":1}"#.into()),
                                    },
                                    JsonMsetEntry {
                                        key: RedisKey::String("msetB".into()),
                                        path: RedisJsonValue::String("$".into()),
                                        value: RedisJsonValue::String(r#"{"keyB":2}"#.into()),
                                    },
                                    JsonMsetEntry {
                                        key: RedisKey::String("msetC".into()),
                                        path: RedisJsonValue::String("$".into()),
                                        value: RedisJsonValue::String(r#"{"keyC":3}"#.into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonMsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify all keys were set by getting them
                    for (key, _expected) in [("msetA", r#"{"keyA":1}"#), ("msetB", r#"{"keyB":2}"#), ("msetC", r#"{"keyC":3}"#)] {
                        let get_result = ctx
                            .raw(
                                &JsonGetInput {
                                    key: RedisKey::String(key.into()),
                                    path: Some(vec![RedisJsonValue::String("$".into())]),
                                    ..Default::default()
                                }
                                .command(),
                            )
                            .await
                            .expect("get failed");

                        let get_output = JsonGetOutput::decode(&get_result).expect("decode get");
                        assert!(get_output.value().is_some(), "Key {} should exist", key);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mset_overwrite_existing() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &JsonMsetInput {
                            set: vec![JsonMsetEntry {
                                key: RedisKey::String("overwrite".into()),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(r#"{"old":true}"#.into()),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Overwrite with new value
                    let result = ctx
                        .raw(
                            &JsonMsetInput {
                                set: vec![JsonMsetEntry {
                                    key: RedisKey::String("overwrite".into()),
                                    path: RedisJsonValue::String("$".into()),
                                    value: RedisJsonValue::String(r#"{"new":true}"#.into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = JsonMsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mset_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &JsonMsetInput {
                        set: vec![JsonMsetEntry {
                            key: RedisKey::String("r2mset".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String("{}".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return simple string");
            let output = JsonMsetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mset_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &JsonMsetInput {
                        set: vec![JsonMsetEntry {
                            key: RedisKey::String("r3mset".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String("{}".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = JsonMsetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
