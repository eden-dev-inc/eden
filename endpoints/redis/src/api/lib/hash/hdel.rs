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

const API_INFO: ApiInfo<RedisApi, HdelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hdel,
    "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HDEL`
/// https://redis.io/docs/latest/commands/hdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HdelInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HdelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HdelInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.fields);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("HDEL requires at least 2 arguments (key + fields), given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[1..].to_vec() })
    }
}

/// Output for Redis HDEL command
///
/// Returns the number of fields that were removed from the hash.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HdelOutput {
    /// The number of fields that were deleted
    deleted: i64,
}

impl HdelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of deleted fields
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Check if any fields were deleted
    pub fn deleted_any(&self) -> bool {
        self.deleted > 0
    }

    /// Decode the Redis protocol response into a HdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HDEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HDEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for HdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HdelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_field() {
            let input = HdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nHDEL\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n");
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![
                    RedisJsonValue::String("f1".into()),
                    RedisJsonValue::String("f2".into()),
                    RedisJsonValue::String("f3".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HDEL"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
            assert!(cmd_str.contains("f3"));
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = HdelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
            assert!(!output.deleted_any());
        }

        #[test]
        fn test_decode_integer_one() {
            let output = HdelOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.deleted(), 1);
            assert!(output.deleted_any());
        }

        #[test]
        fn test_decode_integer_multiple() {
            let output = HdelOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.deleted(), 5);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HdelOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("field1".into())];
            let input = HdelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.fields.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_fields() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let input = HdelInput::decode(args).unwrap();
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = HdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_single_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhdel_single\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hdel_single".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HdelInput {
                                key: RedisKey::String("hdel_single".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_multiple_fields() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhdel_multi\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hdel_multi".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                                Field::new(RedisJsonValue::String("f3".into()), RedisJsonValue::String("v3".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HdelInput {
                                key: RedisKey::String("hdel_multi".into()),
                                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_nonexistent_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhdel_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hdel_missing".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("exists".into()),
                                RedisJsonValue::String("value".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HdelInput {
                                key: RedisKey::String("hdel_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_mixed_existing_and_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhdel_mixed\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hdel_mixed".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HdelInput {
                                key: RedisKey::String("hdel_mixed".into()),
                                fields: vec![
                                    RedisJsonValue::String("f1".into()),
                                    RedisJsonValue::String("nonexistent".into()),
                                    RedisJsonValue::String("f2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 2, "Only existing fields should be counted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhdel_pip\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hdel_pip".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                                Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                                Field::new(RedisJsonValue::String("c".into()), RedisJsonValue::String("3".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &HdelInput {
                            key: RedisKey::String("hdel_pip".into()),
                            fields: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HdelInput {
                            key: RedisKey::String("hdel_pip".into()),
                            fields: vec![RedisJsonValue::String("b".into()), RedisJsonValue::String("c".into())],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = HdelOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.deleted(), 1);

                    let out2 = HdelOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.deleted(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhdel_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hdel_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HdelInput {
                        key: RedisKey::String("hdel_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HdelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hdel_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhdel_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hdel_r3".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                        Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HdelInput {
                        key: RedisKey::String("hdel_r3".into()),
                        fields: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HdelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 2);

            ctx.stop().await;
        }
    }
}
