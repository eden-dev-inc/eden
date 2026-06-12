use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Field, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hset,
    "Creates or modifies the value of a field in a hash",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HSET`
/// https://redis.io/docs/latest/commands/hset/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HsetInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<Field>,
}

impl Serialize for HsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HsetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HsetInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HsetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        for field in &self.fields {
            command.arg(&field.field).arg(&field.value);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("HSET requires at least 3 arguments, found {}", args.len())));
        }

        if args.len() % 2 != 1 {
            return Err(EpError::request(format!(
                "HSET requires odd number of arguments (key + field/value pairs), found {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let mut fields = Vec::new();

        // Parse field/value pairs starting from index 1
        for chunk in args[1..].chunks(2) {
            if chunk.len() != 2 {
                return Err(EpError::request("Invalid field/value pair"));
            }
            fields.push(Field { field: chunk[0].clone(), value: chunk[1].clone() });
        }

        Ok(HsetInput { key, fields })
    }
}

/// Output for Redis HSET command
///
/// Returns the number of fields that were added (not updated).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HsetOutput {
    /// Number of fields that were added (not fields that were updated)
    fields_added: i64,
}

impl HsetOutput {
    pub fn new(fields_added: i64) -> Self {
        Self { fields_added }
    }

    /// Get the number of fields that were added
    pub fn fields_added(&self) -> i64 {
        self.fields_added
    }

    /// Check if any new fields were added
    pub fn added_new_fields(&self) -> bool {
        self.fields_added > 0
    }

    /// Decode the Redis protocol response into a HsetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let fields_added = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HSET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HSET response: {:?}", other)));
                }
            },
        };

        Ok(Self { fields_added })
    }
}

impl Serialize for HsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HsetOutput", 1)?;
        state.serialize_field("fields_added", &self.fields_added)?;
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
            let input = HsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![Field::new(
                    RedisJsonValue::String("field1".into()),
                    RedisJsonValue::String("value1".into()),
                )],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n");
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![
                    Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                    Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HSET"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("v1"));
            assert!(cmd_str.contains("f2"));
            assert!(cmd_str.contains("v2"));
        }

        #[test]
        fn test_decode_fields_added() {
            let output = HsetOutput::decode(b":2\r\n").unwrap();
            assert_eq!(output.fields_added(), 2);
            assert!(output.added_new_fields());
        }

        #[test]
        fn test_decode_no_new_fields() {
            let output = HsetOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.fields_added(), 0);
            assert!(!output.added_new_fields());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HsetOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = HsetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.fields.len(), 1);
            assert_eq!(input.fields[0].field, RedisJsonValue::String("field".into()));
            assert_eq!(input.fields[0].value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_multiple_fields() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("v1".into()),
                RedisJsonValue::String("f2".into()),
                RedisJsonValue::String("v2".into()),
            ];
            let input = HsetInput::decode(args).unwrap();
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let err = HsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_even_args_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("v1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let err = HsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("odd number"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_field_accessors() {
            let field = Field::new(RedisJsonValue::String("myfield".into()), RedisJsonValue::String("myvalue".into()));
            assert_eq!(field.field(), &RedisJsonValue::String("myfield".into()));
            assert_eq!(field.value(), &RedisJsonValue::String("myvalue".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_new_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhset_hash\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HsetInput {
                                key: RedisKey::String("hset_hash".into()),
                                fields: vec![Field::new(
                                    RedisJsonValue::String("field1".into()),
                                    RedisJsonValue::String("value1".into()),
                                )],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.fields_added(), 1);
                    assert!(output.added_new_fields());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_update_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhset_update\r\n").await.expect("raw failed");

                    // First set
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hset_update".into()),
                            fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v1".into()))],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Update same field
                    let result = ctx
                        .raw(
                            &HsetInput {
                                key: RedisKey::String("hset_update".into()),
                                fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v2".into()))],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.fields_added(), 0, "Update should not add new field");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_multiple_fields() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhset_multi\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HsetInput {
                                key: RedisKey::String("hset_multi".into()),
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

                    let output = HsetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.fields_added(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_mixed_new_and_update() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhset_mixed\r\n").await.expect("raw failed");

                    // Create one field
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hset_mixed".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("existing".into()),
                                RedisJsonValue::String("old".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Update existing + add new
                    let result = ctx
                        .raw(
                            &HsetInput {
                                key: RedisKey::String("hset_mixed".into()),
                                fields: vec![
                                    Field::new(RedisJsonValue::String("existing".into()), RedisJsonValue::String("new".into())),
                                    Field::new(RedisJsonValue::String("new_field".into()), RedisJsonValue::String("value".into())),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.fields_added(), 1, "Only new_field should be counted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhset_pip1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhset_pip2\r\n").await.expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &HsetInput {
                            key: RedisKey::String("hset_pip1".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                                Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                            ],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HsetInput {
                            key: RedisKey::String("hset_pip2".into()),
                            fields: vec![Field::new(RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into()))],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = HsetOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.fields_added(), 2);

                    let out2 = HsetOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.fields_added(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhset_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HsetInput {
                        key: RedisKey::String("hset_r2".into()),
                        fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HsetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.fields_added(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hset_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhset_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HsetInput {
                        key: RedisKey::String("hset_r3".into()),
                        fields: vec![
                            Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                            Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                        ],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HsetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.fields_added(), 2);

            ctx.stop().await;
        }
    }
}
