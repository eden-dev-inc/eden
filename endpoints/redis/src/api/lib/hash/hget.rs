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

const API_INFO: ApiInfo<RedisApi, HgetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hget, "Returns the value of a field in a hash", ReqType::Read, true);

/// See official Redis documentation for `HGET`
/// https://redis.io/docs/latest/commands/hget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HgetInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
}

impl Serialize for HgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HgetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.end()
    }
}

impl_redis_operation!(
    HgetInput,
    API_INFO,
    {key, field}
);

impl RedisCommandInput for HgetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.field);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("HGET requires exactly 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, field: args[1].clone() })
    }
}

/// Output for Redis HGET command
///
/// Returns the value associated with field in the hash, or nil if field or key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HgetOutput {
    value: Option<RedisJsonValue>,
}

impl HgetOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the value
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if the field exists
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Decode the Redis protocol response into a HgetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Some(RedisJsonValue::String(String::from_utf8_lossy(&data).to_string())),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HGET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(RedisJsonValue::String(String::from_utf8_lossy(&data).to_string())),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HGET response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for HgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HgetOutput", 1)?;
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
        fn test_encode_command() {
            let input = HgetInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n");
        }

        #[test]
        fn test_decode_output_value() {
            let output = HgetOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(&RedisJsonValue::String("value".into())));
        }

        #[test]
        fn test_decode_output_null() {
            let output = HgetOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert!(output.value().is_none());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HgetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HgetInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("f".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("field1".into())];
            let input = HgetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = HgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let err = HgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
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
        async fn test_hget_existing_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhget_test1\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hget_test1".into()),
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
                            &HgetInput {
                                key: RedisKey::String("hget_test1".into()),
                                field: RedisJsonValue::String("field1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.value(), Some(&RedisJsonValue::String("value1".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hget_nonexistent_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhget_test2\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hget_test2".into()),
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
                            &HgetInput {
                                key: RedisKey::String("hget_test2".into()),
                                field: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hget_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhget_test3\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HgetInput {
                                key: RedisKey::String("hget_test3".into()),
                                field: RedisJsonValue::String("anyfield".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hget_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhget_pip\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hget_pip".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &HgetInput {
                            key: RedisKey::String("hget_pip".into()),
                            field: RedisJsonValue::String("f1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HgetInput {
                            key: RedisKey::String("hget_pip".into()),
                            field: RedisJsonValue::String("f2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HgetInput {
                            key: RedisKey::String("hget_pip".into()),
                            field: RedisJsonValue::String("missing".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = HgetOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.value(), Some(&RedisJsonValue::String("v1".into())));

                    let out2 = HgetOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.value(), Some(&RedisJsonValue::String("v2".into())));

                    let out3 = HgetOutput::decode(responses[2]).expect("decode third");
                    assert!(!out3.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hget_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhget_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hget_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HgetInput {
                        key: RedisKey::String("hget_r2".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = HgetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), Some(&RedisJsonValue::String("v".into())));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hget_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhget_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hget_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HgetInput {
                        key: RedisKey::String("hget_r3".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HgetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), Some(&RedisJsonValue::String("v".into())));

            ctx.stop().await;
        }
    }
}
