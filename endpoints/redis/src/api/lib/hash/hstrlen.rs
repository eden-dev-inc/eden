use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, HstrlenInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hstrlen,
    "Returns the length of the value of a field in a hash",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HSTRLEN`
/// https://redis.io/docs/latest/commands/hstrlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HstrlenInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
}

impl Serialize for HstrlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HstrlenInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.end()
    }
}

impl_redis_operation!(
    HstrlenInput,
    API_INFO,
    {key, field}
);

impl RedisCommandInput for HstrlenInput {
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
        if args.len() < 2 {
            return Err(EpError::request(format!("HSTRLEN requires 2 arguments, given {}", args.len())));
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "HSTRLEN takes 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, field: args[1].clone() })
    }
}

/// Output for Redis HSTRLEN command
///
/// Returns the length of the string value associated with field in the hash.
/// Returns 0 if the key or field does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HstrlenOutput {
    /// The length of the field value, or 0 if key/field doesn't exist
    length: i64,
}

impl HstrlenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the field value length
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the field is empty or doesn't exist
    /// Note: Returns true for both missing fields and empty string values.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into a HstrlenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HSTRLEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HSTRLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for HstrlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HstrlenOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = HstrlenInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("myfield".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nHSTRLEN\r\n$6\r\nmyhash\r\n$7\r\nmyfield\r\n");
        }

        #[test]
        fn test_decode_positive_length() {
            let output = HstrlenOutput::decode(b":11\r\n").unwrap();
            assert_eq!(output.length(), 11);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_zero_length() {
            let output = HstrlenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HstrlenOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let input = HstrlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.field, RedisJsonValue::String("field".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = HstrlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HstrlenInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_existing_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hstrlen_hash".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("Hello World".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HstrlenInput {
                                key: RedisKey::String("hstrlen_hash".into()),
                                field: RedisJsonValue::String("field1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HstrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 11);
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_missing_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hstrlen_hash2".into()),
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
                            &HstrlenInput {
                                key: RedisKey::String("hstrlen_hash2".into()),
                                field: RedisJsonValue::String("missing".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HstrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_missing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &HstrlenInput {
                                key: RedisKey::String("hstrlen_nonexistent".into()),
                                field: RedisJsonValue::String("field".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HstrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_empty_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hstrlen_empty".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("emptyfield".into()),
                                RedisJsonValue::String("".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HstrlenInput {
                                key: RedisKey::String("hstrlen_empty".into()),
                                field: RedisJsonValue::String("emptyfield".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HstrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hstrlen_pipe".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("short".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("this is longer".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &HstrlenInput {
                            key: RedisKey::String("hstrlen_pipe".into()),
                            field: RedisJsonValue::String("f1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HstrlenInput {
                            key: RedisKey::String("hstrlen_pipe".into()),
                            field: RedisJsonValue::String("f2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &HstrlenInput {
                            key: RedisKey::String("hstrlen_pipe".into()),
                            field: RedisJsonValue::String("missing".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = HstrlenOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.length(), 5);

                    let out2 = HstrlenOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.length(), 14);

                    let out3 = HstrlenOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("r2hash".into()),
                    fields: vec![Field::new(
                        RedisJsonValue::String("f".into()),
                        RedisJsonValue::String("test".into()),
                    )],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HstrlenInput {
                        key: RedisKey::String("r2hash".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HstrlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 4);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hstrlen_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("r3hash".into()),
                    fields: vec![Field::new(
                        RedisJsonValue::String("f".into()),
                        RedisJsonValue::String("hello".into()),
                    )],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HstrlenInput {
                        key: RedisKey::String("r3hash".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HstrlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 5);

            ctx.stop().await;
        }
    }
}
