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

const API_INFO: ApiInfo<RedisApi, DecrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Decrby,
    "The DECRBY command reduces the value stored at the specified key by the specified decrement. If the key does not exist, it is initialized with a value of 0 before performing the operation. If the key's value is not of the correct type or cannot be represented as an integer, an error is returned. This operation is limited to 64-bit signed integers",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `DECRBY`
/// https://redis.io/docs/latest/commands/decrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DecrbyInput {
    pub(crate) key: RedisKey,
    pub(crate) decrement: RedisJsonValue,
}

impl Serialize for DecrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DecrbyInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("decrement", &self.decrement)?;
        state.end()
    }
}

impl_redis_operation!(
    DecrbyInput,
    API_INFO,
    {key, decrement}
);

impl RedisCommandInput for DecrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.decrement);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("DECRBY requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "DECRBY takes 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, decrement: args[1].clone() })
    }
}

/// Output for Redis DECRBY command
///
/// Returns the value of key after the decrement.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DecrbyOutput {
    /// The value after decrementing
    value: i64,
}

impl DecrbyOutput {
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    /// Get the decremented value
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Decode the Redis protocol response into a DecrbyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected DECRBY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected DECRBY response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for DecrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("DecrbyOutput", 1)?;
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
            let input = DecrbyInput {
                key: RedisKey::String("mykey".into()),
                decrement: RedisJsonValue::Integer(5),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nDECRBY\r\n$5\r\nmykey\r\n$1\r\n5\r\n");
        }

        #[test]
        fn test_decode_positive_value() {
            let output = DecrbyOutput::decode(b":95\r\n").unwrap();
            assert_eq!(output.value(), 95);
        }

        #[test]
        fn test_decode_zero_value() {
            let output = DecrbyOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.value(), 0);
        }

        #[test]
        fn test_decode_negative_value() {
            let output = DecrbyOutput::decode(b":-10\r\n").unwrap();
            assert_eq!(output.value(), -10);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = DecrbyOutput::decode(b"-ERR value is not an integer or out of range\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(10)];
            let input = DecrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.decrement, RedisJsonValue::Integer(10));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = DecrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = DecrbyInput {
                key: RedisKey::String("mykey".into()),
                decrement: RedisJsonValue::Integer(5),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // DECRBY on non-existent key initializes to 0, then decrements
                    let result = ctx
                        .raw(
                            &DecrbyInput {
                                key: RedisKey::String("decrby_new".into()),
                                decrement: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = DecrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), -5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_existing_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decrby_exist".into()),
                            value: RedisJsonValue::String("100".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &DecrbyInput {
                                key: RedisKey::String("decrby_exist".into()),
                                decrement: RedisJsonValue::Integer(30),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = DecrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 70);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_large_decrement() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decrby_large".into()),
                            value: RedisJsonValue::String("50".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &DecrbyInput {
                                key: RedisKey::String("decrby_large".into()),
                                decrement: RedisJsonValue::Integer(100),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = DecrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), -50);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decrby_pipe".into()),
                            value: RedisJsonValue::String("1000".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &DecrbyInput {
                            key: RedisKey::String("decrby_pipe".into()),
                            decrement: RedisJsonValue::Integer(100),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &DecrbyInput {
                            key: RedisKey::String("decrby_pipe".into()),
                            decrement: RedisJsonValue::Integer(200),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &DecrbyInput {
                            key: RedisKey::String("decrby_pipe".into()),
                            decrement: RedisJsonValue::Integer(300),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = DecrbyOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.value(), 900);

                    let out2 = DecrbyOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.value(), 700);

                    let out3 = DecrbyOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.value(), 400);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("100".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &DecrbyInput {
                        key: RedisKey::String("r2key".into()),
                        decrement: RedisJsonValue::Integer(25),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":75\r\n", "RESP2 integer format");
            let output = DecrbyOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), 75);

            ctx.stop().await;
        }
    }
}
