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

const API_INFO: ApiInfo<RedisApi, IncrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Incrby,
    "Increments the number stored at key by increment. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `INCRBY`
/// https://redis.io/docs/latest/commands/incrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct IncrbyInput {
    pub(crate) key: RedisKey,
    pub(crate) increment: RedisJsonValue,
}

impl Serialize for IncrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IncrbyInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("increment", &self.increment)?;
        state.end()
    }
}

impl_redis_operation!(
    IncrbyInput,
    API_INFO,
    {key, increment}
);

impl RedisCommandInput for IncrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.increment);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("INCRBY requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "INCRBY expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, increment: args[1].clone() })
    }
}

/// Output for Redis INCRBY command
///
/// Returns the value of key after the increment.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct IncrbyOutput {
    /// The value after incrementing
    value: i64,
}

impl IncrbyOutput {
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    /// Get the incremented value
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Decode the Redis protocol response into an IncrbyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected INCRBY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected INCRBY response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for IncrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("IncrbyOutput", 1)?;
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
            let input = IncrbyInput {
                key: RedisKey::String("mykey".into()),
                increment: RedisJsonValue::Integer(5),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nINCRBY\r\n$5\r\nmykey\r\n$1\r\n5\r\n");
        }

        #[test]
        fn test_decode_positive_value() {
            let output = IncrbyOutput::decode(b":105\r\n").unwrap();
            assert_eq!(output.value(), 105);
        }

        #[test]
        fn test_decode_zero_value() {
            let output = IncrbyOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.value(), 0);
        }

        #[test]
        fn test_decode_negative_value() {
            let output = IncrbyOutput::decode(b":-10\r\n").unwrap();
            assert_eq!(output.value(), -10);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = IncrbyOutput::decode(b"-ERR value is not an integer or out of range\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(10)];
            let input = IncrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.increment, RedisJsonValue::Integer(10));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = IncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = IncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = IncrbyInput {
                key: RedisKey::String("mykey".into()),
                increment: RedisJsonValue::Integer(5),
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
        async fn test_incrby_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // INCRBY on non-existent key initializes to 0, then increments
                    let result = ctx
                        .raw(
                            &IncrbyInput {
                                key: RedisKey::String("incrby_new".into()),
                                increment: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_existing_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrby_exist".into()),
                            value: RedisJsonValue::String("100".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &IncrbyInput {
                                key: RedisKey::String("incrby_exist".into()),
                                increment: RedisJsonValue::Integer(30),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 130);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_negative_increment() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrby_neg".into()),
                            value: RedisJsonValue::String("50".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &IncrbyInput {
                                key: RedisKey::String("incrby_neg".into()),
                                increment: RedisJsonValue::Integer(-20),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 30);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrby_pipe".into()),
                            value: RedisJsonValue::String("0".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &IncrbyInput {
                            key: RedisKey::String("incrby_pipe".into()),
                            increment: RedisJsonValue::Integer(100),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &IncrbyInput {
                            key: RedisKey::String("incrby_pipe".into()),
                            increment: RedisJsonValue::Integer(200),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &IncrbyInput {
                            key: RedisKey::String("incrby_pipe".into()),
                            increment: RedisJsonValue::Integer(300),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = IncrbyOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.value(), 100);

                    let out2 = IncrbyOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.value(), 300);

                    let out3 = IncrbyOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.value(), 600);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_resp2_integer_format() {
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
                    &IncrbyInput {
                        key: RedisKey::String("r2key".into()),
                        increment: RedisJsonValue::Integer(25),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":125\r\n", "RESP2 integer format");
            let output = IncrbyOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), 125);

            ctx.stop().await;
        }
    }
}
