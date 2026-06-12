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

const API_INFO: ApiInfo<RedisApi, HincrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hincrby,
    "Increments the integer value of a hash field by a number",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HINCRBY`
/// https://redis.io/docs/latest/commands/hincrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HincrbyInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
    pub(crate) increment: RedisJsonValue,
}

impl Serialize for HincrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HincrbyInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.serialize_field("increment", &self.increment)?;
        state.end()
    }
}

impl_redis_operation!(
    HincrbyInput,
    API_INFO,
    {key, field, increment}
);

impl RedisCommandInput for HincrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.field).arg(&self.increment);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("HINCRBY requires exactly 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            field: args[1].clone(),
            increment: args[2].clone(),
        })
    }
}

/// Output for Redis HINCRBY command
///
/// Returns the value of the field after the increment operation.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HincrbyOutput {
    /// The value of the field after the increment
    value: i64,
}

impl HincrbyOutput {
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    /// Get the value after increment
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Decode the Redis protocol response into a HincrbyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HINCRBY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HINCRBY response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for HincrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HincrbyOutput", 1)?;
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
            let input = HincrbyInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field1".into()),
                increment: RedisJsonValue::Integer(5),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HINCRBY"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_negative_increment() {
            let input = HincrbyInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("counter".into()),
                increment: RedisJsonValue::Integer(-10),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HINCRBY"));
            assert!(cmd_str.contains("-10"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = HincrbyOutput::decode(b":15\r\n").unwrap();
            assert_eq!(output.value(), 15);
        }

        #[test]
        fn test_decode_output_negative() {
            let output = HincrbyOutput::decode(b":-5\r\n").unwrap();
            assert_eq!(output.value(), -5);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = HincrbyOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.value(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HincrbyOutput::decode(b"-ERR hash value is not an integer\r\n").unwrap_err();
            assert!(err.to_string().contains("hash value is not an integer"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = HincrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.field, RedisJsonValue::String("field".into()));
            assert_eq!(input.increment, RedisJsonValue::Integer(10));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let err = HincrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::Integer(10),
                RedisJsonValue::String("extra".into()),
            ];
            let err = HincrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HincrbyInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("f".into()),
                increment: RedisJsonValue::Integer(1),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrby_new_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhincrby_test\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HincrbyInput {
                                key: RedisKey::String("hincrby_test".into()),
                                field: RedisJsonValue::String("counter".into()),
                                increment: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HincrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrby_existing_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhincrby_exists\r\n").await.expect("raw failed");

                    // Set initial value
                    ctx.raw(
                        &HincrbyInput {
                            key: RedisKey::String("hincrby_exists".into()),
                            field: RedisJsonValue::String("counter".into()),
                            increment: RedisJsonValue::Integer(10),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Increment again
                    let result = ctx
                        .raw(
                            &HincrbyInput {
                                key: RedisKey::String("hincrby_exists".into()),
                                field: RedisJsonValue::String("counter".into()),
                                increment: RedisJsonValue::Integer(3),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HincrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 13);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrby_negative_increment() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhincrby_neg\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HincrbyInput {
                            key: RedisKey::String("hincrby_neg".into()),
                            field: RedisJsonValue::String("counter".into()),
                            increment: RedisJsonValue::Integer(20),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HincrbyInput {
                                key: RedisKey::String("hincrby_neg".into()),
                                field: RedisJsonValue::String("counter".into()),
                                increment: RedisJsonValue::Integer(-8),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HincrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 12);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrby_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhincrby_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyInput {
                        key: RedisKey::String("hincrby_r2".into()),
                        field: RedisJsonValue::String("f".into()),
                        increment: RedisJsonValue::Integer(7),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HincrbyOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), 7);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrby_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhincrby_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyInput {
                        key: RedisKey::String("hincrby_r3".into()),
                        field: RedisJsonValue::String("f".into()),
                        increment: RedisJsonValue::Integer(42),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HincrbyOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), 42);

            ctx.stop().await;
        }
    }
}
