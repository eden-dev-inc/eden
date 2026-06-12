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

const API_INFO: ApiInfo<RedisApi, RpushxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Rpushx,
    "Appends an element to a list only when the list exists",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RPUSHX`
/// https://redis.io/docs/latest/commands/rpushx/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RpushxInput {
    pub(crate) key: RedisKey,
    pub(crate) elements: Vec<RedisJsonValue>,
}

impl Serialize for RpushxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RpushxInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("elements", &self.elements)?;
        state.end()
    }
}

impl_redis_operation!(RpushxInput, API_INFO, { key, elements });

impl RedisCommandInput for RpushxInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.elements);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("RPUSHX requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let elements = args[1..].to_vec();

        Ok(Self { key, elements })
    }
}

/// Output for Redis RPUSHX command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RpushxOutput {
    /// The length of the list after the push, or 0 if key doesn't exist
    length: i64,
}

impl RpushxOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the new length of the list (0 if key didn't exist)
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the key existed (push happened)
    pub fn key_existed(&self) -> bool {
        self.length > 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(Self { length: n }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected RPUSHX response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { length: data }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected RPUSHX response: {:?}", other))),
            },
        }
    }
}

impl Serialize for RpushxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RpushxOutput", 1)?;
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
            let input = RpushxInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("value".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nRPUSHX\r\n"));
        }

        #[test]
        fn test_encode_command_multiple_elements() {
            let input = RpushxInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$6\r\nRPUSHX\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::String("value".into())];
            let input = RpushxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert_eq!(input.elements.len(), 1);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = RpushxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = RpushxOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.length(), 3);
            assert!(output.key_existed());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = RpushxOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(!output.key_existed());
        }

        #[test]
        fn test_decode_output_error() {
            let err = RpushxOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = RpushxInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("v".into())],
            };
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpushx_existing_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create the list first
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rpushx_exists".into()),
                            elements: vec![RedisJsonValue::String("initial".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &RpushxInput {
                                key: RedisKey::String("rpushx_exists".into()),
                                elements: vec![RedisJsonValue::String("added".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpushxOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 2);
                    assert!(output.key_existed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpushx_nonexistent_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &RpushxInput {
                                key: RedisKey::String("rpushx_noexist".into()),
                                elements: vec![RedisJsonValue::String("value".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpushxOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(!output.key_existed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpushx_multiple_elements() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rpushx_multi".into()),
                            elements: vec![RedisJsonValue::String("start".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &RpushxInput {
                                key: RedisKey::String("rpushx_multi".into()),
                                elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpushxOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);
                })
            })
            .await;
        }
    }
}
