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

const API_INFO: ApiInfo<RedisApi, LpushInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lpush,
    "Prepends one or more elements to a list. Creates the key if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LPUSH`
/// https://redis.io/docs/latest/commands/lpush/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LpushInput {
    pub(crate) key: RedisKey,
    pub(crate) elements: Vec<RedisJsonValue>,
}

impl Serialize for LpushInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LpushInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("elements", &self.elements)?;
        state.end()
    }
}

impl_redis_operation!(LpushInput, API_INFO, { key, elements });

impl RedisCommandInput for LpushInput {
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
            return Err(EpError::request(format!("LPUSH requires at least 2 arguments, given {}", args.len())));
        }
        let key = args[0].clone().try_into()?;
        let elements = args[1..].to_vec();
        Ok(Self { key, elements })
    }
}

/// Output for Redis LPUSH command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LpushOutput {
    /// The length of the list after the push operation
    length: i64,
}

impl LpushOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }
    pub fn length(&self) -> i64 {
        self.length
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(Self { length: n }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LPUSH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { length: data }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LPUSH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LpushOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LpushOutput", 1)?;
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
        fn test_encode_command_single() {
            let input = LpushInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("value".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$5\r\nLPUSH\r\n"));
        }

        #[test]
        fn test_encode_command_multiple() {
            let input = LpushInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nLPUSH\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::String("elem".into())];
            let input = LpushInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert_eq!(input.elements.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_elements() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let input = LpushInput::decode(args).unwrap();
            assert_eq!(input.elements.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LpushInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output() {
            let output = LpushOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.length(), 5);
        }

        #[test]
        fn test_decode_output_error() {
            let err = LpushOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LpushInput {
                key: RedisKey::String("mylist".into()),
                elements: vec![RedisJsonValue::String("a".into())],
            };
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lrange::{LrangeInput, LrangeOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpush_creates_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LpushInput {
                                key: RedisKey::String("lpush_new".into()),
                                elements: vec![RedisJsonValue::String("first".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("lpush failed");

                    let output = LpushOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpush_prepends() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &LpushInput {
                            key: RedisKey::String("lpush_prepend".into()),
                            elements: vec![RedisJsonValue::String("second".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("lpush failed");

                    ctx.raw(
                        &LpushInput {
                            key: RedisKey::String("lpush_prepend".into()),
                            elements: vec![RedisJsonValue::String("first".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("lpush failed");

                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("lpush_prepend".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let output = LrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements().len(), 2);
                    assert_eq!(output.elements()[0], RedisJsonValue::from("first"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpush_multiple_elements() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LpushInput {
                                key: RedisKey::String("lpush_multi".into()),
                                elements: vec![
                                    RedisJsonValue::String("a".into()),
                                    RedisJsonValue::String("b".into()),
                                    RedisJsonValue::String("c".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("lpush failed");

                    let output = LpushOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);
                })
            })
            .await;
        }
    }
}
