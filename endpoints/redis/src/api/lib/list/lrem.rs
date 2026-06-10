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

const API_INFO: ApiInfo<RedisApi, LremInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lrem,
    "Removes elements from a list. Deletes the list if the last element was removed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LREM`
/// https://redis.io/docs/latest/commands/lrem/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LremInput {
    pub(crate) key: RedisKey,
    pub(crate) count: RedisJsonValue,
    pub(crate) element: RedisJsonValue,
}

impl Serialize for LremInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LremInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("element", &self.element)?;
        state.end()
    }
}

impl_redis_operation!(LremInput, API_INFO, { key, count, element });

impl RedisCommandInput for LremInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.count).arg(&self.element);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("LREM requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            count: args[1].clone(),
            element: args[2].clone(),
        })
    }
}

/// Output for Redis LREM command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LremOutput {
    removed: i64,
}

impl LremOutput {
    pub fn new(removed: i64) -> Self {
        Self { removed }
    }

    pub fn removed(&self) -> i64 {
        self.removed
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(Self { removed: n }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LREM response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { removed: data }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LREM response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LremOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LremOutput", 1)?;
        state.serialize_field("removed", &self.removed)?;
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
            let input = LremInput {
                key: RedisKey::String("mylist".into()),
                count: RedisJsonValue::Integer(2),
                element: RedisJsonValue::String("hello".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$4\r\nLREM\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("elem".into()),
            ];
            let input = LremInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LremInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = LremOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.removed(), 3);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = LremOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.removed(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = LremOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::llen::{LlenInput, LlenOutput};
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrem_remove_all() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrem_list".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("a".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LremInput {
                                key: RedisKey::String("lrem_list".into()),
                                count: RedisJsonValue::Integer(0),
                                element: RedisJsonValue::String("a".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrem_from_head() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrem_head".into()),
                            elements: vec![
                                RedisJsonValue::String("x".into()),
                                RedisJsonValue::String("y".into()),
                                RedisJsonValue::String("x".into()),
                                RedisJsonValue::String("x".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LremInput {
                                key: RedisKey::String("lrem_head".into()),
                                count: RedisJsonValue::Integer(2),
                                element: RedisJsonValue::String("x".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);

                    let len_result =
                        ctx.raw(&LlenInput { key: RedisKey::String("lrem_head".into()) }.command()).await.expect("llen failed");
                    let len_output = LlenOutput::decode(&len_result).expect("decode");
                    assert_eq!(len_output.length(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrem_not_found() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrem_notfound".into()),
                            elements: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LremInput {
                                key: RedisKey::String("lrem_notfound".into()),
                                count: RedisJsonValue::Integer(0),
                                element: RedisJsonValue::String("z".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }
    }
}
