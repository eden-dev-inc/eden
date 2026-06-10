use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{RedisCommandOutput, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, LrangeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Lrange, "Returns a range of elements from a list", ReqType::Read, true);

/// See official Redis documentation for `LRANGE`
/// https://redis.io/docs/latest/commands/lrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LrangeInput {
    pub(crate) key: RedisKey,
    pub(crate) start: RedisJsonValue,
    pub(crate) stop: RedisJsonValue,
}

impl Serialize for LrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LrangeInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;
        state.end()
    }
}

impl_redis_operation!(LrangeInput, API_INFO, { key, start, stop });

impl RedisCommandInput for LrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.start).arg(&self.stop);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("LRANGE requires 3 arguments, given {}", args.len())));
        }
        Ok(Self {
            key: args[0].clone().try_into()?,
            start: args[1].clone(),
            stop: args[2].clone(),
        })
    }
}

/// Output for Redis LRANGE command
#[derive(Debug, Clone)]
pub struct LrangeOutput(Vec<RedisJsonValue>);

impl LrangeOutput {
    pub fn new(elements: Vec<RedisJsonValue>) -> Self {
        Self(elements)
    }
    pub fn elements(&self) -> &Vec<RedisJsonValue> {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let elements = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                arr.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                data.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("LRANGE must return array, got {:?}", other)));
            }
        };
        Ok(Self(elements))
    }
}

impl RedisCommandOutput for LrangeOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Lrange
    }
    fn decode(bytes: &[u8]) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        Self::decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = LrangeInput {
                key: RedisKey::String("mylist".into()),
                start: RedisJsonValue::Integer(0),
                stop: RedisJsonValue::Integer(-1),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$6\r\nLRANGE\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = LrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = LrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_elements() {
            let output = LrangeOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.elements()[0], RedisJsonValue::from("a"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = LrangeOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LrangeInput {
                key: RedisKey::String("mylist".into()),
                start: RedisJsonValue::Integer(0),
                stop: RedisJsonValue::Integer(-1),
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
        async fn test_lrange_full_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrange_full".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("lrange_full".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let output = LrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrange_partial() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrange_partial".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                                RedisJsonValue::String("d".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("lrange_partial".into()),
                                start: RedisJsonValue::Integer(1),
                                stop: RedisJsonValue::Integer(2),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let output = LrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.elements()[0], RedisJsonValue::from("b"));
                    assert_eq!(output.elements()[1], RedisJsonValue::from("c"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrange_negative_indices() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lrange_neg".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("lrange_neg".into()),
                                start: RedisJsonValue::Integer(-2),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let output = LrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.elements()[0], RedisJsonValue::from("b"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lrange_empty_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("nonexistent_lrange".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let output = LrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
