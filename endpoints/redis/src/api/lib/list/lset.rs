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

const API_INFO: ApiInfo<RedisApi, LsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lset,
    "Sets the value of an element in a list by its index",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LSET`
/// https://redis.io/docs/latest/commands/lset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LsetInput {
    pub(crate) key: RedisKey,
    pub(crate) index: RedisJsonValue,
    pub(crate) element: RedisJsonValue,
}

impl Serialize for LsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LsetInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("element", &self.element)?;
        state.end()
    }
}

impl_redis_operation!(LsetInput, API_INFO, { key, index, element });

impl RedisCommandInput for LsetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.index).arg(&self.element);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("LSET requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            index: args[1].clone(),
            element: args[2].clone(),
        })
    }
}

/// Output for Redis LSET command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LsetOutput {
    success: bool,
}

impl LsetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    pub fn success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => Ok(Self {
                    success: String::from_utf8(s).map_err(EpError::parse)? == "OK",
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LSET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => Ok(Self {
                    success: String::from_utf8(data).map_err(EpError::parse)? == "OK",
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LSET response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LsetOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = LsetInput {
                key: RedisKey::String("mylist".into()),
                index: RedisJsonValue::Integer(0),
                element: RedisJsonValue::String("newval".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$4\r\nLSET\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("value".into()),
            ];
            let input = LsetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = LsetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LsetOutput::decode(b"-ERR index out of range\r\n").unwrap_err();
            assert!(err.to_string().contains("index out of range"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lindex::{LindexInput, LindexOutput};
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lset_valid_index() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lset_list".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LsetInput {
                                key: RedisKey::String("lset_list".into()),
                                index: RedisJsonValue::Integer(0),
                                element: RedisJsonValue::String("new".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LsetOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    let idx_result = ctx
                        .raw(
                            &LindexInput {
                                key: RedisKey::String("lset_list".into()),
                                index: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("lindex failed");
                    let idx_output = LindexOutput::decode(&idx_result).expect("decode");
                    assert_eq!(idx_output.value(), Some(&RedisJsonValue::from("new")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lset_negative_index() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lset_neg".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LsetInput {
                                key: RedisKey::String("lset_neg".into()),
                                index: RedisJsonValue::Integer(-1),
                                element: RedisJsonValue::String("last".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LsetOutput::decode(&result).expect("decode failed");
                    assert!(output.success());
                })
            })
            .await;
        }
    }
}
