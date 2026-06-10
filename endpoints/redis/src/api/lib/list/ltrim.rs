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

const API_INFO: ApiInfo<RedisApi, LtrimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Ltrim,
    "Removes elements from both ends of a list. Deletes the list if all elements were trimmed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LTRIM`
/// https://redis.io/docs/latest/commands/ltrim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LtrimInput {
    pub(crate) key: RedisKey,
    pub(crate) start: RedisJsonValue,
    pub(crate) stop: RedisJsonValue,
}

impl Serialize for LtrimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LtrimInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;
        state.end()
    }
}

impl_redis_operation!(LtrimInput, API_INFO, { key, start, stop });

impl RedisCommandInput for LtrimInput {
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
            return Err(EpError::request(format!("LTRIM requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            start: args[1].clone(),
            stop: args[2].clone(),
        })
    }
}

/// Output for Redis LTRIM command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LtrimOutput {
    success: bool,
}

impl LtrimOutput {
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
                other => Err(EpError::parse(format!("unexpected LTRIM response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => Ok(Self {
                    success: String::from_utf8(data).map_err(EpError::parse)? == "OK",
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LTRIM response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LtrimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LtrimOutput", 1)?;
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
            let input = LtrimInput {
                key: RedisKey::String("mylist".into()),
                start: RedisJsonValue::Integer(0),
                stop: RedisJsonValue::Integer(2),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nLTRIM\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(-1),
            ];
            let input = LtrimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LtrimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = LtrimOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LtrimOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
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
        async fn test_ltrim_keep_first_two() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("ltrim_list".into()),
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
                            &LtrimInput {
                                key: RedisKey::String("ltrim_list".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LtrimOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    let len_result =
                        ctx.raw(&LlenInput { key: RedisKey::String("ltrim_list".into()) }.command()).await.expect("llen failed");
                    let len_output = LlenOutput::decode(&len_result).expect("decode");
                    assert_eq!(len_output.length(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ltrim_negative_indices() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("ltrim_neg".into()),
                            elements: vec![
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("2".into()),
                                RedisJsonValue::String("3".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LtrimInput {
                                key: RedisKey::String("ltrim_neg".into()),
                                start: RedisJsonValue::Integer(-2),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LtrimOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    let len_result =
                        ctx.raw(&LlenInput { key: RedisKey::String("ltrim_neg".into()) }.command()).await.expect("llen failed");
                    let len_output = LlenOutput::decode(&len_result).expect("decode");
                    assert_eq!(len_output.length(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ltrim_empty_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("ltrim_empty".into()),
                            elements: vec![RedisJsonValue::String("x".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    // start > stop results in empty list
                    let result = ctx
                        .raw(
                            &LtrimInput {
                                key: RedisKey::String("ltrim_empty".into()),
                                start: RedisJsonValue::Integer(1),
                                stop: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LtrimOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    let len_result =
                        ctx.raw(&LlenInput { key: RedisKey::String("ltrim_empty".into()) }.command()).await.expect("llen failed");
                    let len_output = LlenOutput::decode(&len_result).expect("decode");
                    assert_eq!(len_output.length(), 0);
                })
            })
            .await;
        }
    }
}
