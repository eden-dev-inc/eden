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

const API_INFO: ApiInfo<RedisApi, RpoplpushInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Rpoplpush,
    "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RPOPLPUSH`
/// https://redis.io/docs/latest/commands/rpoplpush/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RpoplpushInput {
    pub(crate) source: RedisKey,
    pub(crate) destination: RedisKey,
}

impl Serialize for RpoplpushInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RpoplpushInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        state.end()
    }
}

impl_redis_operation!(RpoplpushInput, API_INFO, { source, destination });

impl RedisCommandInput for RpoplpushInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.source).arg(&self.destination);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("RPOPLPUSH requires 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            source: args[0].clone().try_into()?,
            destination: args[1].clone().try_into()?,
        })
    }
}

/// Output for Redis RPOPLPUSH command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RpoplpushOutput {
    value: Option<RedisJsonValue>,
}

impl RpoplpushOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    pub fn source_empty(&self) -> bool {
        self.value.is_none()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected RPOPLPUSH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected RPOPLPUSH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for RpoplpushOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RpoplpushOutput", 1)?;
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
            let input = RpoplpushInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$9\r\nRPOPLPUSH\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("source".into()), RedisJsonValue::String("dest".into())];
            let input = RpoplpushInput::decode(args).unwrap();
            assert_eq!(input.source, RedisKey::String("source".into()));
            assert_eq!(input.destination, RedisKey::String("dest".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("source".into())];
            let err = RpoplpushInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = RpoplpushOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.source_empty());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null() {
            let output = RpoplpushOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.source_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = RpoplpushOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = RpoplpushInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
            };
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lrange::{LrangeInput, LrangeOutput};
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpoplpush_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rplp_src".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &RpoplpushInput {
                                source: RedisKey::String("rplp_src".into()),
                                destination: RedisKey::String("rplp_dst".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpoplpushOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("b")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpoplpush_empty_source() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &RpoplpushInput {
                                source: RedisKey::String("nonexistent_rplp".into()),
                                destination: RedisKey::String("rplp_dst2".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpoplpushOutput::decode(&result).expect("decode failed");
                    assert!(output.source_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpoplpush_moves_element() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rplp_src3".into()),
                            elements: vec![RedisJsonValue::String("moved".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    ctx.raw(
                        &RpoplpushInput {
                            source: RedisKey::String("rplp_src3".into()),
                            destination: RedisKey::String("rplp_dst3".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("rpoplpush failed");

                    // Verify destination has the element
                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("rplp_dst3".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let range_output = LrangeOutput::decode(&result).expect("decode");
                    assert_eq!(range_output.elements().len(), 1);
                })
            })
            .await;
        }
    }
}
