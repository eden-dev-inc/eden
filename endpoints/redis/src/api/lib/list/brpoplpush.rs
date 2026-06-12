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

const API_INFO: ApiInfo<RedisApi, BrpoplpushInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Brpoplpush,
    "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was popped.",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `BRPOPLPUSH`
/// https://redis.io/docs/latest/commands/brpoplpush/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BrpoplpushInput {
    pub(crate) source: RedisKey,
    pub(crate) destination: RedisKey,
    pub(crate) timeout: RedisJsonValue,
}

impl Serialize for BrpoplpushInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BrpoplpushInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(BrpoplpushInput, API_INFO, { source, destination, timeout });

impl RedisCommandInput for BrpoplpushInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.source).arg(&self.destination).arg(&self.timeout);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("BRPOPLPUSH requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            source: args[0].clone().try_into()?,
            destination: args[1].clone().try_into()?,
            timeout: args[2].clone(),
        })
    }
}

/// Output for Redis BRPOPLPUSH command
///
/// Returns the element that was popped and pushed, or None if timeout occurred.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BrpoplpushOutput {
    /// The value that was popped and pushed
    value: Option<RedisJsonValue>,
}

impl BrpoplpushOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the popped/pushed value
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if timeout occurred (no element moved)
    pub fn timed_out(&self) -> bool {
        self.value.is_none()
    }

    /// Decode the Redis protocol response into a BrpoplpushOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BRPOPLPUSH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BRPOPLPUSH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for BrpoplpushOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BrpoplpushOutput", 1)?;
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
            let input = BrpoplpushInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                timeout: RedisJsonValue::Integer(5),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$10\r\nBRPOPLPUSH\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = BrpoplpushInput::decode(args).unwrap();
            assert_eq!(input.source, RedisKey::String("source".into()));
            assert_eq!(input.destination, RedisKey::String("dest".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("source".into()), RedisJsonValue::String("dest".into())];
            let err = BrpoplpushInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = BrpoplpushOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.timed_out());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = BrpoplpushOutput::decode(b"_\r\n").unwrap();
            assert!(output.timed_out());
        }

        #[test]
        fn test_decode_output_value() {
            let output = BrpoplpushOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.timed_out());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_error() {
            let err = BrpoplpushOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = BrpoplpushInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                timeout: RedisJsonValue::Integer(0),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lrange::LrangeInput;
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_brpoplpush_with_data() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Push data to source
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("brpoplpush_src".into()),
                            elements: vec![RedisJsonValue::String("elem1".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &BrpoplpushInput {
                                source: RedisKey::String("brpoplpush_src".into()),
                                destination: RedisKey::String("brpoplpush_dst".into()),
                                timeout: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BrpoplpushOutput::decode(&result).expect("decode failed");
                    assert!(!output.timed_out());
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("elem1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_brpoplpush_timeout() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BrpoplpushInput {
                                source: RedisKey::String("nonexistent_src".into()),
                                destination: RedisKey::String("nonexistent_dst".into()),
                                timeout: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BrpoplpushOutput::decode(&result).expect("decode failed");
                    assert!(output.timed_out(), "should timeout on empty source");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_brpoplpush_moves_to_destination() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("brpl_src2".into()),
                            elements: vec![RedisJsonValue::String("moved".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    ctx.raw(
                        &BrpoplpushInput {
                            source: RedisKey::String("brpl_src2".into()),
                            destination: RedisKey::String("brpl_dst2".into()),
                            timeout: RedisJsonValue::Integer(0),
                        }
                        .command(),
                    )
                    .await
                    .expect("brpoplpush failed");

                    // Verify destination has the element
                    let result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("brpl_dst2".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let lrange_output = crate::api::lib::list::lrange::LrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(lrange_output.elements().len(), 1);
                })
            })
            .await;
        }
    }
}
