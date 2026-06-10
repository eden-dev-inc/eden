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

const API_INFO: ApiInfo<RedisApi, LindexInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Lindex, "Returns an element from a list by its index", ReqType::Read, true);

/// See official Redis documentation for `LINDEX`
/// https://redis.io/docs/latest/commands/lindex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LindexInput {
    pub(crate) key: RedisKey,
    pub(crate) index: RedisJsonValue,
}

impl Serialize for LindexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LindexInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("index", &self.index)?;
        state.end()
    }
}

impl_redis_operation!(LindexInput, API_INFO, { key, index });

impl RedisCommandInput for LindexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.index);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("LINDEX requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, index: args[1].clone() })
    }
}

/// Output for Redis LINDEX command
///
/// Returns the element at the given index, or None if index is out of range.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LindexOutput {
    /// The element at the index, or None if index is out of range
    value: Option<RedisJsonValue>,
}

impl LindexOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the value at the index
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if the index was out of range
    pub fn out_of_range(&self) -> bool {
        self.value.is_none()
    }

    /// Decode the Redis protocol response into a LindexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LINDEX response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LINDEX response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LindexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LindexOutput", 1)?;
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
            let input = LindexInput {
                key: RedisKey::String("mylist".into()),
                index: RedisJsonValue::Integer(0),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$1\r\n0\r\n");
        }

        #[test]
        fn test_encode_command_negative_index() {
            let input = LindexInput {
                key: RedisKey::String("mylist".into()),
                index: RedisJsonValue::Integer(-1),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nLINDEX\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::Integer(5)];
            let input = LindexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = LindexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = LindexOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.out_of_range());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = LindexOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.out_of_range());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = LindexOutput::decode(b"_\r\n").unwrap();
            assert!(output.out_of_range());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LindexOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LindexInput {
                key: RedisKey::String("mylist".into()),
                index: RedisJsonValue::Integer(0),
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
        async fn test_lindex_first_element() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lindex_list".into()),
                            elements: vec![
                                RedisJsonValue::String("first".into()),
                                RedisJsonValue::String("second".into()),
                                RedisJsonValue::String("third".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LindexInput {
                                key: RedisKey::String("lindex_list".into()),
                                index: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LindexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("first")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lindex_negative_index() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lindex_neg".into()),
                            elements: vec![RedisJsonValue::String("first".into()), RedisJsonValue::String("last".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LindexInput {
                                key: RedisKey::String("lindex_neg".into()),
                                index: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LindexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("last")), "-1 should return last element");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lindex_out_of_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lindex_range".into()),
                            elements: vec![RedisJsonValue::String("only".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LindexInput {
                                key: RedisKey::String("lindex_range".into()),
                                index: RedisJsonValue::Integer(100),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LindexOutput::decode(&result).expect("decode failed");
                    assert!(output.out_of_range());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lindex_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LindexInput {
                                key: RedisKey::String("nonexistent_lindex".into()),
                                index: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LindexOutput::decode(&result).expect("decode failed");
                    assert!(output.out_of_range());
                })
            })
            .await;
        }
    }
}
