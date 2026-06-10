use crate::api::lib::list::Direction;
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

const API_INFO: ApiInfo<RedisApi, LmoveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lmove,
    "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LMOVE`
/// https://redis.io/docs/latest/commands/lmove/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LmoveInput {
    pub(crate) source: RedisKey,
    pub(crate) destination: RedisKey,
    pub(crate) wherefrom: Direction,
    pub(crate) whereto: Direction,
}

impl Serialize for LmoveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LmoveInput", 5)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("wherefrom", &self.wherefrom)?;
        state.serialize_field("whereto", &self.whereto)?;
        state.end()
    }
}

impl_redis_operation!(LmoveInput, API_INFO, { source, destination, wherefrom, whereto });

impl RedisCommandInput for LmoveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.source).arg(&self.destination);

        match self.wherefrom {
            Direction::Left => command.arg("LEFT"),
            Direction::Right => command.arg("RIGHT"),
        };

        match self.whereto {
            Direction::Left => command.arg("LEFT"),
            Direction::Right => command.arg("RIGHT"),
        };

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 4 {
            return Err(EpError::request(format!("LMOVE requires 4 arguments, given {}", args.len())));
        }

        Ok(Self {
            source: args[0].clone().try_into()?,
            destination: args[1].clone().try_into()?,
            wherefrom: Direction::try_from(args[2].clone())?,
            whereto: Direction::try_from(args[3].clone())?,
        })
    }
}

/// Output for Redis LMOVE command
///
/// Returns the element that was moved, or None if source list is empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LmoveOutput {
    /// The element that was moved
    value: Option<RedisJsonValue>,
}

impl LmoveOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the moved element
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if source was empty (no element moved)
    pub fn source_empty(&self) -> bool {
        self.value.is_none()
    }

    /// Decode the Redis protocol response into a LmoveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LMOVE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LMOVE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LmoveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LmoveOutput", 1)?;
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
        fn test_encode_command_left_right() {
            let input = LmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LMOVE"));
            assert!(cmd_str.contains("LEFT"));
            assert!(cmd_str.contains("RIGHT"));
        }

        #[test]
        fn test_encode_command_right_left() {
            let input = LmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Right,
                whereto: Direction::Left,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$5\r\nLMOVE\r\n"));
        }

        #[test]
        fn test_encode_command_left_left() {
            let input = LmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Left,
                whereto: Direction::Left,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert_eq!(cmd_str.matches("LEFT").count(), 2);
        }

        #[test]
        fn test_encode_command_right_right() {
            let input = LmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Right,
                whereto: Direction::Right,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert_eq!(cmd_str.matches("RIGHT").count(), 2);
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("LEFT".into()),
                RedisJsonValue::String("RIGHT".into()),
            ];
            let input = LmoveInput::decode(args).unwrap();
            assert_eq!(input.wherefrom, Direction::Left);
            assert_eq!(input.whereto, Direction::Right);
        }

        #[test]
        fn test_decode_input_case_insensitive() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("left".into()),
                RedisJsonValue::String("right".into()),
            ];
            let input = LmoveInput::decode(args).unwrap();
            assert_eq!(input.wherefrom, Direction::Left);
            assert_eq!(input.whereto, Direction::Right);
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("source".into()), RedisJsonValue::String("dest".into())];
            let err = LmoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("4 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_direction() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("INVALID".into()),
                RedisJsonValue::String("RIGHT".into()),
            ];
            let err = LmoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("LEFT or RIGHT"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = LmoveOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.source_empty());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = LmoveOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.source_empty());
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = LmoveOutput::decode(b"_\r\n").unwrap();
            assert!(output.source_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LmoveOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
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

        // LMOVE requires Redis 6.2+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lmove_right_left() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lmove_src".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LmoveInput {
                                source: RedisKey::String("lmove_src".into()),
                                destination: RedisKey::String("lmove_dst".into()),
                                wherefrom: Direction::Right,
                                whereto: Direction::Left,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LmoveOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("b")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lmove_left_right() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lmove_src2".into()),
                            elements: vec![RedisJsonValue::String("first".into()), RedisJsonValue::String("second".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LmoveInput {
                                source: RedisKey::String("lmove_src2".into()),
                                destination: RedisKey::String("lmove_dst2".into()),
                                wherefrom: Direction::Left,
                                whereto: Direction::Right,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LmoveOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("first")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lmove_empty_source() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LmoveInput {
                                source: RedisKey::String("nonexistent_lmove".into()),
                                destination: RedisKey::String("lmove_dst3".into()),
                                wherefrom: Direction::Left,
                                whereto: Direction::Left,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LmoveOutput::decode(&result).expect("decode failed");
                    assert!(output.source_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lmove_same_list_rotate() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lmove_rotate".into()),
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

                    // Rotate: pop from right, push to left
                    ctx.raw(
                        &LmoveInput {
                            source: RedisKey::String("lmove_rotate".into()),
                            destination: RedisKey::String("lmove_rotate".into()),
                            wherefrom: Direction::Right,
                            whereto: Direction::Left,
                        }
                        .command(),
                    )
                    .await
                    .expect("lmove failed");

                    // Check the result - list should now be [c, a, b]
                    let range_result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("lmove_rotate".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let range_output = LrangeOutput::decode(&range_result).expect("decode");
                    assert_eq!(range_output.elements().len(), 3);
                })
            })
            .await;
        }
    }
}
