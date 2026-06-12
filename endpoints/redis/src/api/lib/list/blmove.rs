use crate::api::lib::list::Direction;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, BlmoveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Blmove,
    "Pops an element from a list, pushing it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was moved",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `BLMOVE`
/// https://redis.io/docs/latest/commands/blmove/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BlmoveInput {
    pub(crate) source: RedisKey,
    pub(crate) destination: RedisKey,
    pub(crate) wherefrom: Direction,
    pub(crate) whereto: Direction,
    pub(crate) timeout: RedisJsonValue,
}

impl Serialize for BlmoveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BlmoveInput", 6)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("wherefrom", &self.wherefrom)?;
        state.serialize_field("whereto", &self.whereto)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(
    BlmoveInput,
    API_INFO,
    {source, destination, wherefrom, whereto, timeout}
);

impl RedisCommandInput for BlmoveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.source).arg(&self.destination);

        match &self.wherefrom {
            Direction::Left => command.arg("LEFT"),
            Direction::Right => command.arg("RIGHT"),
        };

        match &self.whereto {
            Direction::Left => command.arg("LEFT"),
            Direction::Right => command.arg("RIGHT"),
        };

        command.arg(&self.timeout);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request(format!("BLMOVE requires 5 arguments, given {}", args.len(),)));
        }
        if args.len() > 5 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BLMOVE expects 5 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            source: args[0].clone().try_into()?,
            destination: args[1].clone().try_into()?,
            wherefrom: Direction::try_from(args[2].clone())?,
            whereto: Direction::try_from(args[3].clone())?,
            timeout: args[4].clone(),
        })
    }
}

/// Output for Redis BLMOVE command
///
/// Returns the element that was moved, or None if timeout occurred.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BlmoveOutput {
    value: Option<RedisJsonValue>,
}

impl BlmoveOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the moved element
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if timeout occurred (no element moved)
    pub fn timed_out(&self) -> bool {
        self.value.is_none()
    }

    /// Decode the Redis protocol response into a BlmoveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BLMOVE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BLMOVE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for BlmoveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BlmoveOutput", 1)?;
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
            let input = BlmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Right,
                whereto: Direction::Left,
                timeout: RedisJsonValue::Integer(0),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BLMOVE"));
            assert!(cmd_str.contains("RIGHT"));
            assert!(cmd_str.contains("LEFT"));
        }

        #[test]
        fn test_encode_command_all_directions() {
            for (from, to) in [
                (Direction::Left, Direction::Left),
                (Direction::Left, Direction::Right),
                (Direction::Right, Direction::Left),
                (Direction::Right, Direction::Right),
            ] {
                let input = BlmoveInput {
                    source: RedisKey::String("src".into()),
                    destination: RedisKey::String("dst".into()),
                    wherefrom: from,
                    whereto: to,
                    timeout: RedisJsonValue::Integer(5),
                };
                let cmd = input.command();
                assert!(cmd.starts_with(b"*6\r\n$6\r\nBLMOVE\r\n"));
            }
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("LEFT".into()),
                RedisJsonValue::String("RIGHT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = BlmoveInput::decode(args).unwrap();
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
                RedisJsonValue::Integer(0),
            ];
            let input = BlmoveInput::decode(args).unwrap();
            assert_eq!(input.wherefrom, Direction::Left);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("source".into()), RedisJsonValue::String("dest".into())];
            let err = BlmoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("5 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_direction() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("INVALID".into()),
                RedisJsonValue::String("RIGHT".into()),
                RedisJsonValue::Integer(0),
            ];
            let err = BlmoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("LEFT or RIGHT"));
        }

        #[test]
        fn test_decode_output_value() {
            let output = BlmoveOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.timed_out());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = BlmoveOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.timed_out());
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = BlmoveOutput::decode(b"_\r\n").unwrap();
            assert!(output.timed_out());
        }

        #[test]
        fn test_decode_output_error() {
            let err = BlmoveOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = BlmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
                timeout: RedisJsonValue::Integer(0),
            };
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        // BLMOVE requires Redis 6.2+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmove_with_data() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("blmove_src".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &BlmoveInput {
                                source: RedisKey::String("blmove_src".into()),
                                destination: RedisKey::String("blmove_dst".into()),
                                wherefrom: Direction::Right,
                                whereto: Direction::Left,
                                timeout: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmoveOutput::decode(&result).expect("decode failed");
                    assert!(!output.timed_out());
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("b")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmove_timeout() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BlmoveInput {
                                source: RedisKey::String("nonexistent_blmove".into()),
                                destination: RedisKey::String("blmove_dst2".into()),
                                wherefrom: Direction::Left,
                                whereto: Direction::Right,
                                timeout: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmoveOutput::decode(&result).expect("decode failed");
                    assert!(output.timed_out());
                })
            })
            .await;
        }
    }
}
