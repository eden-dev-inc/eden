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

const API_INFO: ApiInfo<RedisApi, BlmpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Blmpop,
    "Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes the list if the last element was popped",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `BLMPOP`
/// https://redis.io/docs/latest/commands/blmpop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BlmpopInput {
    pub(crate) timeout: RedisJsonValue,
    pub(crate) keys: Vec<RedisKey>,
    pub(crate) direction: Direction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
}

impl Serialize for BlmpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BlmpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("timeout", &self.timeout)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("direction", &self.direction)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    BlmpopInput,
    API_INFO,
    {timeout, keys, direction, count}
);

impl RedisCommandInput for BlmpopInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.timeout).arg(self.keys.len()).arg(&self.keys);

        match self.direction {
            Direction::Left => command.arg("LEFT"),
            Direction::Right => command.arg("RIGHT"),
        };

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("BLMPOP requires at least 4 arguments, given {}", args.len())));
        }

        let timeout = args[0].clone();

        let numkeys = match &args[1] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be an integer")),
        };

        if args.len() < 3 + numkeys {
            return Err(EpError::request("Insufficient arguments for keys and direction"));
        }

        let mut keys = vec![];
        for key in args[2..2 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        let direction = match &args[2 + numkeys] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "LEFT" => Direction::Left,
                "RIGHT" => Direction::Right,
                _ => return Err(EpError::parse("Direction must be LEFT or RIGHT")),
            },
            _ => return Err(EpError::parse("Direction must be a string")),
        };

        let count = if args.len() > 3 + numkeys {
            if let RedisJsonValue::String(cmd) = &args[3 + numkeys] {
                if cmd.to_uppercase() == "COUNT" && args.len() > 4 + numkeys {
                    Some(args[4 + numkeys].clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { timeout, keys, direction, count })
    }
}

/// Output for Redis BLMPOP command
///
/// Returns the key and elements that were popped, or None if timeout occurred.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BlmpopOutput {
    key: Option<String>,
    elements: Vec<RedisJsonValue>,
}

impl BlmpopOutput {
    pub fn new(key: Option<String>, elements: Vec<RedisJsonValue>) -> Self {
        Self { key, elements }
    }

    /// Get the key from which elements were popped
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Get the popped elements
    pub fn elements(&self) -> &[RedisJsonValue] {
        &self.elements
    }

    /// Check if timeout occurred (no elements popped)
    pub fn timed_out(&self) -> bool {
        self.key.is_none()
    }

    /// Decode the Redis protocol response into a BlmpopOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { key: None, elements: vec![] }),
                Resp2Frame::Array(arr) if arr.len() == 2 => {
                    let key = match &arr[0] {
                        Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected bulk string for key")),
                    };
                    let elements = match &arr[1] {
                        Resp2Frame::Array(elems) => {
                            elems.iter().map(|e| RedisJsonValue::try_from(e.clone())).collect::<Result<Vec<_>, _>>()?
                        }
                        _ => return Err(EpError::parse("expected array for elements")),
                    };
                    Ok(Self { key: Some(key), elements })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BLMPOP response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { key: None, elements: vec![] }),
                Resp3Frame::Array { data, .. } if data.len() == 2 => {
                    let key = match &data[0] {
                        Resp3Frame::BlobString { data: b, .. } => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected blob string for key")),
                    };
                    let elements = match &data[1] {
                        Resp3Frame::Array { data: elems, .. } => {
                            elems.iter().map(|e| RedisJsonValue::try_from(e.clone())).collect::<Result<Vec<_>, _>>()?
                        }
                        _ => return Err(EpError::parse("expected array for elements")),
                    };
                    Ok(Self { key: Some(key), elements })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BLMPOP response: {:?}", other))),
            },
        }
    }
}

impl Serialize for BlmpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BlmpopOutput", 2)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("elements", &self.elements)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_without_count() {
            let input = BlmpopInput {
                timeout: RedisJsonValue::Integer(5),
                keys: vec![RedisKey::String("list1".into())],
                direction: Direction::Left,
                count: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BLMPOP"));
            assert!(cmd_str.contains("LEFT"));
            assert!(!cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = BlmpopInput {
                timeout: RedisJsonValue::Integer(0),
                keys: vec![RedisKey::String("list1".into())],
                direction: Direction::Right,
                count: Some(RedisJsonValue::Integer(3)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BLMPOP"));
            assert!(cmd_str.contains("RIGHT"));
            assert!(cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = BlmpopInput {
                timeout: RedisJsonValue::Integer(10),
                keys: vec![RedisKey::String("list1".into()), RedisKey::String("list2".into())],
                direction: Direction::Left,
                count: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            // numkeys should be 2
            assert!(cmd_str.contains("$1\r\n2"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(5),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("LEFT".into()),
            ];
            let input = BlmpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.direction, Direction::Left);
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("RIGHT".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = BlmpopInput::decode(args).unwrap();
            assert_eq!(input.direction, Direction::Right);
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(5), RedisJsonValue::Integer(1)];
            let err = BlmpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 4 arguments"));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = BlmpopOutput::decode(b"*-1\r\n").unwrap();
            assert!(output.timed_out());
            assert!(output.key().is_none());
            assert!(output.elements().is_empty());
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = BlmpopOutput::decode(b"_\r\n").unwrap();
            assert!(output.timed_out());
        }

        #[test]
        fn test_decode_output_error() {
            let err = BlmpopOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = BlmpopInput {
                timeout: RedisJsonValue::Integer(0),
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
                direction: Direction::Left,
                count: None,
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

        // BLMPOP requires Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmpop_single_element() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("blmpop_list".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &BlmpopInput {
                                timeout: RedisJsonValue::Integer(0),
                                keys: vec![RedisKey::String("blmpop_list".into())],
                                direction: Direction::Left,
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmpopOutput::decode(&result).expect("decode failed");
                    assert!(!output.timed_out());
                    assert_eq!(output.key(), Some("blmpop_list"));
                    assert_eq!(output.elements().len(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmpop_with_count() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("blmpop_count".into()),
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
                            &BlmpopInput {
                                timeout: RedisJsonValue::Integer(0),
                                keys: vec![RedisKey::String("blmpop_count".into())],
                                direction: Direction::Left,
                                count: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmpop_timeout() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BlmpopInput {
                                timeout: RedisJsonValue::Integer(1),
                                keys: vec![RedisKey::String("nonexistent_blmpop".into())],
                                direction: Direction::Left,
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmpopOutput::decode(&result).expect("decode failed");
                    assert!(output.timed_out());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blmpop_right_direction() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("blmpop_right".into()),
                            elements: vec![RedisJsonValue::String("first".into()), RedisJsonValue::String("last".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &BlmpopInput {
                                timeout: RedisJsonValue::Integer(0),
                                keys: vec![RedisKey::String("blmpop_right".into())],
                                direction: Direction::Right,
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlmpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements().first(), Some(&RedisJsonValue::from("last")));
                })
            })
            .await;
        }
    }
}
