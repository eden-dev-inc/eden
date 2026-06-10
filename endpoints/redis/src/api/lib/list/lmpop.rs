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

const API_INFO: ApiInfo<RedisApi, LmpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lmpop,
    "Returns multiple elements from a list, removing them. Deletes the list if the last element was popped",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LMPOP`
/// https://redis.io/docs/latest/commands/lmpop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LmpopInput {
    pub(crate) keys: Vec<RedisKey>,
    pub(crate) direction: Direction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
}

impl Serialize for LmpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("LmpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("direction", &self.direction)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(LmpopInput, API_INFO, { keys, direction, count });

impl RedisCommandInput for LmpopInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(self.keys.len()).arg(&self.keys);

        match &self.direction {
            Direction::Right => command.arg("RIGHT"),
            Direction::Left => command.arg("LEFT"),
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
        if args.len() < 3 {
            return Err(EpError::request(format!("LMPOP requires at least 3 arguments, given {}", args.len())));
        }

        let numkeys = match &args[0] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be an integer")),
        };

        if args.len() < 2 + numkeys {
            return Err(EpError::request("Insufficient arguments"));
        }

        let mut keys = vec![];
        for key in args[1..1 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        let direction = match &args[1 + numkeys] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "LEFT" => Direction::Left,
                "RIGHT" => Direction::Right,
                _ => return Err(EpError::parse("Direction must be LEFT or RIGHT")),
            },
            _ => return Err(EpError::parse("Direction must be a string")),
        };

        let count = if args.len() > 2 + numkeys {
            if let RedisJsonValue::String(cmd) = &args[2 + numkeys] {
                if cmd.to_uppercase() == "COUNT" && args.len() > 3 + numkeys {
                    Some(args[3 + numkeys].clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { keys, direction, count })
    }
}

/// Output for Redis LMPOP command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LmpopOutput {
    key: Option<String>,
    elements: Vec<RedisJsonValue>,
}

impl LmpopOutput {
    pub fn new(key: Option<String>, elements: Vec<RedisJsonValue>) -> Self {
        Self { key, elements }
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn elements(&self) -> &[RedisJsonValue] {
        &self.elements
    }

    pub fn all_empty(&self) -> bool {
        self.key.is_none()
    }

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
                other => Err(EpError::parse(format!("unexpected LMPOP response: {:?}", other))),
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
                other => Err(EpError::parse(format!("unexpected LMPOP response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LmpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LmpopOutput", 2)?;
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
            let input = LmpopInput {
                keys: vec![RedisKey::String("list1".into())],
                direction: Direction::Left,
                count: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LMPOP"));
            assert!(!cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = LmpopInput {
                keys: vec![RedisKey::String("list1".into())],
                direction: Direction::Right,
                count: Some(RedisJsonValue::Integer(5)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("LEFT".into()),
            ];
            let input = LmpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.direction, Direction::Left);
        }

        #[test]
        fn test_decode_output_null() {
            let output = LmpopOutput::decode(b"*-1\r\n").unwrap();
            assert!(output.all_empty());
        }

        #[test]
        fn test_keys_accessor() {
            let input = LmpopInput {
                keys: vec![RedisKey::String("a".into())],
                direction: Direction::Left,
                count: None,
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
        async fn test_lmpop_single_element() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lmpop_list".into()),
                            elements: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LmpopInput {
                                keys: vec![RedisKey::String("lmpop_list".into())],
                                direction: Direction::Left,
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LmpopOutput::decode(&result).expect("decode failed");
                    assert!(!output.all_empty());
                })
            })
            .await;
        }
    }
}
