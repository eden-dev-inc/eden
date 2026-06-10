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

const API_INFO: ApiInfo<RedisApi, BlpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Blpop,
    "Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `BLPOP`
/// https://redis.io/docs/latest/commands/blpop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BlpopInput {
    pub(crate) keys: Vec<RedisKey>,
    pub(crate) timeout: RedisJsonValue,
}

impl Serialize for BlpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BlpopInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(BlpopInput, API_INFO, { keys, timeout });

impl RedisCommandInput for BlpopInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.keys).arg(&self.timeout);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("BLPOP requires at least 2 arguments, given {}", args.len())));
        }

        let mut keys = vec![];
        for key in args[..args.len() - 1].iter() {
            keys.push(key.try_into()?);
        }

        let timeout = args[args.len() - 1].clone();

        Ok(Self { keys, timeout })
    }
}

/// Output for Redis BLPOP command
///
/// Returns the key and value that was popped, or None if timeout occurred.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BlpopOutput {
    /// The key from which the element was popped
    key: Option<String>,
    /// The value that was popped
    value: Option<RedisJsonValue>,
}

impl BlpopOutput {
    pub fn new(key: Option<String>, value: Option<RedisJsonValue>) -> Self {
        Self { key, value }
    }

    /// Get the key from which element was popped
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Get the popped value
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if timeout occurred (no element popped)
    pub fn timed_out(&self) -> bool {
        self.key.is_none()
    }

    /// Decode the Redis protocol response into a BlpopOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { key: None, value: None }),
                Resp2Frame::Array(arr) if arr.len() == 2 => {
                    let key = match &arr[0] {
                        Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected bulk string for key")),
                    };
                    let value = RedisJsonValue::try_from(arr[1].clone())?;
                    Ok(Self { key: Some(key), value: Some(value) })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BLPOP response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { key: None, value: None }),
                Resp3Frame::Array { data, .. } if data.len() == 2 => {
                    let key = match &data[0] {
                        Resp3Frame::BlobString { data: b, .. } => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected blob string for key")),
                    };
                    let value = RedisJsonValue::try_from(data[1].clone())?;
                    Ok(Self { key: Some(key), value: Some(value) })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BLPOP response: {:?}", other))),
            },
        }
    }
}

impl Serialize for BlpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BlpopOutput", 2)?;
        state.serialize_field("key", &self.key)?;
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
        fn test_encode_command_single_key() {
            let input = BlpopInput {
                keys: vec![RedisKey::String("mylist".into())],
                timeout: RedisJsonValue::Integer(0),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$5\r\nBLPOP\r\n"));
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = BlpopInput {
                keys: vec![RedisKey::String("list1".into()), RedisKey::String("list2".into())],
                timeout: RedisJsonValue::Integer(5),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nBLPOP\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::Integer(0)];
            let input = BlpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![
                RedisJsonValue::String("list1".into()),
                RedisJsonValue::String("list2".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = BlpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let err = BlpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = BlpopOutput::decode(b"*-1\r\n").unwrap();
            assert!(output.timed_out());
            assert_eq!(output.key(), None);
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = BlpopOutput::decode(b"_\r\n").unwrap();
            assert!(output.timed_out());
        }

        #[test]
        fn test_decode_output_array() {
            // *2\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n
            let output = BlpopOutput::decode(b"*2\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n").unwrap();
            assert!(!output.timed_out());
            assert_eq!(output.key(), Some("mylist"));
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_error() {
            let err = BlpopOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = BlpopInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
                timeout: RedisJsonValue::Integer(0),
            };
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lpush::LpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blpop_with_data() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Push data first
                    ctx.raw(
                        &LpushInput {
                            key: RedisKey::String("blpoplist".into()),
                            elements: vec![RedisJsonValue::String("elem1".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("lpush failed");

                    let result = ctx
                        .raw(
                            &BlpopInput {
                                keys: vec![RedisKey::String("blpoplist".into())],
                                timeout: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlpopOutput::decode(&result).expect("decode failed");
                    assert!(!output.timed_out());
                    assert_eq!(output.key(), Some("blpoplist"));
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("elem1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blpop_timeout() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BlpopInput {
                                keys: vec![RedisKey::String("nonexistent_blpop".into())],
                                timeout: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlpopOutput::decode(&result).expect("decode failed");
                    assert!(output.timed_out(), "should timeout on empty list");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_blpop_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Push to second list only
                    ctx.raw(
                        &LpushInput {
                            key: RedisKey::String("blpop_second".into()),
                            elements: vec![RedisJsonValue::String("fromSecond".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("lpush failed");

                    let result = ctx
                        .raw(
                            &BlpopInput {
                                keys: vec![RedisKey::String("blpop_first".into()), RedisKey::String("blpop_second".into())],
                                timeout: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BlpopOutput::decode(&result).expect("decode failed");
                    assert!(!output.timed_out());
                    assert_eq!(output.key(), Some("blpop_second"));
                })
            })
            .await;
        }
    }
}
