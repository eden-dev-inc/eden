use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use redis_protocol::resp3::types::FrameMap;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, PubsubNumsubInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::PubsubNumsub,
    "Returns a count of subscribers to channels",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PUBSUB NUMSUB`
/// https://redis.io/docs/latest/commands/pubsub-numsub/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PubsubNumsubInput {
    /// Channels to query subscriber count for. If None, returns an empty list.
    /// Use RedisJsonValue::Array for multiple channels.
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<RedisJsonValue>,
}

impl Serialize for PubsubNumsubInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubNumsubInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("channel", &self.channel)?;
        state.end()
    }
}

impl_redis_operation!(PubsubNumsubInput, API_INFO, { channel });

impl RedisCommandInput for PubsubNumsubInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(ref channels) = self.channel {
            command.arg(channels);
        }
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let channel = if args.is_empty() {
            None
        } else if args.len() == 1 {
            Some(args.into_iter().next().unwrap())
        } else {
            Some(RedisJsonValue::Array(args))
        };
        Ok(Self { channel })
    }
}

/// Output for Redis PUBSUB NUMSUB command
///
/// Returns a map of channel names to subscriber counts.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PubsubNumsubOutput {
    /// Map of channel name to subscriber count
    subscribers: HashMap<String, i64>,
}

impl PubsubNumsubOutput {
    pub fn new(subscribers: HashMap<String, i64>) -> Self {
        Self { subscribers }
    }

    /// Get the subscriber map
    pub fn subscribers(&self) -> &HashMap<String, i64> {
        &self.subscribers
    }

    /// Get subscriber count for a specific channel
    pub fn get(&self, channel: &str) -> Option<i64> {
        self.subscribers.get(channel).copied()
    }

    /// Check if any channels have subscribers
    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    /// Decode the Redis protocol response into a PubsubNumsubOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let subscribers = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB NUMSUB response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data)?,
                Resp3Frame::Map { data, .. } => Self::decode_map_resp3(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB NUMSUB response: {:?}", other)));
                }
            },
        };

        Ok(Self { subscribers })
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<HashMap<String, i64>, EpError> {
        let mut subscribers = HashMap::new();
        let mut iter = items.into_iter();

        while let Some(channel_frame) = iter.next() {
            let channel = match channel_frame {
                Resp2Frame::BulkString(b) => String::from_utf8(b).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                other => {
                    return Err(EpError::parse(format!("unexpected channel type: {:?}", other)));
                }
            };

            let count_frame = iter.next().ok_or_else(|| EpError::parse("PUBSUB NUMSUB response missing count for channel"))?;

            let count = match count_frame {
                Resp2Frame::Integer(n) => n,
                other => {
                    return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
                }
            };

            subscribers.insert(channel, count);
        }

        Ok(subscribers)
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<HashMap<String, i64>, EpError> {
        let mut subscribers = HashMap::new();
        let mut iter = items.into_iter();

        while let Some(channel_frame) = iter.next() {
            let channel = match channel_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                other => {
                    return Err(EpError::parse(format!("unexpected channel type: {:?}", other)));
                }
            };

            let count_frame = iter.next().ok_or_else(|| EpError::parse("PUBSUB NUMSUB response missing count for channel"))?;

            let count = match count_frame {
                Resp3Frame::Number { data, .. } => data,
                other => {
                    return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
                }
            };

            subscribers.insert(channel, count);
        }

        Ok(subscribers)
    }

    fn decode_map_resp3(items: FrameMap<Resp3Frame, Resp3Frame>) -> Result<HashMap<String, i64>, EpError> {
        let mut subscribers = HashMap::new();

        for (key_frame, value_frame) in items {
            let channel = match key_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                other => {
                    return Err(EpError::parse(format!("unexpected channel type: {:?}", other)));
                }
            };

            let count = match value_frame {
                Resp3Frame::Number { data, .. } => data,
                other => {
                    return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
                }
            };

            subscribers.insert(channel, count);
        }

        Ok(subscribers)
    }
}

impl Serialize for PubsubNumsubOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubNumsubOutput", 1)?;
        state.serialize_field("subscribers", &self.subscribers)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_channels() {
            let input = PubsubNumsubInput { channel: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n");
        }

        #[test]
        fn test_encode_command_single_channel() {
            let input = PubsubNumsubInput { channel: Some(RedisJsonValue::String("mychannel".into())) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$9\r\nmychannel\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = PubsubNumsubInput {
                channel: Some(RedisJsonValue::Array(vec![
                    RedisJsonValue::String("ch1".into()),
                    RedisJsonValue::String("ch2".into()),
                ])),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$3\r\nch1\r\n$3\r\nch2\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PubsubNumsubInput::decode(args).unwrap();
            assert!(input.channel.is_none());
        }

        #[test]
        fn test_decode_input_with_channels() {
            let args = vec![RedisJsonValue::String("ch1".into()), RedisJsonValue::String("ch2".into())];
            let input = PubsubNumsubInput::decode(args).unwrap();
            assert!(input.channel.is_some());
            assert!(matches!(input.channel, Some(RedisJsonValue::Array(_))));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = PubsubNumsubOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_with_data() {
            // *4\r\n$3\r\nch1\r\n:5\r\n$3\r\nch2\r\n:10\r\n
            let bytes = b"*4\r\n$3\r\nch1\r\n:5\r\n$3\r\nch2\r\n:10\r\n";
            let output = PubsubNumsubOutput::decode(bytes).unwrap();
            assert_eq!(output.get("ch1"), Some(5));
            assert_eq!(output.get("ch2"), Some(10));
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PubsubNumsubOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PubsubNumsubInput { channel: Some(RedisJsonValue::String("ch".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PubsubNumsubInput::default();
            assert!(input.channel.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numsub_no_channels() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PubsubNumsubInput { channel: None }.command()).await.expect("raw failed");

                    let output = PubsubNumsubOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numsub_with_channels() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PubsubNumsubInput {
                                channel: Some(RedisJsonValue::Array(vec![
                                    RedisJsonValue::String("testch1".into()),
                                    RedisJsonValue::String("testch2".into()),
                                ])),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PubsubNumsubOutput::decode(&result).expect("decode failed");
                    // No subscribers yet, but channels should be in response
                    assert_eq!(output.get("testch1"), Some(0));
                    assert_eq!(output.get("testch2"), Some(0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numsub_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(&PubsubNumsubInput { channel: Some(RedisJsonValue::String("r2ch".into())) }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = PubsubNumsubOutput::decode(&result).expect("decode failed");
            assert_eq!(output.get("r2ch"), Some(0));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numsub_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(&PubsubNumsubInput { channel: Some(RedisJsonValue::String("r3ch".into())) }.command())
                .await
                .expect("raw failed");

            let output = PubsubNumsubOutput::decode(&result).expect("decode failed");
            assert_eq!(output.get("r3ch"), Some(0));

            ctx.stop().await;
        }
    }
}
