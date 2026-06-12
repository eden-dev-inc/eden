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

const API_INFO: ApiInfo<RedisApi, PubsubShardnumsubInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::PubsubShardnumsub,
    "Returns the count of subscribers of shard channels",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PUBSUB SHARDNUMSUB`
/// https://redis.io/docs/latest/commands/pubsub-shardnumsub/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PubsubShardnumsubInput {
    /// Shard channels to query subscriber count for. If None, returns an empty list.
    /// Use RedisJsonValue::Array for multiple channels.
    shard_channel: Option<RedisJsonValue>,
}

impl Serialize for PubsubShardnumsubInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubShardnumsubInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("shard_channel", &self.shard_channel)?;
        state.end()
    }
}

impl_redis_operation!(PubsubShardnumsubInput, API_INFO, { shard_channel });

impl RedisCommandInput for PubsubShardnumsubInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(ref channels) = self.shard_channel {
            command.arg(channels);
        }
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let shard_channel = if args.is_empty() {
            None
        } else if args.len() == 1 {
            Some(args.into_iter().next().unwrap())
        } else {
            Some(RedisJsonValue::Array(args))
        };
        Ok(Self { shard_channel })
    }
}

/// Output for Redis PUBSUB SHARDNUMSUB command
///
/// Returns a map of shard channel names to subscriber counts.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PubsubShardnumsubOutput {
    /// Map of shard channel name to subscriber count
    subscribers: HashMap<String, i64>,
}

impl PubsubShardnumsubOutput {
    pub fn new(subscribers: HashMap<String, i64>) -> Self {
        Self { subscribers }
    }

    /// Get the subscriber map
    pub fn subscribers(&self) -> &HashMap<String, i64> {
        &self.subscribers
    }

    /// Get subscriber count for a specific shard channel
    pub fn get(&self, channel: &str) -> Option<i64> {
        self.subscribers.get(channel).copied()
    }

    /// Check if any shard channels have subscribers
    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    /// Decode the Redis protocol response into a PubsubShardnumsubOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let subscribers = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB SHARDNUMSUB response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected PUBSUB SHARDNUMSUB response: {:?}", other)));
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

            let count_frame = iter.next().ok_or_else(|| EpError::parse("PUBSUB SHARDNUMSUB response missing count for channel"))?;

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

            let count_frame = iter.next().ok_or_else(|| EpError::parse("PUBSUB SHARDNUMSUB response missing count for channel"))?;

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

impl Serialize for PubsubShardnumsubOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubShardnumsubOutput", 1)?;
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
            let input = PubsubShardnumsubInput { shard_channel: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$11\r\nSHARDNUMSUB\r\n");
        }

        #[test]
        fn test_encode_command_single_channel() {
            let input = PubsubShardnumsubInput {
                shard_channel: Some(RedisJsonValue::String("shardch".into())),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nPUBSUB\r\n$11\r\nSHARDNUMSUB\r\n$7\r\nshardch\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = PubsubShardnumsubInput {
                shard_channel: Some(RedisJsonValue::Array(vec![
                    RedisJsonValue::String("sch1".into()),
                    RedisJsonValue::String("sch2".into()),
                ])),
            };
            assert_eq!(
                input.command().to_vec(),
                b"*4\r\n$6\r\nPUBSUB\r\n$11\r\nSHARDNUMSUB\r\n$4\r\nsch1\r\n$4\r\nsch2\r\n"
            );
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PubsubShardnumsubInput::decode(args).unwrap();
            assert!(input.shard_channel.is_none());
        }

        #[test]
        fn test_decode_input_with_channels() {
            let args = vec![RedisJsonValue::String("sch1".into()), RedisJsonValue::String("sch2".into())];
            let input = PubsubShardnumsubInput::decode(args).unwrap();
            assert!(input.shard_channel.is_some());
            assert!(matches!(input.shard_channel, Some(RedisJsonValue::Array(_))));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = PubsubShardnumsubOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_with_data() {
            let bytes = b"*4\r\n$4\r\nsch1\r\n:5\r\n$4\r\nsch2\r\n:10\r\n";
            let output = PubsubShardnumsubOutput::decode(bytes).unwrap();
            assert_eq!(output.get("sch1"), Some(5));
            assert_eq!(output.get("sch2"), Some(10));
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PubsubShardnumsubOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PubsubShardnumsubInput { shard_channel: Some(RedisJsonValue::String("sch".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PubsubShardnumsubInput::default();
            assert!(input.shard_channel.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: PUBSUB SHARDNUMSUB requires Redis 7.0+

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardnumsub_no_channels() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PubsubShardnumsubInput { shard_channel: None }.command()).await.expect("raw failed");

                    let output = PubsubShardnumsubOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardnumsub_with_channels() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PubsubShardnumsubInput {
                                shard_channel: Some(RedisJsonValue::Array(vec![
                                    RedisJsonValue::String("testsch1".into()),
                                    RedisJsonValue::String("testsch2".into()),
                                ])),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PubsubShardnumsubOutput::decode(&result).expect("decode failed");
                    // No subscribers yet, but channels should be in response
                    assert_eq!(output.get("testsch1"), Some(0));
                    assert_eq!(output.get("testsch2"), Some(0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardnumsub_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(&PubsubShardnumsubInput { shard_channel: Some(RedisJsonValue::String("r2sch".into())) }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = PubsubShardnumsubOutput::decode(&result).expect("decode failed");
            assert_eq!(output.get("r2sch"), Some(0));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardnumsub_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(&PubsubShardnumsubInput { shard_channel: Some(RedisJsonValue::String("r3sch".into())) }.command())
                .await
                .expect("raw failed");

            let output = PubsubShardnumsubOutput::decode(&result).expect("decode failed");
            assert_eq!(output.get("r3sch"), Some(0));

            ctx.stop().await;
        }
    }
}
