use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, PubsubShardchannelsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::PubsubShardchannels,
    "Returns the active shard channels",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PUBSUB SHARDCHANNELS`
/// https://redis.io/docs/latest/commands/pubsub-shardchannels/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PubsubShardchannelsInput {
    /// Optional pattern to filter shard channels. Supports glob-style patterns.
    pattern: Option<RedisJsonValue>,
}

impl Serialize for PubsubShardchannelsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubShardchannelsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("pattern", &self.pattern)?;
        state.end()
    }
}

impl_redis_operation!(PubsubShardchannelsInput, API_INFO, { pattern });

impl RedisCommandInput for PubsubShardchannelsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(ref pattern) = self.pattern {
            command.arg(pattern);
        }
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let pattern = args.first().cloned();
        Ok(Self { pattern })
    }
}

/// Output for Redis PUBSUB SHARDCHANNELS command
///
/// Returns a list of active shard channels matching the pattern (if provided).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PubsubShardchannelsOutput {
    /// List of active shard channel names
    channels: Vec<String>,
}

impl PubsubShardchannelsOutput {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// Get the list of active shard channels
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Get the number of active shard channels
    pub fn count(&self) -> usize {
        self.channels.len()
    }

    /// Check if any shard channels are active
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Decode the Redis protocol response into a PubsubShardchannelsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let channels = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB SHARDCHANNELS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB SHARDCHANNELS response: {:?}", other)));
                }
            },
        };

        Ok(Self { channels })
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Vec<String>, EpError> {
        items
            .into_iter()
            .map(|item| match item {
                Resp2Frame::BulkString(b) => String::from_utf8(b).map_err(EpError::parse),
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse),
                other => Err(EpError::parse(format!("unexpected channel type: {:?}", other))),
            })
            .collect()
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Vec<String>, EpError> {
        items
            .into_iter()
            .map(|item| match item {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse),
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse),
                other => Err(EpError::parse(format!("unexpected channel type: {:?}", other))),
            })
            .collect()
    }
}

impl Serialize for PubsubShardchannelsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubShardchannelsOutput", 1)?;
        state.serialize_field("channels", &self.channels)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_pattern() {
            let input = PubsubShardchannelsInput { pattern: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$13\r\nSHARDCHANNELS\r\n");
        }

        #[test]
        fn test_encode_command_with_pattern() {
            let input = PubsubShardchannelsInput { pattern: Some(RedisJsonValue::String("shard*".into())) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nPUBSUB\r\n$13\r\nSHARDCHANNELS\r\n$6\r\nshard*\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PubsubShardchannelsInput::decode(args).unwrap();
            assert!(input.pattern.is_none());
        }

        #[test]
        fn test_decode_input_with_pattern() {
            let args = vec![RedisJsonValue::String("test*".into())];
            let input = PubsubShardchannelsInput::decode(args).unwrap();
            assert!(input.pattern.is_some());
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = PubsubShardchannelsOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_with_channels() {
            let bytes = b"*2\r\n$5\r\nsch_1\r\n$5\r\nsch_2\r\n";
            let output = PubsubShardchannelsOutput::decode(bytes).unwrap();
            assert_eq!(output.count(), 2);
            assert_eq!(output.channels(), &["sch_1", "sch_2"]);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PubsubShardchannelsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PubsubShardchannelsInput { pattern: Some(RedisJsonValue::String("*".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PubsubShardchannelsInput::default();
            assert!(input.pattern.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: PUBSUB SHARDCHANNELS requires Redis 7.0+

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardchannels_empty() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PubsubShardchannelsInput { pattern: None }.command()).await.expect("raw failed");

                    let output = PubsubShardchannelsOutput::decode(&result).expect("decode failed");
                    // No active shard subscriptions yet
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardchannels_with_pattern() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&PubsubShardchannelsInput { pattern: Some(RedisJsonValue::String("test*".into())) }.command())
                        .await
                        .expect("raw failed");

                    let output = PubsubShardchannelsOutput::decode(&result).expect("decode failed");
                    // Should return empty or matching channels
                    assert!(output.channels().iter().all(|c| c.starts_with("test")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardchannels_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&PubsubShardchannelsInput { pattern: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let _output = PubsubShardchannelsOutput::decode(&result).expect("decode failed");
            // assert!(output.channels().len() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_shardchannels_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&PubsubShardchannelsInput { pattern: None }.command()).await.expect("raw failed");

            let _output = PubsubShardchannelsOutput::decode(&result).expect("decode failed");
            // assert!(output.channels().len() >= 0);

            ctx.stop().await;
        }
    }
}
