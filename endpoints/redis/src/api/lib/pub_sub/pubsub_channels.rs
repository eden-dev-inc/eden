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

const API_INFO: ApiInfo<RedisApi, PubsubChannelsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::PubsubChannels, "Returns the active channels", ReqType::Read, true);

/// See official Redis documentation for `PUBSUB CHANNELS`
/// https://redis.io/docs/latest/commands/pubsub-channels/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PubsubChannelsInput {
    /// Optional pattern to filter channels. Supports glob-style patterns.
    pattern: Option<RedisJsonValue>,
}

impl Serialize for PubsubChannelsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubChannelsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("pattern", &self.pattern)?;
        state.end()
    }
}

impl_redis_operation!(PubsubChannelsInput, API_INFO, { pattern });

impl RedisCommandInput for PubsubChannelsInput {
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

/// Output for Redis PUBSUB CHANNELS command
///
/// Returns a list of active channels matching the pattern (if provided).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PubsubChannelsOutput {
    /// List of active channel names
    channels: Vec<String>,
}

impl PubsubChannelsOutput {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// Get the list of active channels
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Get the number of active channels
    pub fn count(&self) -> usize {
        self.channels.len()
    }

    /// Check if any channels are active
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Decode the Redis protocol response into a PubsubChannelsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let channels = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB CHANNELS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB CHANNELS response: {:?}", other)));
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

impl Serialize for PubsubChannelsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubChannelsOutput", 1)?;
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
            let input = PubsubChannelsInput { pattern: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n");
        }

        #[test]
        fn test_encode_command_with_pattern() {
            let input = PubsubChannelsInput { pattern: Some(RedisJsonValue::String("news.*".into())) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n$6\r\nnews.*\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PubsubChannelsInput::decode(args).unwrap();
            assert!(input.pattern.is_none());
        }

        #[test]
        fn test_decode_input_with_pattern() {
            let args = vec![RedisJsonValue::String("test*".into())];
            let input = PubsubChannelsInput::decode(args).unwrap();
            assert!(input.pattern.is_some());
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = PubsubChannelsOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_with_channels() {
            let bytes = b"*2\r\n$4\r\nch_1\r\n$4\r\nch_2\r\n";
            let output = PubsubChannelsOutput::decode(bytes).unwrap();
            assert_eq!(output.count(), 2);
            assert_eq!(output.channels(), &["ch_1", "ch_2"]);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PubsubChannelsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PubsubChannelsInput { pattern: Some(RedisJsonValue::String("*".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PubsubChannelsInput::default();
            assert!(input.pattern.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_channels_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PubsubChannelsInput { pattern: None }.command()).await.expect("raw failed");

                    let output = PubsubChannelsOutput::decode(&result).expect("decode failed");
                    // No active subscriptions yet
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_channels_with_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&PubsubChannelsInput { pattern: Some(RedisJsonValue::String("test*".into())) }.command())
                        .await
                        .expect("raw failed");

                    let output = PubsubChannelsOutput::decode(&result).expect("decode failed");
                    // Should return empty or matching channels
                    assert!(output.channels().iter().all(|c| c.starts_with("test")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_channels_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PubsubChannelsInput { pattern: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let _output = PubsubChannelsOutput::decode(&result).expect("decode failed");
            // assert!(output.channels().len() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_channels_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PubsubChannelsInput { pattern: None }.command()).await.expect("raw failed");

            let _output = PubsubChannelsOutput::decode(&result).expect("decode failed");
            // assert!(output.channels().len() >= 0);

            ctx.stop().await;
        }
    }
}
