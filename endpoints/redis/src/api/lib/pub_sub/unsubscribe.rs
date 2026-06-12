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

const API_INFO: ApiInfo<RedisApi, UnsubscribeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Unsubscribe,
    "Stops listening to messages posted to channels",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `UNSUBSCRIBE`
/// https://redis.io/docs/latest/commands/unsubscribe/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct UnsubscribeInput {
    /// Channels to unsubscribe from. If None, unsubscribes from all channels.
    /// Use RedisJsonValue::Array for multiple channels.
    channel: Option<RedisJsonValue>,
}

impl Serialize for UnsubscribeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UnsubscribeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("channel", &self.channel)?;
        state.end()
    }
}

impl_redis_operation!(UnsubscribeInput, API_INFO, { channel });

impl RedisCommandInput for UnsubscribeInput {
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

/// Output for Redis UNSUBSCRIBE command
///
/// Returns unsubscription confirmation with channel name and remaining subscription count.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct UnsubscribeOutput {
    /// The type of message (always "unsubscribe" for this command)
    kind: String,
    /// The channel unsubscribed from
    channel: String,
    /// Remaining number of subscriptions
    count: i64,
}

impl UnsubscribeOutput {
    pub fn new(kind: String, channel: String, count: i64) -> Self {
        Self { kind, channel, count }
    }

    /// Get the message type
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Get the channel name
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Get the remaining subscription count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into an UnsubscribeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected UNSUBSCRIBE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::Push { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected UNSUBSCRIBE response: {:?}", other))),
            },
        }
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("UNSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let channel = match &items[1] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            Resp2Frame::Null => String::new(),
            other => {
                return Err(EpError::parse(format!("unexpected channel type: {:?}", other)));
            }
        };

        let count = match &items[2] {
            Resp2Frame::Integer(n) => *n,
            other => {
                return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
            }
        };

        Ok(Self { kind, channel, count })
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("UNSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let channel = match &items[1] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::Null => String::new(),
            other => {
                return Err(EpError::parse(format!("unexpected channel type: {:?}", other)));
            }
        };

        let count = match &items[2] {
            Resp3Frame::Number { data, .. } => *data,
            other => {
                return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
            }
        };

        Ok(Self { kind, channel, count })
    }
}

impl Serialize for UnsubscribeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("UnsubscribeOutput", 3)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("channel", &self.channel)?;
        state.serialize_field("count", &self.count)?;
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
            let input = UnsubscribeInput { channel: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$11\r\nUNSUBSCRIBE\r\n");
        }

        #[test]
        fn test_encode_command_single_channel() {
            let input = UnsubscribeInput { channel: Some(RedisJsonValue::String("mychannel".into())) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$9\r\nmychannel\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = UnsubscribeInput {
                channel: Some(RedisJsonValue::Array(vec![
                    RedisJsonValue::String("ch1".into()),
                    RedisJsonValue::String("ch2".into()),
                ])),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nch1\r\n$3\r\nch2\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = UnsubscribeInput::decode(args).unwrap();
            assert!(input.channel.is_none());
        }

        #[test]
        fn test_decode_input_with_channels() {
            let args = vec![RedisJsonValue::String("ch1".into()), RedisJsonValue::String("ch2".into())];
            let input = UnsubscribeInput::decode(args).unwrap();
            assert!(input.channel.is_some());
            assert!(matches!(input.channel, Some(RedisJsonValue::Array(_))));
        }

        #[test]
        fn test_decode_output_resp2() {
            let bytes = b"*3\r\n$11\r\nunsubscribe\r\n$9\r\nmychannel\r\n:0\r\n";
            let output = UnsubscribeOutput::decode(bytes).unwrap();
            assert_eq!(output.kind(), "unsubscribe");
            assert_eq!(output.channel(), "mychannel");
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = UnsubscribeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = UnsubscribeInput { channel: Some(RedisJsonValue::String("ch".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = UnsubscribeInput::default();
            assert!(input.channel.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SubscribeInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unsubscribe_after_subscribe() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First subscribe
                    let _ = ctx
                        .raw(&SubscribeInput { channel: RedisJsonValue::String("testch".into()) }.command())
                        .await
                        .expect("subscribe failed");

                    // Then unsubscribe
                    let result = ctx
                        .raw(&UnsubscribeInput { channel: Some(RedisJsonValue::String("testch".into())) }.command())
                        .await
                        .expect("raw failed");

                    let output = UnsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "unsubscribe");
                    assert_eq!(output.channel(), "testch");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unsubscribe_not_subscribed() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&UnsubscribeInput { channel: Some(RedisJsonValue::String("nonexistent".into())) }.command())
                        .await
                        .expect("raw failed");

                    let output = UnsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "unsubscribe");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unsubscribe_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result =
                ctx.raw(&UnsubscribeInput { channel: Some(RedisJsonValue::String("r2ch".into())) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = UnsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "unsubscribe");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unsubscribe_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result =
                ctx.raw(&UnsubscribeInput { channel: Some(RedisJsonValue::String("r3ch".into())) }.command()).await.expect("raw failed");

            let output = UnsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "unsubscribe");

            ctx.stop().await;
        }
    }
}
