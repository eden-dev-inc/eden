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

const API_INFO: ApiInfo<RedisApi, SubscribeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Subscribe,
    "Listens for messages published to channels",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `SUBSCRIBE`
/// https://redis.io/docs/latest/commands/subscribe/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SubscribeInput {
    /// One or more channels to subscribe to (use RedisJsonValue::Array for multiple)
    pub(crate) channel: RedisJsonValue,
}

impl Serialize for SubscribeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SubscribeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("channel", &self.channel)?;
        state.end()
    }
}

impl_redis_operation!(SubscribeInput, API_INFO, { channel });

impl RedisCommandInput for SubscribeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.channel);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SUBSCRIBE requires at least 1 channel"));
        }

        let channel = if args.len() == 1 {
            args.into_iter().next().unwrap()
        } else {
            RedisJsonValue::Array(args)
        };

        Ok(Self { channel })
    }
}

/// Output for Redis SUBSCRIBE command
///
/// Returns subscription confirmation with channel name and subscription count.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SubscribeOutput {
    /// The type of message (always "subscribe" for this command)
    kind: String,
    /// The channel subscribed to
    channel: String,
    /// Current number of subscriptions
    count: i64,
}

impl SubscribeOutput {
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

    /// Get the subscription count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a SubscribeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SUBSCRIBE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::Push { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SUBSCRIBE response: {:?}", other))),
            },
        }
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("SUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let channel = match &items[1] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
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
            return Err(EpError::parse(format!("SUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let channel = match &items[1] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
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

impl Serialize for SubscribeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SubscribeOutput", 3)?;
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
        fn test_encode_command_single_channel() {
            let input = SubscribeInput { channel: RedisJsonValue::String("mychannel".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$9\r\nSUBSCRIBE\r\n$9\r\nmychannel\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = SubscribeInput {
                channel: RedisJsonValue::Array(vec![RedisJsonValue::String("ch1".into()), RedisJsonValue::String("ch2".into())]),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n$3\r\nch2\r\n");
        }

        #[test]
        fn test_decode_input_valid_single() {
            let args = vec![RedisJsonValue::String("channel".into())];
            let input = SubscribeInput::decode(args).unwrap();
            assert!(matches!(input.channel, RedisJsonValue::String(_)));
        }

        #[test]
        fn test_decode_input_valid_multiple() {
            let args = vec![
                RedisJsonValue::String("ch1".into()),
                RedisJsonValue::String("ch2".into()),
                RedisJsonValue::String("ch3".into()),
            ];
            let input = SubscribeInput::decode(args).unwrap();
            assert!(matches!(input.channel, RedisJsonValue::Array(_)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SubscribeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 channel"));
        }

        #[test]
        fn test_decode_output_resp2() {
            // *3\r\n$9\r\nsubscribe\r\n$9\r\nmychannel\r\n:1\r\n
            let bytes = b"*3\r\n$9\r\nsubscribe\r\n$9\r\nmychannel\r\n:1\r\n";
            let output = SubscribeOutput::decode(bytes).unwrap();
            assert_eq!(output.kind(), "subscribe");
            assert_eq!(output.channel(), "mychannel");
            assert_eq!(output.count(), 1);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = SubscribeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SubscribeInput { channel: RedisJsonValue::String("ch".into()) };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: SUBSCRIBE puts the connection in subscriber mode.
        // These tests verify the initial subscription response.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_subscribe_single_channel() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&SubscribeInput { channel: RedisJsonValue::String("testch".into()) }.command()).await.expect("raw failed");

                    let output = SubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "subscribe");
                    assert_eq!(output.channel(), "testch");
                    assert_eq!(output.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_subscribe_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&SubscribeInput { channel: RedisJsonValue::String("r2ch".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = SubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "subscribe");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_subscribe_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&SubscribeInput { channel: RedisJsonValue::String("r3ch".into()) }.command()).await.expect("raw failed");

            // RESP3 uses push type (>) for pub/sub messages
            let output = SubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "subscribe");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }
    }
}
