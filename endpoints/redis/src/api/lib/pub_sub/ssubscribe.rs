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

const API_INFO: ApiInfo<RedisApi, SsubscribeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Ssubscribe,
    "Listens for messages published to shard channels",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `SSUBSCRIBE`
/// https://redis.io/docs/latest/commands/ssubscribe/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SsubscribeInput {
    /// One or more shard channels to subscribe to (use RedisJsonValue::Array for multiple)
    pub(crate) shard_channel: RedisJsonValue,
}

impl Serialize for SsubscribeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SsubscribeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("shard_channel", &self.shard_channel)?;
        state.end()
    }
}

impl_redis_operation!(SsubscribeInput, API_INFO, { shard_channel });

impl RedisCommandInput for SsubscribeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.shard_channel);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SSUBSCRIBE requires at least 1 shard channel"));
        }

        let shard_channel = if args.len() == 1 {
            args.into_iter().next().unwrap()
        } else {
            RedisJsonValue::Array(args)
        };

        Ok(Self { shard_channel })
    }
}

/// Output for Redis SSUBSCRIBE command
///
/// Returns subscription confirmation with shard channel name and subscription count.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SsubscribeOutput {
    /// The type of message (always "ssubscribe" for this command)
    kind: String,
    /// The shard channel subscribed to
    shard_channel: String,
    /// Current number of shard subscriptions
    count: i64,
}

impl SsubscribeOutput {
    pub fn new(kind: String, shard_channel: String, count: i64) -> Self {
        Self { kind, shard_channel, count }
    }

    /// Get the message type
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Get the shard channel name
    pub fn shard_channel(&self) -> &str {
        &self.shard_channel
    }

    /// Get the subscription count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a SsubscribeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SSUBSCRIBE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::Push { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SSUBSCRIBE response: {:?}", other))),
            },
        }
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("SSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let shard_channel = match &items[1] {
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

        Ok(Self { kind, shard_channel, count })
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("SSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let shard_channel = match &items[1] {
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

        Ok(Self { kind, shard_channel, count })
    }
}

impl Serialize for SsubscribeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SsubscribeOutput", 3)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("shard_channel", &self.shard_channel)?;
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
            let input = SsubscribeInput { shard_channel: RedisJsonValue::String("shardch".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$10\r\nSSUBSCRIBE\r\n$7\r\nshardch\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = SsubscribeInput {
                shard_channel: RedisJsonValue::Array(vec![RedisJsonValue::String("sch1".into()), RedisJsonValue::String("sch2".into())]),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$10\r\nSSUBSCRIBE\r\n$4\r\nsch1\r\n$4\r\nsch2\r\n");
        }

        #[test]
        fn test_decode_input_valid_single() {
            let args = vec![RedisJsonValue::String("shardch".into())];
            let input = SsubscribeInput::decode(args).unwrap();
            assert!(matches!(input.shard_channel, RedisJsonValue::String(_)));
        }

        #[test]
        fn test_decode_input_valid_multiple() {
            let args = vec![RedisJsonValue::String("sch1".into()), RedisJsonValue::String("sch2".into())];
            let input = SsubscribeInput::decode(args).unwrap();
            assert!(matches!(input.shard_channel, RedisJsonValue::Array(_)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SsubscribeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 shard channel"));
        }

        #[test]
        fn test_decode_output_resp2() {
            let bytes = b"*3\r\n$10\r\nssubscribe\r\n$7\r\nshardch\r\n:1\r\n";
            let output = SsubscribeOutput::decode(bytes).unwrap();
            assert_eq!(output.kind(), "ssubscribe");
            assert_eq!(output.shard_channel(), "shardch");
            assert_eq!(output.count(), 1);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = SsubscribeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SsubscribeInput { shard_channel: RedisJsonValue::String("sch".into()) };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: SSUBSCRIBE requires Redis 7.0+ and is designed for cluster mode.
        // In standalone mode, it behaves like SUBSCRIBE.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ssubscribe_single_channel() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&SsubscribeInput { shard_channel: RedisJsonValue::String("shardtest".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = SsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "ssubscribe");
                    assert_eq!(output.shard_channel(), "shardtest");
                    assert_eq!(output.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ssubscribe_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result =
                ctx.raw(&SsubscribeInput { shard_channel: RedisJsonValue::String("r2sch".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = SsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "ssubscribe");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ssubscribe_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result =
                ctx.raw(&SsubscribeInput { shard_channel: RedisJsonValue::String("r3sch".into()) }.command()).await.expect("raw failed");

            let output = SsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "ssubscribe");

            ctx.stop().await;
        }
    }
}
