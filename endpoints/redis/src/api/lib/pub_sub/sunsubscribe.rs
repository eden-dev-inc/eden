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

const API_INFO: ApiInfo<RedisApi, SunsubscribeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sunsubscribe,
    "Stops listening to messages posted to shard channels",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `SUNSUBSCRIBE`
/// https://redis.io/docs/latest/commands/sunsubscribe/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SunsubscribeInput {
    /// Shard channels to unsubscribe from. If None, unsubscribes from all shard channels.
    /// Use RedisJsonValue::Array for multiple channels.
    shard_channel: Option<RedisJsonValue>,
}

impl Serialize for SunsubscribeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SunsubscribeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("shard_channel", &self.shard_channel)?;
        state.end()
    }
}

impl_redis_operation!(SunsubscribeInput, API_INFO, { shard_channel });

impl RedisCommandInput for SunsubscribeInput {
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

/// Output for Redis SUNSUBSCRIBE command
///
/// Returns unsubscription confirmation with shard channel name and remaining subscription count.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SunsubscribeOutput {
    /// The type of message (always "sunsubscribe" for this command)
    kind: String,
    /// The shard channel unsubscribed from
    shard_channel: String,
    /// Remaining number of shard subscriptions
    count: i64,
}

impl SunsubscribeOutput {
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

    /// Get the remaining subscription count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a SunsubscribeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SUNSUBSCRIBE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::Push { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SUNSUBSCRIBE response: {:?}", other))),
            },
        }
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("SUNSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let shard_channel = match &items[1] {
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

        Ok(Self { kind, shard_channel, count })
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("SUNSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let shard_channel = match &items[1] {
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

        Ok(Self { kind, shard_channel, count })
    }
}

impl Serialize for SunsubscribeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SunsubscribeOutput", 3)?;
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
        fn test_encode_command_no_channels() {
            let input = SunsubscribeInput { shard_channel: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$12\r\nSUNSUBSCRIBE\r\n");
        }

        #[test]
        fn test_encode_command_single_channel() {
            let input = SunsubscribeInput {
                shard_channel: Some(RedisJsonValue::String("shardch".into())),
            };
            assert_eq!(input.command().to_vec(), b"*2\r\n$12\r\nSUNSUBSCRIBE\r\n$7\r\nshardch\r\n");
        }

        #[test]
        fn test_encode_command_multiple_channels() {
            let input = SunsubscribeInput {
                shard_channel: Some(RedisJsonValue::Array(vec![
                    RedisJsonValue::String("sch1".into()),
                    RedisJsonValue::String("sch2".into()),
                ])),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$12\r\nSUNSUBSCRIBE\r\n$4\r\nsch1\r\n$4\r\nsch2\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = SunsubscribeInput::decode(args).unwrap();
            assert!(input.shard_channel.is_none());
        }

        #[test]
        fn test_decode_input_with_channels() {
            let args = vec![RedisJsonValue::String("sch1".into()), RedisJsonValue::String("sch2".into())];
            let input = SunsubscribeInput::decode(args).unwrap();
            assert!(input.shard_channel.is_some());
            assert!(matches!(input.shard_channel, Some(RedisJsonValue::Array(_))));
        }

        #[test]
        fn test_decode_output_resp2() {
            let bytes = b"*3\r\n$12\r\nsunsubscribe\r\n$7\r\nshardch\r\n:0\r\n";
            let output = SunsubscribeOutput::decode(bytes).unwrap();
            assert_eq!(output.kind(), "sunsubscribe");
            assert_eq!(output.shard_channel(), "shardch");
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = SunsubscribeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SunsubscribeInput { shard_channel: Some(RedisJsonValue::String("sch".into())) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = SunsubscribeInput::default();
            assert!(input.shard_channel.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SsubscribeInput;
        use crate::test_utils::*;
        use serial_test::serial;
        // Note: SUNSUBSCRIBE requires Redis 7.0+

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunsubscribe_after_ssubscribe() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // First subscribe
                    let _ = ctx
                        .raw(&SsubscribeInput { shard_channel: RedisJsonValue::String("shardtest".into()) }.command())
                        .await
                        .expect("ssubscribe failed");

                    // Then unsubscribe
                    let result = ctx
                        .raw(
                            &SunsubscribeInput {
                                shard_channel: Some(RedisJsonValue::String("shardtest".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "sunsubscribe");
                    assert_eq!(output.shard_channel(), "shardtest");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunsubscribe_not_subscribed() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SunsubscribeInput {
                                shard_channel: Some(RedisJsonValue::String("nonexistent".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "sunsubscribe");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunsubscribe_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(&SunsubscribeInput { shard_channel: Some(RedisJsonValue::String("r2sch".into())) }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = SunsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "sunsubscribe");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunsubscribe_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(&SunsubscribeInput { shard_channel: Some(RedisJsonValue::String("r3sch".into())) }.command())
                .await
                .expect("raw failed");

            let output = SunsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "sunsubscribe");

            ctx.stop().await;
        }
    }
}
