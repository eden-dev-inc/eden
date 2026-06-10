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

const API_INFO: ApiInfo<RedisApi, PsubscribeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Psubscribe,
    "Listens for messages published to channels that match one or more patterns",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `PSUBSCRIBE`
/// https://redis.io/docs/latest/commands/psubscribe/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PsubscribeInput {
    /// One or more patterns to subscribe to (use RedisJsonValue::Array for multiple)
    pub(crate) pattern: RedisJsonValue,
}

impl Serialize for PsubscribeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PsubscribeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("pattern", &self.pattern)?;
        state.end()
    }
}

impl_redis_operation!(PsubscribeInput, API_INFO, { pattern });

impl RedisCommandInput for PsubscribeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.pattern);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("PSUBSCRIBE requires at least 1 pattern"));
        }

        let pattern = if args.len() == 1 {
            args.into_iter().next().unwrap()
        } else {
            RedisJsonValue::Array(args)
        };

        Ok(Self { pattern })
    }
}

/// Output for Redis PSUBSCRIBE command
///
/// Returns subscription confirmation with pattern and subscription count.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PsubscribeOutput {
    /// The type of message (always "psubscribe" for this command)
    kind: String,
    /// The pattern subscribed to
    pattern: String,
    /// Current number of subscriptions (channels + patterns)
    count: i64,
}

impl PsubscribeOutput {
    pub fn new(kind: String, pattern: String, count: i64) -> Self {
        Self { kind, pattern, count }
    }

    /// Get the message type
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Get the pattern
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Get the subscription count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a PsubscribeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected PSUBSCRIBE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::Push { data, .. } => Self::decode_array_resp3(data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected PSUBSCRIBE response: {:?}", other))),
            },
        }
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("PSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let pattern = match &items[1] {
            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
            other => {
                return Err(EpError::parse(format!("unexpected pattern type: {:?}", other)));
            }
        };

        let count = match &items[2] {
            Resp2Frame::Integer(n) => *n,
            other => {
                return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
            }
        };

        Ok(Self { kind, pattern, count })
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Self, EpError> {
        if items.len() != 3 {
            return Err(EpError::parse(format!("PSUBSCRIBE response expected 3 elements, got {}", items.len())));
        }

        let kind = match &items[0] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => return Err(EpError::parse(format!("unexpected kind type: {:?}", other))),
        };

        let pattern = match &items[1] {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
            other => {
                return Err(EpError::parse(format!("unexpected pattern type: {:?}", other)));
            }
        };

        let count = match &items[2] {
            Resp3Frame::Number { data, .. } => *data,
            other => {
                return Err(EpError::parse(format!("unexpected count type: {:?}", other)));
            }
        };

        Ok(Self { kind, pattern, count })
    }
}

impl Serialize for PsubscribeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PsubscribeOutput", 3)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("pattern", &self.pattern)?;
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
        fn test_encode_command_single_pattern() {
            let input = PsubscribeInput { pattern: RedisJsonValue::String("news.*".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n");
        }

        #[test]
        fn test_encode_command_multiple_patterns() {
            let input = PsubscribeInput {
                pattern: RedisJsonValue::Array(vec![RedisJsonValue::String("news.*".into()), RedisJsonValue::String("weather.*".into())]),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n$9\r\nweather.*\r\n");
        }

        #[test]
        fn test_decode_input_valid_single() {
            let args = vec![RedisJsonValue::String("pattern*".into())];
            let input = PsubscribeInput::decode(args).unwrap();
            assert!(matches!(input.pattern, RedisJsonValue::String(_)));
        }

        #[test]
        fn test_decode_input_valid_multiple() {
            let args = vec![RedisJsonValue::String("p1*".into()), RedisJsonValue::String("p2*".into())];
            let input = PsubscribeInput::decode(args).unwrap();
            assert!(matches!(input.pattern, RedisJsonValue::Array(_)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PsubscribeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 pattern"));
        }

        #[test]
        fn test_decode_output_resp2() {
            let bytes = b"*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:1\r\n";
            let output = PsubscribeOutput::decode(bytes).unwrap();
            assert_eq!(output.kind(), "psubscribe");
            assert_eq!(output.pattern(), "news.*");
            assert_eq!(output.count(), 1);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PsubscribeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PsubscribeInput { pattern: RedisJsonValue::String("*".into()) };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psubscribe_single_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&PsubscribeInput { pattern: RedisJsonValue::String("test*".into()) }.command()).await.expect("raw failed");

                    let output = PsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "psubscribe");
                    assert_eq!(output.pattern(), "test*");
                    assert_eq!(output.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psubscribe_wildcard_all() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&PsubscribeInput { pattern: RedisJsonValue::String("*".into()) }.command()).await.expect("raw failed");

                    let output = PsubscribeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.kind(), "psubscribe");
                    assert_eq!(output.pattern(), "*");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psubscribe_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PsubscribeInput { pattern: RedisJsonValue::String("r2*".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = PsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "psubscribe");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psubscribe_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PsubscribeInput { pattern: RedisJsonValue::String("r3*".into()) }.command()).await.expect("raw failed");

            let output = PsubscribeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.kind(), "psubscribe");

            ctx.stop().await;
        }
    }
}
