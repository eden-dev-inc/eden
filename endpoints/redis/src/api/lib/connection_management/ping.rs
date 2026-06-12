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

const API_INFO: ApiInfo<RedisApi, PingInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Ping, "Returns the server's liveliness response", ReqType::Read, true);

/// See official Redis documentation for `PING`
/// https://redis.io/docs/latest/commands/ping/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PingInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<RedisJsonValue>,
}

impl Serialize for PingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields = if self.message.is_some() { 2 } else { 1 };
        let mut state = serializer.serialize_struct("PingInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(ref msg) = self.message {
            state.serialize_field("message", msg)?;
        }
        state.end()
    }
}

impl_redis_operation!(PingInput, API_INFO, { message });

impl RedisCommandInput for PingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(ref msg) = self.message {
            command.arg(msg);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            return Err(EpError::request(format!("PING takes at most 1 argument, given {}", args.len())));
        }

        Ok(Self { message: args.first().cloned() })
    }
}

/// Output for Redis PING command
///
/// Returns "PONG" if no argument is provided, or the argument if one is given.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PingOutput {
    /// The response: "PONG" or the echoed message
    response: RedisJsonValue,
}

impl PingOutput {
    pub fn new(response: RedisJsonValue) -> Self {
        Self { response }
    }

    /// Get the ping response
    pub fn response(&self) -> &RedisJsonValue {
        &self.response
    }

    /// Get the response as a string if it is one
    pub fn as_str(&self) -> Option<&str> {
        match &self.response {
            RedisJsonValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Check if the response is the standard PONG
    pub fn is_pong(&self) -> bool {
        self.as_str() == Some("PONG")
    }

    /// Decode the Redis protocol response into a PingOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let response = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::BulkString(bytes) => RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PING response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::BlobString { data, .. } => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PING response: {:?}", other)));
                }
            },
        };

        Ok(Self { response })
    }
}

impl Serialize for PingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PingOutput", 1)?;
        state.serialize_field("response", &self.response)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_message() {
            let input = PingInput { message: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nPING\r\n");
        }

        #[test]
        fn test_encode_command_with_message() {
            let input = PingInput { message: Some(RedisJsonValue::String("hello".into())) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
        }

        #[test]
        fn test_decode_simple_pong() {
            let output = PingOutput::decode(b"+PONG\r\n").unwrap();
            assert!(output.is_pong());
            assert_eq!(output.as_str(), Some("PONG"));
        }

        #[test]
        fn test_decode_bulk_string_response() {
            let output = PingOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(!output.is_pong());
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = PingOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PingInput::decode(args).unwrap();
            assert!(input.message.is_none());
        }

        #[test]
        fn test_decode_input_with_message() {
            let args = vec![RedisJsonValue::String("hello".into())];
            let input = PingInput::decode(args).unwrap();
            assert_eq!(input.message, Some(RedisJsonValue::String("hello".into())));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = PingInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at most 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PingInput { message: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PingInput::default();
            assert!(input.message.is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_no_message() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PingInput { message: None }.command()).await.expect("raw failed");

                    let output = PingOutput::decode(&result).expect("decode failed");
                    assert!(output.is_pong());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_with_message() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&PingInput { message: Some(RedisJsonValue::String("hello".into())) }.command()).await.expect("raw failed");

                    let output = PingOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_pong());
                    assert_eq!(output.as_str(), Some("hello"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_empty_message() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&PingInput { message: Some(RedisJsonValue::String("".into())) }.command()).await.expect("raw failed");

                    let output = PingOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some(""));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&PingInput { message: None }.command());
                    pipeline.extend_from_slice(&PingInput { message: Some(RedisJsonValue::String("one".into())) }.command());
                    pipeline.extend_from_slice(&PingInput { message: Some(RedisJsonValue::String("two".into())) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = PingOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.is_pong());

                    let out2 = PingOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.as_str(), Some("one"));

                    let out3 = PingOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.as_str(), Some("two"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PingInput { message: None }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+PONG\r\n", "RESP2 simple string PONG");
            let output = PingOutput::decode(&result).expect("decode failed");
            assert!(output.is_pong());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PingInput { message: None }.command()).await.expect("raw failed");

            let output = PingOutput::decode(&result).expect("decode failed");
            assert!(output.is_pong());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ping_resp2_with_message_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PingInput { message: Some(RedisJsonValue::String("test".into())) }.command()).await.expect("raw failed");

            // With a message, RESP2 returns bulk string
            assert!(result.starts_with(b"$"), "RESP2 should return bulk string with message");
            let output = PingOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_str(), Some("test"));

            ctx.stop().await;
        }
    }
}
