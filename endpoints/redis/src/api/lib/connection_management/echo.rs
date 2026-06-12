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

const API_INFO: ApiInfo<RedisApi, EchoInput> = ApiInfo::new(EpKind::Redis, RedisApi::Echo, "Returns the given string", ReqType::Read, true);

/// See official Redis documentation for `ECHO`
/// https://redis.io/docs/latest/commands/echo/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct EchoInput {
    message: RedisJsonValue,
}

impl Serialize for EchoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EchoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

impl_redis_operation!(EchoInput, API_INFO, { message });

impl RedisCommandInput for EchoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.message);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("ECHO requires 1 argument, given {}", args.len())));
        }

        Ok(Self { message: args[0].clone() })
    }
}

/// Output for Redis ECHO command
///
/// Returns the message that was sent to the server.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct EchoOutput {
    /// The echoed message
    message: RedisJsonValue,
}

impl EchoOutput {
    pub fn new(message: RedisJsonValue) -> Self {
        Self { message }
    }

    /// Get the echoed message
    pub fn message(&self) -> &RedisJsonValue {
        &self.message
    }

    /// Get the message as a string if it is one
    pub fn as_str(&self) -> Option<&str> {
        match &self.message {
            RedisJsonValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Decode the Redis protocol response into an EchoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(s) => RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ECHO response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ECHO response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for EchoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EchoOutput", 1)?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = EchoInput { message: RedisJsonValue::String("Hello World".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nECHO\r\n$11\r\nHello World\r\n");
        }

        #[test]
        fn test_encode_command_empty_string() {
            let input = EchoInput { message: RedisJsonValue::String("".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = EchoOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_empty_string() {
            let output = EchoOutput::decode(b"$0\r\n\r\n").unwrap();
            assert_eq!(output.as_str(), Some(""));
        }

        #[test]
        fn test_decode_simple_string() {
            let output = EchoOutput::decode(b"+hello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = EchoOutput::decode(b"-ERR wrong number of arguments\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("test".into())];
            let input = EchoInput::decode(args).unwrap();
            assert_eq!(input.message, RedisJsonValue::String("test".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = EchoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = EchoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = EchoInput { message: RedisJsonValue::String("test".into()) };
            assert!(input.keys().is_empty());
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
        async fn test_echo_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&EchoInput { message: RedisJsonValue::String("Hello World".into()) }.command()).await.expect("raw failed");

                    let output = EchoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("Hello World"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_echo_empty_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&EchoInput { message: RedisJsonValue::String("".into()) }.command()).await.expect("raw failed");

                    let output = EchoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some(""));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_echo_special_characters() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&EchoInput { message: RedisJsonValue::String("Hello\r\nWorld\t!".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = EchoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("Hello\r\nWorld\t!"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_echo_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&EchoInput { message: RedisJsonValue::String("first".into()) }.command());
                    pipeline.extend_from_slice(&EchoInput { message: RedisJsonValue::String("second".into()) }.command());
                    pipeline.extend_from_slice(&EchoInput { message: RedisJsonValue::String("third".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = EchoOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.as_str(), Some("first"));

                    let out2 = EchoOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.as_str(), Some("second"));

                    let out3 = EchoOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.as_str(), Some("third"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_echo_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&EchoInput { message: RedisJsonValue::String("test".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = EchoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_str(), Some("test"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_echo_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&EchoInput { message: RedisJsonValue::String("test".into()) }.command()).await.expect("raw failed");

            let output = EchoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_str(), Some("test"));

            ctx.stop().await;
        }
    }
}
