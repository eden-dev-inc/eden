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

const API_INFO: ApiInfo<RedisApi, PublishInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Publish, "Posts a message to a channel", ReqType::Write, true);

/// See official Redis documentation for `PUBLISH`
/// https://redis.io/docs/latest/commands/publish/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PublishInput {
    channel: RedisJsonValue,
    message: RedisJsonValue,
}

impl Serialize for PublishInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PublishInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("channel", &self.channel)?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

impl_redis_operation!(PublishInput, API_INFO, { channel, message });

impl RedisCommandInput for PublishInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.channel).arg(&self.message);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("PUBLISH requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { channel: args[0].clone(), message: args[1].clone() })
    }
}

/// Output for Redis PUBLISH command
///
/// Returns the number of clients that received the message.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PublishOutput {
    /// Number of clients that received the message
    receivers: i64,
}

impl PublishOutput {
    pub fn new(receivers: i64) -> Self {
        Self { receivers }
    }

    /// Get the number of clients that received the message
    pub fn receivers(&self) -> i64 {
        self.receivers
    }

    /// Decode the Redis protocol response into a PublishOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let receivers = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBLISH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PUBLISH response: {:?}", other)));
                }
            },
        };

        Ok(Self { receivers })
    }
}

impl Serialize for PublishOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PublishOutput", 1)?;
        state.serialize_field("receivers", &self.receivers)?;
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
            let input = PublishInput {
                channel: RedisJsonValue::String("mychannel".into()),
                message: RedisJsonValue::String("hello".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nPUBLISH\r\n$9\r\nmychannel\r\n$5\r\nhello\r\n");
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("channel".into()), RedisJsonValue::String("message".into())];
            let input = PublishInput::decode(args).unwrap();
            assert_eq!(input.channel, RedisJsonValue::String("channel".into()));
            assert_eq!(input.message, RedisJsonValue::String("message".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("channel".into())];
            let err = PublishInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = PublishInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = PublishOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.receivers(), 3);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = PublishOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.receivers(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PublishOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PublishInput {
                channel: RedisJsonValue::String("ch".into()),
                message: RedisJsonValue::String("msg".into()),
            };
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
        async fn test_publish_no_subscribers() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PublishInput {
                                channel: RedisJsonValue::String("testchannel".into()),
                                message: RedisJsonValue::String("testmessage".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PublishOutput::decode(&result).expect("decode failed");
                    // No subscribers, should return 0
                    assert_eq!(output.receivers(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_publish_empty_message() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PublishInput {
                                channel: RedisJsonValue::String("ch".into()),
                                message: RedisJsonValue::String("".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PublishOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.receivers(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_publish_binary_message() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PublishInput {
                                channel: RedisJsonValue::String("binary_ch".into()),
                                message: RedisJsonValue::String("hello\x00world".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PublishOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.receivers(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_publish_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &PublishInput {
                        channel: RedisJsonValue::String("r2ch".into()),
                        message: RedisJsonValue::String("msg".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = PublishOutput::decode(&result).expect("decode failed");
            assert_eq!(output.receivers(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_publish_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &PublishInput {
                        channel: RedisJsonValue::String("r3ch".into()),
                        message: RedisJsonValue::String("msg".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = PublishOutput::decode(&result).expect("decode failed");
            assert_eq!(output.receivers(), 0);

            ctx.stop().await;
        }
    }
}
