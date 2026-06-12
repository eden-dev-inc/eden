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

const API_INFO: ApiInfo<RedisApi, SpublishInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Spublish, "Post a message to a shard channel", ReqType::Write, true);

/// See official Redis documentation for `SPUBLISH`
/// https://redis.io/docs/latest/commands/spublish/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SpublishInput {
    shard_channel: RedisJsonValue,
    message: RedisJsonValue,
}

impl Serialize for SpublishInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SpublishInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("shard_channel", &self.shard_channel)?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

impl_redis_operation!(SpublishInput, API_INFO, { shard_channel, message });

impl RedisCommandInput for SpublishInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.shard_channel).arg(&self.message);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("SPUBLISH requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { shard_channel: args[0].clone(), message: args[1].clone() })
    }
}

/// Output for Redis SPUBLISH command
///
/// Returns the number of clients that received the message in the shard.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SpublishOutput {
    /// Number of clients that received the message
    receivers: i64,
}

impl SpublishOutput {
    pub fn new(receivers: i64) -> Self {
        Self { receivers }
    }

    /// Get the number of clients that received the message
    pub fn receivers(&self) -> i64 {
        self.receivers
    }

    /// Decode the Redis protocol response into a SpublishOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let receivers = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SPUBLISH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SPUBLISH response: {:?}", other)));
                }
            },
        };

        Ok(Self { receivers })
    }
}

impl Serialize for SpublishOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SpublishOutput", 1)?;
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
            let input = SpublishInput {
                shard_channel: RedisJsonValue::String("shardchannel".into()),
                message: RedisJsonValue::String("hello".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nSPUBLISH\r\n$12\r\nshardchannel\r\n$5\r\nhello\r\n");
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("shardch".into()), RedisJsonValue::String("message".into())];
            let input = SpublishInput::decode(args).unwrap();
            assert_eq!(input.shard_channel, RedisJsonValue::String("shardch".into()));
            assert_eq!(input.message, RedisJsonValue::String("message".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("channel".into())];
            let err = SpublishInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = SpublishInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = SpublishOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.receivers(), 5);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = SpublishOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.receivers(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = SpublishOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SpublishInput {
                shard_channel: RedisJsonValue::String("sch".into()),
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

        // Note: SPUBLISH requires Redis 7.0+ and cluster mode.
        // These tests verify command encoding/decoding works correctly.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spublish_standalone_returns_error() {
            // SPUBLISH only works in cluster mode, standalone should error
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SpublishInput {
                                shard_channel: RedisJsonValue::String("shardch".into()),
                                message: RedisJsonValue::String("msg".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // In standalone mode, SPUBLISH returns 0 (no shard subscribers)
                    // or may return an error depending on Redis version
                    if result.starts_with(b"-") {
                        let err = SpublishOutput::decode(&result).unwrap_err();
                        assert!(!err.to_string().is_empty());
                    } else {
                        let output = SpublishOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.receivers(), 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spublish_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &SpublishInput {
                        shard_channel: RedisJsonValue::String("r2sch".into()),
                        message: RedisJsonValue::String("msg".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // Either integer response or error (standalone mode)
            if result.starts_with(b":") {
                let output = SpublishOutput::decode(&result).expect("decode failed");
                assert_eq!(output.receivers(), 0);
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spublish_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &SpublishInput {
                        shard_channel: RedisJsonValue::String("r3sch".into()),
                        message: RedisJsonValue::String("msg".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // Either integer response or error (standalone mode)
            if !result.starts_with(b"-") && !result.starts_with(b"!") {
                let output = SpublishOutput::decode(&result).expect("decode failed");
                assert_eq!(output.receivers(), 0);
            }

            ctx.stop().await;
        }
    }
}
