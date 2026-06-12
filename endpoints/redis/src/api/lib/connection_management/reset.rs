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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ResetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Reset, "Resets the connection", ReqType::Write, false);

/// See official Redis documentation for `RESET`
/// https://redis.io/docs/latest/commands/reset/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ResetInput {}

impl Serialize for ResetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ResetInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ResetInput, API_INFO);

impl RedisCommandInput for ResetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            return Err(EpError::request(format!("RESET requires no arguments, given {}", args.len())));
        }

        Ok(Self::default())
    }
}

/// Output for Redis RESET command
///
/// Returns RESET to confirm the connection has been reset.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ResetOutput {
    /// The response, typically "RESET"
    result: String,
}

impl ResetOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    /// Get the result string
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if the result indicates a successful reset
    pub fn is_reset(&self) -> bool {
        self.result == "RESET"
    }

    /// Decode the Redis protocol response into a ResetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected RESET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected RESET response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ResetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ResetOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = ResetInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$5\r\nRESET\r\n");
        }

        #[test]
        fn test_decode_simple_reset() {
            let output = ResetOutput::decode(b"+RESET\r\n").unwrap();
            assert!(output.is_reset());
            assert_eq!(output.result(), "RESET");
        }

        #[test]
        fn test_decode_bulk_string_reset() {
            let output = ResetOutput::decode(b"$5\r\nRESET\r\n").unwrap();
            assert!(output.is_reset());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ResetOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ResetInput::decode(args).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Reset);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let err = ResetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires no arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ResetInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = ResetInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Reset);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::api::{key::RedisKey, value::RedisJsonValue};
        use crate::test_utils::*;
        use serial_test::serial;

        // RESET command was added in Redis 6.2

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_reset_returns_reset() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ResetInput {}.command()).await.expect("raw failed");

                    let output = ResetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_reset(), "RESET should return RESET");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_reset_clears_transaction_state() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Start a MULTI transaction
                    ctx.raw(b"*1\r\n$5\r\nMULTI\r\n").await.expect("MULTI failed");

                    // Queue a command
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("test".into()),
                            value: RedisJsonValue::String("value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("SET in transaction failed");

                    // RESET should clear the transaction
                    let result = ctx.raw(&ResetInput {}.command()).await.expect("raw failed");

                    let output = ResetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_reset());

                    // Connection should be back to normal state
                    // We can execute a new command without EXEC/DISCARD
                    let ping_result = ctx.raw(b"*1\r\n$4\r\nPING\r\n").await.expect("PING failed");

                    assert!(ping_result.starts_with(b"+PONG"), "Should be able to PING after RESET");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_reset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.0")).await;

            let result = ctx.raw(&ResetInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+RESET\r\n", "RESP2 simple string RESET");
            let output = ResetOutput::decode(&result).expect("decode failed");
            assert!(output.is_reset());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_reset_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.0")).await;

            let result = ctx.raw(&ResetInput {}.command()).await.expect("raw failed");

            let output = ResetOutput::decode(&result).expect("decode failed");
            assert!(output.is_reset());

            ctx.stop().await;
        }
    }
}
