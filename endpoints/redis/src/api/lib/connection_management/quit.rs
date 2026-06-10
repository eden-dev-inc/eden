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

const API_INFO: ApiInfo<RedisApi, QuitInput> = ApiInfo::new(EpKind::Redis, RedisApi::Quit, "Closes the connection", ReqType::Write, false);

/// See official Redis documentation for `QUIT`
/// https://redis.io/docs/latest/commands/quit/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct QuitInput {}

impl Serialize for QuitInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("QuitInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(QuitInput, API_INFO);

impl RedisCommandInput for QuitInput {
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
            return Err(EpError::request(format!("QUIT requires no arguments, given {}", args.len())));
        }

        Ok(Self::default())
    }
}

/// Output for Redis QUIT command
///
/// Returns OK before closing the connection.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct QuitOutput {
    /// The response, typically "OK"
    result: String,
}

impl QuitOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    /// Get the result string
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if the result is OK
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    /// Decode the Redis protocol response into a QuitOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected QUIT response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected QUIT response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for QuitOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("QuitOutput", 1)?;
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
            let input = QuitInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nQUIT\r\n");
        }

        #[test]
        fn test_decode_simple_ok() {
            let output = QuitOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_bulk_string_ok() {
            let output = QuitOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = QuitOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = QuitInput::decode(args).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Quit);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let err = QuitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires no arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = QuitInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = QuitInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Quit);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: QUIT closes the connection, so we test it carefully.
        // After QUIT, the connection is no longer usable.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_quit_returns_ok() {
            // Test on a fresh connection for each protocol
            for resp in [RespVersion::Resp2, RespVersion::Resp3] {
                // Skip RESP3 on old Redis
                let mut ctx = setup(resp, None).await;

                let result = ctx.raw(&QuitInput {}.command()).await.expect("raw failed");

                let output = QuitOutput::decode(&result).expect("decode failed");
                assert!(output.is_ok(), "QUIT should return OK");

                // Connection is closed after QUIT, just stop the container
                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_quit_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&QuitInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK");
            let output = QuitOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_quit_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&QuitInput {}.command()).await.expect("raw failed");

            let output = QuitOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
