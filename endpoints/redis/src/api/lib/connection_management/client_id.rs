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

const API_INFO: ApiInfo<RedisApi, ClientIdInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientId,
    "Returns the unique client ID of the connection",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT ID`
/// https://redis.io/docs/latest/commands/client-id/
#[derive(Debug, Deserialize, PartialEq, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientIdInput {}

impl Serialize for ClientIdInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientIdInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientIdInput, API_INFO);

impl RedisCommandInput for ClientIdInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let api_str = API_INFO.api.to_string();
        let args: Vec<&str> = api_str.split_whitespace().collect();
        let mut command = crate::command::cmd(args[0]);
        for arg in &args[1..] {
            command.arg(*arg);
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            return Err(EpError::request(format!("CLIENT ID requires no arguments, given {}", args.len())));
        }

        Ok(Self::default())
    }
}

/// Output for Redis CLIENT ID command
///
/// Returns the unique client ID of the current connection.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientIdOutput {
    /// The unique client ID
    id: i64,
}

impl ClientIdOutput {
    pub fn new(id: i64) -> Self {
        Self { id }
    }

    /// Get the client ID
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Decode the Redis protocol response into a ClientIdOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let id = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT ID response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT ID response: {:?}", other)));
                }
            },
        };

        Ok(Self { id })
    }
}

impl Serialize for ClientIdOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientIdOutput", 1)?;
        state.serialize_field("id", &self.id)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::RedisCommandInput;
    use crate::protocol::decoder::{DecoderRespFrame, RedisCommandArgs};
    use redis_protocol::resp2::decode::decode as decode_resp2;
    use redis_protocol::resp3::decode::complete::decode as decode_resp3;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClientIdInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n");
        }

        #[test]
        fn test_decode_integer_response() {
            let output = ClientIdOutput::decode(b":12345\r\n").unwrap();
            assert_eq!(output.id(), 12345);
        }

        #[test]
        fn test_decode_large_id() {
            let output = ClientIdOutput::decode(b":9999999999\r\n").unwrap();
            assert_eq!(output.id(), 9999999999);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClientIdOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let result = ClientIdInput::decode(args).unwrap();
            assert_eq!(result, ClientIdInput::default());
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = ClientIdInput::decode(args);
            assert!(result.is_err());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClientIdInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_raw_resp2_client_id() {
            let raw = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::ClientId);
            assert_eq!(input.args().len(), 0);
        }

        #[test]
        fn test_decode_raw_resp2_client_id_lowercase() {
            let raw = b"*2\r\n$6\r\nclient\r\n$2\r\nid\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::ClientId);
        }

        #[test]
        fn test_decode_raw_resp3_client_id() {
            let raw = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::ClientId);
            assert_eq!(input.args().len(), 0);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_id_returns_integer() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientIdInput {}.command()).await.expect("raw failed");

                    let output = ClientIdOutput::decode(&result).expect("decode failed");
                    assert!(output.id() > 0, "Client ID should be positive");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_id_unique_per_connection() {
            // Each connection should get a unique client ID
            // Use pinned connections to guarantee we're getting distinct connections
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Get two separate pinned connections from the pool
            let mut conn1 = ctx.pinned_connection().await.expect("pinned connection 1 failed");
            let mut conn2 = ctx.pinned_connection().await.expect("pinned connection 2 failed");

            let result1 = TestContext::raw_on_pinned(&mut conn1, &ClientIdInput {}.command()).await.expect("raw failed");
            let result2 = TestContext::raw_on_pinned(&mut conn2, &ClientIdInput {}.command()).await.expect("raw failed");

            let output1 = ClientIdOutput::decode(&result1).expect("decode failed");
            let output2 = ClientIdOutput::decode(&result2).expect("decode failed");

            assert_ne!(output1.id(), output2.id(), "Different connections should have different IDs");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_id_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ClientIdInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer type");
            let output = ClientIdOutput::decode(&result).expect("decode failed");
            assert!(output.id() > 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_id_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ClientIdInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP3 should return integer type");
            let output = ClientIdOutput::decode(&result).expect("decode failed");
            assert!(output.id() > 0);

            ctx.stop().await;
        }
    }
}
