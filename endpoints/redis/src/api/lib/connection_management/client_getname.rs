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

const API_INFO: ApiInfo<RedisApi, ClientGetnameInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClientGetname, "Returns the name of the connection", ReqType::Read, false);

/// See official Redis documentation for `CLIENT GETNAME`
/// https://redis.io/docs/latest/commands/client-getname/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientGetnameInput {}

impl Serialize for ClientGetnameInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientGetnameInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientGetnameInput, API_INFO);

impl RedisCommandInput for ClientGetnameInput {
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
            return Err(EpError::request(format!("CLIENT GETNAME requires no arguments, given {}", args.len())));
        }

        Ok(Self::default())
    }
}

/// Output for Redis CLIENT GETNAME command
///
/// Returns the name of the current connection, or None if no name is set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientGetnameOutput {
    /// The connection name, or None if not set
    name: Option<String>,
}

impl ClientGetnameOutput {
    pub fn new(name: Option<String>) -> Self {
        Self { name }
    }

    /// Get the connection name
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Check if a name is set
    pub fn has_name(&self) -> bool {
        self.name.is_some()
    }

    /// Decode the Redis protocol response into a ClientGetnameOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let name = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(s) => Some(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT GETNAME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT GETNAME response: {:?}", other)));
                }
            },
        };

        Ok(Self { name })
    }
}

impl Serialize for ClientGetnameOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientGetnameOutput", 1)?;
        state.serialize_field("name", &self.name)?;
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
            let input = ClientGetnameInput {};
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"CLIENT"));
            assert!(cmd.windows(7).any(|w| w == b"GETNAME"));
        }

        #[test]
        fn test_decode_bulk_string_name() {
            let output = ClientGetnameOutput::decode(b"$6\r\nmyconn\r\n").unwrap();
            assert!(output.has_name());
            assert_eq!(output.name(), Some("myconn"));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = ClientGetnameOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.has_name());
            assert_eq!(output.name(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ClientGetnameOutput::decode(b"_\r\n").unwrap();
            assert!(!output.has_name());
            assert_eq!(output.name(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClientGetnameOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClientGetnameInput::decode(args).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClientGetname);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let args = vec![RedisJsonValue::String("extra".into())];
            let err = ClientGetnameInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires no arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClientGetnameInput {};
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::connection_management::client_setname::ClientSetnameInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_getname_no_name_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientGetnameInput {}.command()).await.expect("raw failed");

                    let output = ClientGetnameOutput::decode(&result).expect("decode failed");
                    assert!(!output.has_name(), "New connection should have no name");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_getname_after_setname() {
            // This test uses pinned connection because CLIENT SETNAME/GETNAME
            // are connection-specific commands that require the same connection
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut conn = ctx.pinned_connection().await.expect("pinned connection failed");

                    // Set a connection name on the pinned connection
                    TestContext::raw_on_pinned(
                        &mut conn,
                        &ClientSetnameInput {
                            connection_name: RedisJsonValue::String("test-connection".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("SETNAME failed");

                    // Get the name on the same connection
                    let result = TestContext::raw_on_pinned(&mut conn, &ClientGetnameInput {}.command()).await.expect("raw failed");

                    let output = ClientGetnameOutput::decode(&result).expect("decode failed");
                    assert!(output.has_name());
                    assert_eq!(output.name(), Some("test-connection"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_getname_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ClientGetnameInput {}.command()).await.expect("raw failed");

            // No name set, should be null bulk string
            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null format");
            let output = ClientGetnameOutput::decode(&result).expect("decode failed");
            assert!(!output.has_name());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_getname_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ClientGetnameInput {}.command()).await.expect("raw failed");

            // No name set, should be null
            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = ClientGetnameOutput::decode(&result).expect("decode failed");
            assert!(!output.has_name());

            ctx.stop().await;
        }
    }
}
