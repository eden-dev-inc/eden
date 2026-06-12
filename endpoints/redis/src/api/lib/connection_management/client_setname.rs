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

const API_INFO: ApiInfo<RedisApi, ClientSetnameInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClientSetname, "Sets the connection name", ReqType::Write, false);

/// See official Redis documentation for `CLIENT SETNAME`
/// https://redis.io/docs/latest/commands/client-setname/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientSetnameInput {
    pub connection_name: RedisJsonValue,
}

impl Serialize for ClientSetnameInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientSetnameInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("connection_name", &self.connection_name)?;
        state.end()
    }
}

impl_redis_operation!(ClientSetnameInput, API_INFO, { connection_name });

impl RedisCommandInput for ClientSetnameInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.connection_name);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("CLIENT SETNAME requires 1 argument, given {}", args.len())));
        }

        Ok(Self { connection_name: args[0].clone() })
    }
}

/// Output for Redis CLIENT SETNAME command
///
/// Returns OK if the name was successfully set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientSetnameOutput {
    /// The result, typically "OK"
    result: String,
}

impl ClientSetnameOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    /// Get the result string
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if the name was successfully set
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    /// Decode the Redis protocol response into a ClientSetnameOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT SETNAME response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected CLIENT SETNAME response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientSetnameOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientSetnameOutput", 1)?;
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
            let input = ClientSetnameInput {
                connection_name: RedisJsonValue::String("myconnection".into()),
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"CLIENT"));
            assert!(cmd.windows(7).any(|w| w == b"SETNAME"));
            assert!(cmd.windows(12).any(|w| w == b"myconnection"));
        }

        #[test]
        fn test_decode_simple_ok() {
            let output = ClientSetnameOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClientSetnameOutput::decode(b"-ERR invalid name\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myname".into())];
            let input = ClientSetnameInput::decode(args).unwrap();
            assert_eq!(input.connection_name, RedisJsonValue::String("myname".into()));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClientSetnameInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args_fails() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = ClientSetnameInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClientSetnameInput { connection_name: RedisJsonValue::String("test".into()) };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::connection_management::client_getname::ClientGetnameInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_setname_success() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&ClientSetnameInput { connection_name: RedisJsonValue::String("test-conn".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = ClientSetnameOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_setname_then_getname() {
            // This test uses pinned connection because CLIENT SETNAME/GETNAME
            // are connection-specific commands that require the same connection
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut conn = ctx.pinned_connection().await.expect("pinned connection failed");

                    // Set name on the pinned connection
                    let set_result = TestContext::raw_on_pinned(
                        &mut conn,
                        &ClientSetnameInput {
                            connection_name: RedisJsonValue::String("verified-name".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let set_output = ClientSetnameOutput::decode(&set_result).expect("decode failed");
                    assert!(set_output.is_ok());

                    // Verify with GETNAME on the same connection
                    use crate::api::lib::connection_management::client_getname::ClientGetnameOutput;
                    let get_result = TestContext::raw_on_pinned(&mut conn, &ClientGetnameInput {}.command()).await.expect("raw failed");

                    let get_output = ClientGetnameOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.name(), Some("verified-name"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_setname_empty_clears_name() {
            // This test uses pinned connection because CLIENT SETNAME/GETNAME
            // are connection-specific commands that require the same connection
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut conn = ctx.pinned_connection().await.expect("pinned connection failed");

                    // First set a name
                    TestContext::raw_on_pinned(
                        &mut conn,
                        &ClientSetnameInput { connection_name: RedisJsonValue::String("temp-name".into()) }.command(),
                    )
                    .await
                    .expect("SETNAME failed");

                    // Clear it with empty string
                    let result = TestContext::raw_on_pinned(
                        &mut conn,
                        &ClientSetnameInput { connection_name: RedisJsonValue::String("".into()) }.command(),
                    )
                    .await
                    .expect("raw failed");

                    let output = ClientSetnameOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify name is cleared on the same connection
                    use crate::api::lib::connection_management::client_getname::ClientGetnameOutput;
                    let get_result = TestContext::raw_on_pinned(&mut conn, &ClientGetnameInput {}.command()).await.expect("raw failed");

                    let get_output = ClientGetnameOutput::decode(&get_result).expect("decode failed");
                    // Empty string or null depending on Redis version
                    assert!(get_output.name().is_none() || get_output.name() == Some(""), "Name should be cleared");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_setname_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(&ClientSetnameInput { connection_name: RedisJsonValue::String("r2name".into()) }.command())
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK");
            let output = ClientSetnameOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_setname_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(&ClientSetnameInput { connection_name: RedisJsonValue::String("r3name".into()) }.command())
                .await
                .expect("raw failed");

            let output = ClientSetnameOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
