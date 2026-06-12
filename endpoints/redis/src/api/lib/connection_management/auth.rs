use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, AuthInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Auth, "Authenticates the connection", ReqType::Read, false);

/// See official Redis documentation for `AUTH`
/// https://redis.io/docs/latest/commands/auth/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct AuthInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<RedisJsonValue>,
    password: RedisJsonValue,
}

impl Serialize for AuthInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields = if self.username.is_some() { 3 } else { 2 };
        let mut state = serializer.serialize_struct("AuthInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(ref username) = self.username {
            state.serialize_field("username", username)?;
        }
        state.serialize_field("password", &self.password)?;
        state.end()
    }
}

impl_redis_operation!(AuthInput, API_INFO, { username, password });

impl RedisCommandInput for AuthInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(ref username) = self.username {
            command.arg(username);
        }
        command.arg(&self.password);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("AUTH requires at least 1 argument (password), given 0"));
        } else if args.len() == 1 {
            return Ok(Self { username: None, password: args[0].clone() });
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "AUTH expects at most 2 arguments (username, password), given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { username: Some(args[0].clone()), password: args[1].clone() })
    }
}

/// Output for Redis AUTH command
///
/// Returns OK if authentication was successful.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct AuthOutput {
    /// The result, typically "OK"
    result: String,
}

impl AuthOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    /// Get the result string
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if authentication was successful
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    /// Decode the Redis protocol response into an AuthOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected AUTH response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected AUTH response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for AuthOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AuthOutput", 1)?;
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
        fn test_encode_command_password_only() {
            let input = AuthInput {
                username: None,
                password: RedisJsonValue::String("secret".into()),
            };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
        }

        #[test]
        fn test_encode_command_with_username() {
            let input = AuthInput {
                username: Some(RedisJsonValue::String("user".into())),
                password: RedisJsonValue::String("pass".into()),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"AUTH"));
            assert!(cmd.windows(4).any(|w| w == b"user"));
            assert!(cmd.windows(4).any(|w| w == b"pass"));
        }

        #[test]
        fn test_decode_simple_ok() {
            let output = AuthOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_error_wrong_password() {
            let err = AuthOutput::decode(b"-WRONGPASS invalid username-password pair\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGPASS"));
        }

        #[test]
        fn test_decode_error_no_auth() {
            let err = AuthOutput::decode(b"-ERR Client sent AUTH, but no password is set\r\n").unwrap_err();
            assert!(err.to_string().contains("no password"));
        }

        #[test]
        fn test_decode_input_password_only() {
            let args = vec![RedisJsonValue::String("mypassword".into())];
            let input = AuthInput::decode(args).unwrap();
            assert!(input.username.is_none());
            assert_eq!(input.password, RedisJsonValue::String("mypassword".into()));
        }

        #[test]
        fn test_decode_input_with_username() {
            let args = vec![RedisJsonValue::String("myuser".into()), RedisJsonValue::String("mypassword".into())];
            let input = AuthInput::decode(args).unwrap();
            assert_eq!(input.username, Some(RedisJsonValue::String("myuser".into())));
            assert_eq!(input.password, RedisJsonValue::String("mypassword".into()));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = AuthInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AuthInput {
                username: None,
                password: RedisJsonValue::String("pass".into()),
            };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: AUTH tests require a Redis instance configured with authentication.
        // These tests verify the command encoding and response decoding work correctly.
        // In a default Redis setup without authentication, AUTH will return an error.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_auth_no_password_configured() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Default Redis has no password, so AUTH should fail
                    let result = ctx
                        .raw(
                            &AuthInput {
                                username: None,
                                password: RedisJsonValue::String("anypassword".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should get an error response
                    let err = AuthOutput::decode(&result);
                    assert!(err.is_err(), "AUTH should fail on Redis without password");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_auth_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &AuthInput {
                        username: None,
                        password: RedisJsonValue::String("wrong".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP2 error starts with -
            assert!(result.starts_with(b"-"), "RESP2 error should start with -");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_auth_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &AuthInput {
                        username: None,
                        password: RedisJsonValue::String("wrong".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // Should get an error
            let err = AuthOutput::decode(&result);
            assert!(err.is_err());

            ctx.stop().await;
        }
    }
}
