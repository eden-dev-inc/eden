use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::wrapper::RuleWrapper;
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

const API_INFO: ApiInfo<RedisApi, AclSetuserInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclSetuser,
    "Creates and modifies an ACL user and its rules",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ACL SETUSER`
/// https://redis.io/docs/latest/commands/acl-setuser/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclSetuserInput {
    pub username: RedisJsonValue,
    pub rules: Vec<RuleWrapper>,
}

impl Serialize for AclSetuserInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclSetuserInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("username", &self.username)?;
        state.serialize_field("rules", &self.rules)?;
        state.end()
    }
}

impl_redis_operation!(AclSetuserInput, API_INFO, { username, rules });

impl RedisCommandInput for AclSetuserInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        // Always include the username first
        command.arg(&self.username);

        // Then add any rules
        for rule in &self.rules {
            command.arg(RedisJsonValue::from(rule));
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("ACL SETUSER requires at least a username"));
        }

        let username = args[0].clone();
        let mut rules: Vec<RuleWrapper> = vec![];
        if args.len() > 1 {
            for rule in &args[1..] {
                rules.push(rule.clone().try_into()?);
            }
        };

        Ok(Self { username, rules })
    }
}

/// Output for Redis ACL SETUSER command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclSetuserOutput {
    /// Whether the operation was successful
    success: bool,
}

impl AclSetuserOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an AclSetuserOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => Ok(Self { success: s == b"OK" }),
                Resp2Frame::BulkString(s) => Ok(Self { success: s == b"OK" }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected ACL SETUSER response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => Ok(Self { success: data == b"OK" }),
                Resp3Frame::BlobString { data, .. } => Ok(Self { success: data == b"OK" }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected ACL SETUSER response: {:?}", other))),
            },
        }
    }
}

impl Serialize for AclSetuserOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclSetuserOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_username_only() {
            let input = AclSetuserInput {
                username: RedisJsonValue::String("newuser".into()),
                rules: vec![],
            };
            // ACL SETUSER splits into: ACL, SETUSER, newuser
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\nnewuser\r\n");
        }

        #[test]
        fn test_encode_command_with_rules() {
            let input = AclSetuserInput {
                username: RedisJsonValue::String("testuser".into()),
                rules: vec![
                    RuleWrapper::try_from(RedisJsonValue::String("on".into())).unwrap(),
                    RuleWrapper::try_from(RedisJsonValue::String(">password".into())).unwrap(),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            // ACL SETUSER is now split into ACL and SETUSER
            assert!(cmd_str.contains("ACL"));
            assert!(cmd_str.contains("SETUSER"));
            assert!(cmd_str.contains("testuser"));
        }

        #[test]
        fn test_decode_ok_simple_string() {
            let output = AclSetuserOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_ok_bulk_string() {
            let output = AclSetuserOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclSetuserOutput::decode(b"-ERR invalid rule\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_username_only() {
            let args = vec![RedisJsonValue::String("myuser".into())];
            let input = AclSetuserInput::decode(args).unwrap();
            assert_eq!(input.username, RedisJsonValue::String("myuser".into()));
            assert!(input.rules.is_empty());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = AclSetuserInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("username"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclSetuserInput {
                username: RedisJsonValue::String("user".into()),
                rules: vec![],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclSetuserInput {
                username: RedisJsonValue::String("user".into()),
                rules: vec![],
            };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclSetuser);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::acl::{AclDeluserInput, AclGetuserInput, AclGetuserOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_setuser_create_simple() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclSetuserInput {
                                username: RedisJsonValue::String("simpleuser".into()),
                                rules: vec![],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclSetuserOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify user exists
                    let get_result = ctx
                        .raw(&AclGetuserInput { username: RedisJsonValue::String("simpleuser".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = AclGetuserOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());

                    // Cleanup
                    ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("simpleuser".into())] }.command())
                        .await
                        .expect("cleanup failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_setuser_with_rules() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclSetuserInput {
                                username: RedisJsonValue::String("ruleduser".into()),
                                rules: vec![
                                    RuleWrapper::try_from(RedisJsonValue::String("on".into())).unwrap(),
                                    RuleWrapper::try_from(RedisJsonValue::String("+get".into())).unwrap(),
                                    RuleWrapper::try_from(RedisJsonValue::String("~keys:*".into())).unwrap(),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclSetuserOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify user settings
                    let get_result = ctx
                        .raw(&AclGetuserInput { username: RedisJsonValue::String("ruleduser".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = AclGetuserOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());
                    assert!(get_output.flags().iter().any(|f| f == "on"));

                    // Cleanup
                    ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("ruleduser".into())] }.command())
                        .await
                        .expect("cleanup failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_setuser_modify_existing() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Create user
                    ctx.raw(
                        &AclSetuserInput {
                            username: RedisJsonValue::String("moduser".into()),
                            rules: vec![RuleWrapper::try_from(RedisJsonValue::String("off".into())).unwrap()],
                        }
                        .command(),
                    )
                    .await
                    .expect("create failed");

                    // Modify user to be enabled
                    let result = ctx
                        .raw(
                            &AclSetuserInput {
                                username: RedisJsonValue::String("moduser".into()),
                                rules: vec![RuleWrapper::try_from(RedisJsonValue::String("on".into())).unwrap()],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclSetuserOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify modification
                    let get_result = ctx
                        .raw(&AclGetuserInput { username: RedisJsonValue::String("moduser".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = AclGetuserOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.flags().iter().any(|f| f == "on"));

                    // Cleanup
                    ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("moduser".into())] }.command())
                        .await
                        .expect("cleanup failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_setuser_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &AclSetuserInput {
                        username: RedisJsonValue::String("resp2user".into()),
                        rules: vec![],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK") || result.starts_with(b"$"), "RESP2 should return OK");
            let output = AclSetuserOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            // Cleanup
            ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("resp2user".into())] }.command())
                .await
                .expect("cleanup failed");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_setuser_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &AclSetuserInput {
                        username: RedisJsonValue::String("resp3user".into()),
                        rules: vec![],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = AclSetuserOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            // Cleanup
            ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("resp3user".into())] }.command())
                .await
                .expect("cleanup failed");

            ctx.stop().await;
        }
    }
}
