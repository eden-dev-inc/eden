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

const API_INFO: ApiInfo<RedisApi, AclDryrunInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclDryrun,
    "Simulates the execution of a command by a user, without executing the command",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL DRYRUN`
/// https://redis.io/docs/latest/commands/acl-dryrun/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclDryrunInput {
    username: RedisJsonValue,
    command: RedisJsonValue,
    args: Option<Vec<RedisJsonValue>>,
}

impl Serialize for AclDryrunInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("AclDryrunInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("username", &self.username)?;
        state.serialize_field("command", &self.command)?;
        if let Some(args) = &self.args {
            state.serialize_field("args", &args)?;
        }
        state.end()
    }
}

impl_redis_operation!(AclDryrunInput, API_INFO, { username, command, args });

impl RedisCommandInput for AclDryrunInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.username).arg(&self.command);

        if let Some(args) = &self.args {
            for arg in args {
                command.arg(arg);
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!(
                "ACL DRYRUN requires at least 2 arguments (username and command), given {}",
                args.len(),
            )));
        }

        let username = args[0].clone();
        let command = args[1].clone();
        let command_args = if args.len() > 2 { Some(args[2..].to_vec()) } else { None };

        Ok(Self { username, command, args: command_args })
    }
}

/// Output for Redis ACL DRYRUN command
///
/// Returns "OK" if the user can execute the command, or an error message explaining why not.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclDryrunOutput {
    /// The result - "OK" or an error message
    result: String,
    /// Whether the command would be allowed
    allowed: bool,
}

impl AclDryrunOutput {
    pub fn new(result: String, allowed: bool) -> Self {
        Self { result, allowed }
    }

    /// Get the result message
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if the command would be allowed
    pub fn is_allowed(&self) -> bool {
        self.allowed
    }

    /// Decode the Redis protocol response into an AclDryrunOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let (result, allowed) = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(bytes) => {
                    let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                    let allowed = s == "OK";
                    (s, allowed)
                }
                Resp2Frame::BulkString(bytes) => {
                    let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                    let allowed = s == "OK";
                    (s, allowed)
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL DRYRUN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    let allowed = s == "OK";
                    (s, allowed)
                }
                Resp3Frame::BlobString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    let allowed = s == "OK";
                    (s, allowed)
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL DRYRUN response: {:?}", other)));
                }
            },
        };

        Ok(Self { result, allowed })
    }
}

impl Serialize for AclDryrunOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclDryrunOutput", 2)?;
        state.serialize_field("result", &self.result)?;
        state.serialize_field("allowed", &self.allowed)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_simple() {
            let input = AclDryrunInput {
                username: RedisJsonValue::String("default".into()),
                command: RedisJsonValue::String("GET".into()),
                args: Some(vec![RedisJsonValue::String("mykey".into())]),
            };
            // ACL DRYRUN splits into: ACL, DRYRUN, default, GET, mykey
            assert_eq!(
                input.command().to_vec(),
                b"*5\r\n$3\r\nACL\r\n$6\r\nDRYRUN\r\n$7\r\ndefault\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
            );
        }

        #[test]
        fn test_encode_command_no_args() {
            let input = AclDryrunInput {
                username: RedisJsonValue::String("default".into()),
                command: RedisJsonValue::String("PING".into()),
                args: None,
            };
            // ACL DRYRUN splits into: ACL, DRYRUN, default, PING
            assert_eq!(input.command().to_vec(), b"*4\r\n$3\r\nACL\r\n$6\r\nDRYRUN\r\n$7\r\ndefault\r\n$4\r\nPING\r\n");
        }

        #[test]
        fn test_decode_ok_simple_string() {
            let output = AclDryrunOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_allowed());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_ok_bulk_string() {
            let output = AclDryrunOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_allowed());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_not_allowed() {
            let output = AclDryrunOutput::decode(b"$48\r\nUser default has no permissions to run the 'set' command\r\n").unwrap();
            assert!(!output.is_allowed());
            assert!(output.result().contains("no permissions"));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclDryrunOutput::decode(b"-ERR unknown user\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("testuser".into()),
                RedisJsonValue::String("SET".into()),
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = AclDryrunInput::decode(args).unwrap();
            assert_eq!(input.username, RedisJsonValue::String("testuser".into()));
            assert_eq!(input.command, RedisJsonValue::String("SET".into()));
            assert_eq!(input.args.unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_no_command_args() {
            let args = vec![RedisJsonValue::String("testuser".into()), RedisJsonValue::String("PING".into())];
            let input = AclDryrunInput::decode(args).unwrap();
            assert!(input.args.is_none());
        }

        #[test]
        fn test_decode_input_too_few_args_fails() {
            let args = vec![RedisJsonValue::String("testuser".into())];
            let err = AclDryrunInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclDryrunInput {
                username: RedisJsonValue::String("default".into()),
                command: RedisJsonValue::String("GET".into()),
                args: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclDryrunInput {
                username: RedisJsonValue::String("default".into()),
                command: RedisJsonValue::String("GET".into()),
                args: None,
            };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclDryrun);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::acl::AclSetuserInput;
        use crate::api::wrapper::RuleWrapper;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_dryrun_default_user_allowed() {
            // ACL DRYRUN was added in Redis 7.0
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclDryrunInput {
                                username: RedisJsonValue::String("default".into()),
                                command: RedisJsonValue::String("GET".into()),
                                args: Some(vec![RedisJsonValue::String("anykey".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclDryrunOutput::decode(&result).expect("decode failed");
                    assert!(output.is_allowed(), "default user should be able to GET");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_dryrun_restricted_user() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Create a user with only read permissions
                    ctx.raw(
                        &AclSetuserInput {
                            username: RedisJsonValue::String("readonlyuser".into()),
                            rules: vec![
                                RuleWrapper::try_from(RedisJsonValue::String("on".into())).unwrap(),
                                RuleWrapper::try_from(RedisJsonValue::String(">password".into())).unwrap(),
                                RuleWrapper::try_from(RedisJsonValue::String("+get".into())).unwrap(),
                                RuleWrapper::try_from(RedisJsonValue::String("~*".into())).unwrap(),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("create user failed");

                    // Test GET should be allowed
                    let get_result = ctx
                        .raw(
                            &AclDryrunInput {
                                username: RedisJsonValue::String("readonlyuser".into()),
                                command: RedisJsonValue::String("GET".into()),
                                args: Some(vec![RedisJsonValue::String("key".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let get_output = AclDryrunOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.is_allowed(), "readonly user should be able to GET");

                    // Test SET should NOT be allowed
                    let set_result = ctx
                        .raw(
                            &AclDryrunInput {
                                username: RedisJsonValue::String("readonlyuser".into()),
                                command: RedisJsonValue::String("SET".into()),
                                args: Some(vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("value".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let set_output = AclDryrunOutput::decode(&set_result).expect("decode failed");
                    assert!(!set_output.is_allowed(), "readonly user should NOT be able to SET");

                    // Cleanup
                    ctx.raw(b"*2\r\n$11\r\nACL DELUSER\r\n$12\r\nreadonlyuser\r\n").await.expect("cleanup failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_dryrun_nonexistent_user() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclDryrunInput {
                                username: RedisJsonValue::String("nonexistent_user_xyz".into()),
                                command: RedisJsonValue::String("GET".into()),
                                args: Some(vec![RedisJsonValue::String("key".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should return an error for nonexistent user
                    let err = AclDryrunOutput::decode(&result);
                    assert!(err.is_err(), "should error for nonexistent user");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_dryrun_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &AclDryrunInput {
                        username: RedisJsonValue::String("default".into()),
                        command: RedisJsonValue::String("PING".into()),
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = AclDryrunOutput::decode(&result).expect("decode failed");
            assert!(output.is_allowed());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_dryrun_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &AclDryrunInput {
                        username: RedisJsonValue::String("default".into()),
                        command: RedisJsonValue::String("PING".into()),
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = AclDryrunOutput::decode(&result).expect("decode failed");
            assert!(output.is_allowed());

            ctx.stop().await;
        }
    }
}
