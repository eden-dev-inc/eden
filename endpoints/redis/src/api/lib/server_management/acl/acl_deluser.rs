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

const API_INFO: ApiInfo<RedisApi, AclDeluserInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclDeluser,
    "Deletes ACL users, and terminates their connections",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ACL DELUSER`
/// https://redis.io/docs/latest/commands/acl-deluser/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclDeluserInput {
    pub usernames: Vec<RedisJsonValue>,
}

impl Serialize for AclDeluserInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclDeluserInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("usernames", &self.usernames)?;
        state.end()
    }
}

impl_redis_operation!(AclDeluserInput, API_INFO, { usernames });

impl RedisCommandInput for AclDeluserInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        for username in &self.usernames {
            command.arg(username);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("ACL DELUSER requires at least one username"));
        }

        Ok(Self { usernames: args })
    }
}

/// Output for Redis ACL DELUSER command
///
/// Returns the number of users that were deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclDeluserOutput {
    /// The number of users that were deleted
    deleted: i64,
}

impl AclDeluserOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of deleted users
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into an AclDeluserOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL DELUSER response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL DELUSER response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for AclDeluserOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclDeluserOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_user() {
            let input = AclDeluserInput { usernames: vec![RedisJsonValue::String("testuser".into())] };
            // ACL DELUSER splits into: ACL, DELUSER, testuser
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$8\r\ntestuser\r\n");
        }

        #[test]
        fn test_encode_command_multiple_users() {
            let input = AclDeluserInput {
                usernames: vec![RedisJsonValue::String("user1".into()), RedisJsonValue::String("user2".into())],
            };
            // ACL DELUSER splits into: ACL, DELUSER, user1, user2
            assert_eq!(input.command().to_vec(), b"*4\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$5\r\nuser1\r\n$5\r\nuser2\r\n");
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = AclDeluserOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_integer_one() {
            let output = AclDeluserOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.deleted(), 1);
        }

        #[test]
        fn test_decode_integer_multiple() {
            let output = AclDeluserOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.deleted(), 3);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclDeluserOutput::decode(b"-ERR cannot delete default user\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_single_user() {
            let args = vec![RedisJsonValue::String("myuser".into())];
            let input = AclDeluserInput::decode(args).unwrap();
            assert_eq!(input.usernames.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_users() {
            let args = vec![RedisJsonValue::String("user1".into()), RedisJsonValue::String("user2".into())];
            let input = AclDeluserInput::decode(args).unwrap();
            assert_eq!(input.usernames.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = AclDeluserInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one username"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclDeluserInput { usernames: vec![RedisJsonValue::String("user".into())] };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclDeluserInput { usernames: vec![RedisJsonValue::String("user".into())] };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclDeluser);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::acl::{AclSetuserInput, AclUsersInput, AclUsersOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_nonexistent() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclDeluserInput {
                                usernames: vec![RedisJsonValue::String("nonexistent_user_12345".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclDeluserOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0, "nonexistent user should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_after_create() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Create a user first
                    ctx.raw(
                        &AclSetuserInput {
                            username: RedisJsonValue::String("tempuser".into()),
                            rules: vec![],
                        }
                        .command(),
                    )
                    .await
                    .expect("create user failed");

                    // Delete the user
                    let result = ctx
                        .raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("tempuser".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = AclDeluserOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);

                    // Verify user is gone
                    let users_result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

                    let users_output = AclUsersOutput::decode(&users_result).expect("decode failed");
                    assert!(!users_output.users().contains(&"tempuser".to_string()), "user should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_cannot_delete_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("default".into())] }.command())
                        .await
                        .expect("raw failed");

                    // Attempting to delete 'default' user should return 0 or error
                    // depending on Redis version
                    let output = AclDeluserOutput::decode(&result);
                    if let Ok(out) = output {
                        assert_eq!(out.deleted(), 0, "cannot delete default user");
                    }
                    // If it errors, that's also acceptable
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_multiple_users() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Create multiple users
                    for i in 1..=3 {
                        ctx.raw(
                            &AclSetuserInput {
                                username: RedisJsonValue::String(format!("bulkuser{}", i)),
                                rules: vec![],
                            }
                            .command(),
                        )
                        .await
                        .expect("create user failed");
                    }

                    // Delete all at once
                    let result = ctx
                        .raw(
                            &AclDeluserInput {
                                usernames: vec![
                                    RedisJsonValue::String("bulkuser1".into()),
                                    RedisJsonValue::String("bulkuser2".into()),
                                    RedisJsonValue::String("bulkuser3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclDeluserOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("nobody".into())] }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = AclDeluserOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_deluser_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("nobody".into())] }.command())
                .await
                .expect("raw failed");

            let output = AclDeluserOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 0);

            ctx.stop().await;
        }
    }
}
