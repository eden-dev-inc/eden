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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, AclUsersInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::AclUsers, "Lists all ACL users", ReqType::Read, true);

/// See official Redis documentation for `ACL USERS`
/// https://redis.io/docs/latest/commands/acl-users/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct AclUsersInput {}

impl Serialize for AclUsersInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclUsersInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AclUsersInput, API_INFO);

impl RedisCommandInput for AclUsersInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
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
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "ACL USERS expected no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis ACL USERS command
///
/// Returns a list of all ACL usernames.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclUsersOutput {
    /// List of usernames
    users: Vec<String>,
}

impl AclUsersOutput {
    pub fn new(users: Vec<String>) -> Self {
        Self { users }
    }

    /// Get the list of usernames
    pub fn users(&self) -> &[String] {
        &self.users
    }

    /// Get the number of users
    pub fn len(&self) -> usize {
        self.users.len()
    }

    /// Check if there are no users
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// Check if a specific user exists
    pub fn contains(&self, username: &str) -> bool {
        self.users.iter().any(|u| u == username)
    }

    /// Decode the Redis protocol response into an AclUsersOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let users = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut users = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => {
                                users.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL USERS response: {:?}", other)));
                            }
                        }
                    }
                    users
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL USERS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut users = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                users.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL USERS response: {:?}", other)));
                            }
                        }
                    }
                    users
                }
                Resp3Frame::Set { data, .. } => {
                    let mut users = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                users.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL USERS response: {:?}", other)));
                            }
                        }
                    }
                    users
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL USERS response: {:?}", other)));
                }
            },
        };

        Ok(Self { users })
    }
}

impl Serialize for AclUsersOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclUsersOutput", 1)?;
        state.serialize_field("users", &self.users)?;
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
            let input = AclUsersInput {};
            // ACL USERS splits into: ACL, USERS
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n");
        }

        #[test]
        fn test_decode_array_response() {
            let output = AclUsersOutput::decode(b"*2\r\n$7\r\ndefault\r\n$5\r\nadmin\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!(output.contains("default"));
            assert!(output.contains("admin"));
        }

        #[test]
        fn test_decode_single_user() {
            let output = AclUsersOutput::decode(b"*1\r\n$7\r\ndefault\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert!(output.contains("default"));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = AclUsersOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclUsersOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = AclUsersInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclUsersInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclUsersInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclUsers);
        }

        #[test]
        fn test_contains() {
            let output = AclUsersOutput::new(vec!["default".into(), "admin".into()]);
            assert!(output.contains("default"));
            assert!(output.contains("admin"));
            assert!(!output.contains("nobody"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::acl::{AclDeluserInput, AclSetuserInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_users_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

                    let output = AclUsersOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "should have at least default user");
                    assert!(output.contains("default"), "should contain default user");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_users_after_create() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Create a new user
                    ctx.raw(
                        &AclSetuserInput {
                            username: RedisJsonValue::String("listuser".into()),
                            rules: vec![],
                        }
                        .command(),
                    )
                    .await
                    .expect("create user failed");

                    let result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

                    let output = AclUsersOutput::decode(&result).expect("decode failed");
                    assert!(output.contains("listuser"), "should contain new user");

                    // Cleanup
                    ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("listuser".into())] }.command())
                        .await
                        .expect("cleanup failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_users_after_delete() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Create and then delete a user
                    ctx.raw(
                        &AclSetuserInput {
                            username: RedisJsonValue::String("tempuser".into()),
                            rules: vec![],
                        }
                        .command(),
                    )
                    .await
                    .expect("create user failed");

                    ctx.raw(&AclDeluserInput { usernames: vec![RedisJsonValue::String("tempuser".into())] }.command())
                        .await
                        .expect("delete user failed");

                    let result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

                    let output = AclUsersOutput::decode(&result).expect("decode failed");
                    assert!(!output.contains("tempuser"), "should not contain deleted user");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_users_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = AclUsersOutput::decode(&result).expect("decode failed");
            assert!(output.contains("default"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_users_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclUsersInput {}.command()).await.expect("raw failed");

            let output = AclUsersOutput::decode(&result).expect("decode failed");
            assert!(output.contains("default"));

            ctx.stop().await;
        }
    }
}
