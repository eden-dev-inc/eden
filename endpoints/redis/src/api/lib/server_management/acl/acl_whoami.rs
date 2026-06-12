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

const API_INFO: ApiInfo<RedisApi, AclWhoamiInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclWhoami,
    "Returns the authenticated username of current connection",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL WHOAMI`
/// https://redis.io/docs/latest/commands/acl-whoami/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclWhoamiInput {}

impl Serialize for AclWhoamiInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclWhoamiInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AclWhoamiInput, API_INFO);

impl RedisCommandInput for AclWhoamiInput {
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
                "ACL WHOAMI expected no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis ACL WHOAMI command
///
/// Returns the username of the current connection.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclWhoamiOutput {
    /// The username of the current connection
    username: String,
}

impl AclWhoamiOutput {
    pub fn new(username: String) -> Self {
        Self { username }
    }

    /// Get the username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Check if the current user is the default user
    pub fn is_default(&self) -> bool {
        self.username == "default"
    }

    /// Decode the Redis protocol response into an AclWhoamiOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let username = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL WHOAMI response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL WHOAMI response: {:?}", other)));
                }
            },
        };

        Ok(Self { username })
    }
}

impl Serialize for AclWhoamiOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclWhoamiOutput", 1)?;
        state.serialize_field("username", &self.username)?;
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
            let input = AclWhoamiInput {};
            // ACL WHOAMI splits into: ACL, WHOAMI
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = AclWhoamiOutput::decode(b"$7\r\ndefault\r\n").unwrap();
            assert_eq!(output.username(), "default");
            assert!(output.is_default());
        }

        #[test]
        fn test_decode_simple_string() {
            let output = AclWhoamiOutput::decode(b"+default\r\n").unwrap();
            assert_eq!(output.username(), "default");
        }

        #[test]
        fn test_decode_custom_user() {
            let output = AclWhoamiOutput::decode(b"$5\r\nadmin\r\n").unwrap();
            assert_eq!(output.username(), "admin");
            assert!(!output.is_default());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclWhoamiOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = AclWhoamiInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclWhoamiInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclWhoamiInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclWhoami);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_whoami_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclWhoamiInput {}.command()).await.expect("raw failed");

                    let output = AclWhoamiOutput::decode(&result).expect("decode failed");
                    // Without authentication, should be default user
                    assert_eq!(output.username(), "default");
                    assert!(output.is_default());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_whoami_pipeline() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Pipeline multiple WHOAMI calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&AclWhoamiInput {}.command());
                    pipeline.extend_from_slice(&AclWhoamiInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = crate::protocol::RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    for resp in responses {
                        let output = AclWhoamiOutput::decode(resp).expect("decode failed");
                        assert_eq!(output.username(), "default");
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_whoami_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclWhoamiInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = AclWhoamiOutput::decode(&result).expect("decode failed");
            assert_eq!(output.username(), "default");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_whoami_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclWhoamiInput {}.command()).await.expect("raw failed");

            let output = AclWhoamiOutput::decode(&result).expect("decode failed");
            assert_eq!(output.username(), "default");

            ctx.stop().await;
        }
    }
}
