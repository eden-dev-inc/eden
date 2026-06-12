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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, AclListInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclList,
    "Dumps the effective rules in ACL file format",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL LIST`
/// https://redis.io/docs/latest/commands/acl-list/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclListInput {}

impl Serialize for AclListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclListInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AclListInput, API_INFO);

impl RedisCommandInput for AclListInput {
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
                "ACL LIST expected no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis ACL LIST command
///
/// Returns a list of ACL rules in the format used by the ACL file.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclListOutput {
    /// List of ACL rules for each user
    rules: Vec<String>,
}

impl AclListOutput {
    pub fn new(rules: Vec<String>) -> Self {
        Self { rules }
    }

    /// Get the list of ACL rules
    pub fn rules(&self) -> &[String] {
        &self.rules
    }

    /// Get the number of users
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if there are no rules
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Decode the Redis protocol response into an AclListOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let rules = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut rules = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => {
                                rules.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL LIST response: {:?}", other)));
                            }
                        }
                    }
                    rules
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL LIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut rules = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                rules.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL LIST response: {:?}", other)));
                            }
                        }
                    }
                    rules
                }
                Resp3Frame::Set { data, .. } => {
                    let mut rules = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                rules.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL LIST response: {:?}", other)));
                            }
                        }
                    }
                    rules
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL LIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { rules })
    }
}

impl Serialize for AclListOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclListOutput", 1)?;
        state.serialize_field("rules", &self.rules)?;
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
            let input = AclListInput {};
            // ACL LIST splits into: ACL, LIST
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n");
        }

        #[test]
        fn test_decode_array_response() {
            let output = AclListOutput::decode(b"*1\r\n$27\r\nuser default on nopass ~* +@all\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert!(output.rules()[0].contains("default"));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = AclListOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclListOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = AclListInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclListInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclListInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclList);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_list_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclListInput {}.command()).await.expect("raw failed");

                    let output = AclListOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "should have at least default user");

                    // Should contain the default user rule
                    let has_default = output.rules().iter().any(|r| r.contains("default"));
                    assert!(has_default, "should contain default user");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_list_format() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclListInput {}.command()).await.expect("raw failed");

                    let output = AclListOutput::decode(&result).expect("decode failed");

                    // Each rule should start with "user"
                    for rule in output.rules() {
                        assert!(rule.starts_with("user "), "rule should start with 'user ': {}", rule);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclListInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = AclListOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_list_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclListInput {}.command()).await.expect("raw failed");

            let output = AclListOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }
    }
}
