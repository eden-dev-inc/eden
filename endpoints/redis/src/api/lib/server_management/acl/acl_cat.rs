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

const API_INFO: ApiInfo<RedisApi, AclCatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclCat,
    "Lists the ACL categories, or the commands inside a category",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL CAT`
/// https://redis.io/docs/latest/commands/acl-cat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclCatInput {
    pub category: Option<RedisJsonValue>,
}

impl Serialize for AclCatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;

        if self.category.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("AclCatInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(category) = &self.category {
            state.serialize_field("category", category)?;
        }
        state.end()
    }
}

impl_redis_operation!(AclCatInput, API_INFO, { category });

impl RedisCommandInput for AclCatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(category) = &self.category {
            command.arg(category);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError> {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "ACL CAT expected 0 or 1 argument, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self { category: args.first().cloned() })
    }
}

/// Output for Redis ACL CAT command
///
/// Returns a list of ACL categories or commands within a category.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclCatOutput {
    /// List of categories or commands
    items: Vec<String>,
}

impl AclCatOutput {
    pub fn new(items: Vec<String>) -> Self {
        Self { items }
    }

    /// Get the list of categories or commands
    pub fn items(&self) -> &[String] {
        &self.items
    }

    /// Get the number of items returned
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Decode the Redis protocol response into an AclCatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let items = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut items = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(bytes) => {
                                items.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            Resp2Frame::SimpleString(bytes) => {
                                items.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL CAT response: {:?}", other)));
                            }
                        }
                    }
                    items
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL CAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut items = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                items.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                items.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL CAT response: {:?}", other)));
                            }
                        }
                    }
                    items
                }
                Resp3Frame::Set { data, .. } => {
                    let mut items = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                items.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                items.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in ACL CAT response: {:?}", other)));
                            }
                        }
                    }
                    items
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL CAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { items })
    }
}

impl Serialize for AclCatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclCatOutput", 1)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_category() {
            let input = AclCatInput { category: None };
            // ACL CAT splits into two arguments: ACL and CAT
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n");
        }

        #[test]
        fn test_encode_command_with_category() {
            let input = AclCatInput { category: Some(RedisJsonValue::String("read".into())) };
            // ACL CAT splits into: ACL, CAT, read
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$3\r\nCAT\r\n$4\r\nread\r\n");
        }

        #[test]
        fn test_decode_array_response() {
            let output = AclCatOutput::decode(b"*3\r\n$4\r\nread\r\n$5\r\nwrite\r\n$4\r\nfast\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.items(), &["read", "write", "fast"]);
        }

        #[test]
        fn test_decode_empty_array() {
            let output = AclCatOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclCatOutput::decode(b"-ERR unknown category\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = AclCatInput::decode(vec![]).unwrap();
            assert!(input.category.is_none());
        }

        #[test]
        fn test_decode_input_with_category() {
            let args = vec![RedisJsonValue::String("dangerous".into())];
            let input = AclCatInput::decode(args).unwrap();
            assert_eq!(input.category, Some(RedisJsonValue::String("dangerous".into())));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclCatInput { category: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclCatInput { category: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclCat);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_cat_list_categories() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclCatInput { category: None }.command()).await.expect("raw failed");

                    let output = AclCatOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "should return categories");

                    // Check for some expected categories
                    let items = output.items();
                    assert!(items.iter().any(|c| c == "read" || c == "@read"), "should contain read category");
                    assert!(items.iter().any(|c| c == "write" || c == "@write"), "should contain write category");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_cat_list_commands_in_category() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&AclCatInput { category: Some(RedisJsonValue::String("read".into())) }.command())
                        .await
                        .expect("raw failed");

                    let output = AclCatOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "should return commands in read category");

                    // GET should be in the read category
                    let items = output.items();
                    assert!(items.iter().any(|c| c.to_lowercase().contains("get")), "read category should contain get command");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_cat_invalid_category() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclCatInput {
                                category: Some(RedisJsonValue::String("nonexistent_category".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = AclCatOutput::decode(&result);
                    assert!(err.is_err(), "invalid category should return error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_cat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclCatInput { category: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = AclCatOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_cat_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclCatInput { category: None }.command()).await.expect("raw failed");

            let output = AclCatOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }
    }
}
