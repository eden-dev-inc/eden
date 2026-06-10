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

const API_INFO: ApiInfo<RedisApi, AclLoadInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclLoad,
    "Reloads the ACL rules from the configured ACL file",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ACL LOAD`
/// https://redis.io/docs/latest/commands/acl-load/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclLoadInput {}

impl Serialize for AclLoadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclLoadInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AclLoadInput, API_INFO);

impl RedisCommandInput for AclLoadInput {
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
                "ACL LOAD expected no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis ACL LOAD command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclLoadOutput {
    /// Whether the load was successful
    success: bool,
}

impl AclLoadOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the load was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an AclLoadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(bytes) => {
                    let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp2Frame::BulkString(bytes) => {
                    let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected ACL LOAD response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp3Frame::BlobString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected ACL LOAD response: {:?}", other))),
            },
        }
    }
}

impl Serialize for AclLoadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclLoadOutput", 1)?;
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
        fn test_encode_command() {
            let input = AclLoadInput {};
            // ACL LOAD splits into: ACL, LOAD
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$4\r\nLOAD\r\n");
        }

        #[test]
        fn test_decode_ok_simple_string() {
            let output = AclLoadOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_ok_bulk_string() {
            let output = AclLoadOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclLoadOutput::decode(b"-ERR This Redis instance is not configured to use an ACL file\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = AclLoadInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclLoadInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclLoadInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclLoad);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: ACL LOAD requires an ACL file to be configured.
        // In default test setup without an ACL file, it will return an error.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_load_no_file_configured() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclLoadInput {}.command()).await.expect("raw failed");

                    // Without an ACL file configured, this should error
                    let output = AclLoadOutput::decode(&result);
                    assert!(output.is_err(), "ACL LOAD should fail without ACL file configured");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_load_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclLoadInput {}.command()).await.expect("raw failed");

            // Should be an error response
            assert!(result.starts_with(b"-"), "RESP2 should return error");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_load_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclLoadInput {}.command()).await.expect("raw failed");

            // Should fail to decode as success
            let output = AclLoadOutput::decode(&result);
            assert!(output.is_err());

            ctx.stop().await;
        }
    }
}
