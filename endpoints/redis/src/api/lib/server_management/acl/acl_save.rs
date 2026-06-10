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

const API_INFO: ApiInfo<RedisApi, AclSaveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclSave,
    "Saves the effective ACL rules in the configured ACL file",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ACL SAVE`
/// https://redis.io/docs/latest/commands/acl-save/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclSaveInput {}

impl Serialize for AclSaveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclSaveInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AclSaveInput, API_INFO);

impl RedisCommandInput for AclSaveInput {
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
                "ACL SAVE expected no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis ACL SAVE command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclSaveOutput {
    /// Whether the save was successful
    success: bool,
}

impl AclSaveOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the save was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an AclSaveOutput
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
                other => Err(EpError::parse(format!("unexpected ACL SAVE response: {:?}", other))),
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
                other => Err(EpError::parse(format!("unexpected ACL SAVE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for AclSaveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclSaveOutput", 1)?;
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
            let input = AclSaveInput {};
            // ACL SAVE splits into: ACL, SAVE
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$4\r\nSAVE\r\n");
        }

        #[test]
        fn test_decode_ok_simple_string() {
            let output = AclSaveOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_ok_bulk_string() {
            let output = AclSaveOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclSaveOutput::decode(b"-ERR This Redis instance is not configured to use an ACL file\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = AclSaveInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclSaveInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclSaveInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclSave);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: ACL SAVE requires an ACL file to be configured.
        // In default test setup without an ACL file, it will return an error.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_save_no_file_configured() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclSaveInput {}.command()).await.expect("raw failed");

                    // Without an ACL file configured, this should error
                    let output = AclSaveOutput::decode(&result);
                    assert!(output.is_err(), "ACL SAVE should fail without ACL file configured");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_save_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclSaveInput {}.command()).await.expect("raw failed");

            // Should be an error response
            assert!(result.starts_with(b"-"), "RESP2 should return error");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_save_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclSaveInput {}.command()).await.expect("raw failed");

            // Should fail to decode as success
            let output = AclSaveOutput::decode(&result);
            assert!(output.is_err());

            ctx.stop().await;
        }
    }
}
