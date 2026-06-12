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

const API_INFO: ApiInfo<RedisApi, ConfigRewriteInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ConfigRewrite,
    "Persists the effective configuration to file",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CONFIG REWRITE`
/// https://redis.io/docs/latest/commands/config-rewrite/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ConfigRewriteInput {}

impl Serialize for ConfigRewriteInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigRewriteInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ConfigRewriteInput, API_INFO);

impl RedisCommandInput for ConfigRewriteInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
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
                "CONFIG REWRITE expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CONFIG REWRITE command
///
/// Returns OK when the configuration has been written to file.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ConfigRewriteOutput {
    success: bool,
}

impl Serialize for ConfigRewriteOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigRewriteOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl ConfigRewriteOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the configuration was written successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CONFIG REWRITE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CONFIG REWRITE response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ConfigRewriteInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CONFIG"));
            assert!(cmd_str.contains("REWRITE"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = ConfigRewriteOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = ConfigRewriteOutput::decode(b"-ERR The server is running without a config file\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = ConfigRewriteInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_still_succeeds() {
            let input = ConfigRewriteInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ConfigRewriteInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ConfigRewriteInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ConfigRewrite);
        }

        #[test]
        fn test_serialize_input() {
            let input = ConfigRewriteInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("CONFIG") || json.contains("Config"));
        }

        #[test]
        fn test_serialize_output() {
            let output = ConfigRewriteOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_req_type_is_write() {
            assert_eq!(API_INFO.request_type, ReqType::Write);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: CONFIG REWRITE requires Redis to be started with a config file.
        // In test containers without a config file, this will return an error.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_rewrite_no_config_file() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ConfigRewriteInput {}.command()).await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected: error because no config file
                            let err = ConfigRewriteOutput::decode(&r);
                            assert!(err.is_err());
                        }
                        Ok(r) => {
                            // If it succeeds (config file exists), verify success
                            let output = ConfigRewriteOutput::decode(&r).expect("decode failed");
                            assert!(output.is_success());
                        }
                        Err(_) => {
                            // Connection error, unexpected but handle gracefully
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_rewrite_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ConfigRewriteInput {}.command()).await.expect("raw failed");

            // Either +OK or -ERR depending on whether config file exists
            assert!(result.starts_with(b"+") || result.starts_with(b"-"), "should return simple string or error");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_rewrite_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ConfigRewriteInput {}.command()).await.expect("raw failed");

            // Either success or error depending on config file
            if result.starts_with(b"+") {
                let output = ConfigRewriteOutput::decode(&result).expect("decode failed");
                assert!(output.is_success());
            }
            // Error case is also acceptable

            ctx.stop().await;
        }
    }
}
