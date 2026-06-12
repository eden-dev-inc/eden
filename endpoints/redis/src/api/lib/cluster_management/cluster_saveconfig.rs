use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
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

const API_INFO: ApiInfo<RedisApi, ClusterSaveconfigInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterSaveconfig,
    "Forces a node to save the cluster configuration to disk",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER SAVECONFIG`
/// https://redis.io/docs/latest/commands/cluster-saveconfig/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterSaveconfigInput {}

impl Serialize for ClusterSaveconfigInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSaveconfigInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterSaveconfigInput, API_INFO);

impl RedisCommandInput for ClusterSaveconfigInput {
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
                "CLUSTER SAVECONFIG expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER SAVECONFIG command
///
/// Returns OK on success. An error is returned if the configuration
/// cannot be saved to disk.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterSaveconfigOutput {
    /// Always "OK" on success
    status: String,
}

impl ClusterSaveconfigOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }
}

impl Default for ClusterSaveconfigOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterSaveconfigOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSaveconfigOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterSaveconfigOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterSaveconfig
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CLUSTER SAVECONFIG response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER SAVECONFIG response: {:?}", other))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterSaveconfigInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("SAVECONFIG"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterSaveconfigOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterSaveconfigOutput::decode(b"-ERR cluster not enabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster not enabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterSaveconfigInput::decode(args).unwrap();
            // Successfully decoded with no args
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns_but_succeeds() {
            // Extra args should log a warning but still succeed
            let args = vec![RedisJsonValue::String("unexpected".into())];
            let input = ClusterSaveconfigInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterSaveconfigInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization() {
            let input = ClusterSaveconfigInput {};
            let json = serde_json::to_value(&input).unwrap();
            assert!(json.get("type").is_some());
        }

        #[test]
        fn test_default() {
            let input = ClusterSaveconfigInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_serialization() {
            let output = ClusterSaveconfigOutput::new();
            let json = serde_json::to_value(&output).unwrap();
            assert_eq!(json.get("status").unwrap(), "OK");
        }
    }
}
