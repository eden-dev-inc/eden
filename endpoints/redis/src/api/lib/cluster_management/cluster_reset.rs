use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
use crate::api::{Reset, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterResetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterReset, "Resets a node", ReqType::Write, true);

/// See official Redis documentation for `CLUSTER RESET`
/// https://redis.io/docs/latest/commands/cluster-reset/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct ClusterResetInput {
    /// Reset mode: HARD or SOFT (default: SOFT)
    #[serde(skip_serializing_if = "Option::is_none")]
    reset: Option<Reset>,
}

impl Serialize for ClusterResetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.reset.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ClusterResetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(reset) = &self.reset {
            state.serialize_field("reset", reset)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClusterResetInput, API_INFO, { reset });

impl RedisCommandInput for ClusterResetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(reset) = &self.reset {
            match reset {
                Reset::HARD => command.arg("HARD"),
                Reset::SOFT => command.arg("SOFT"),
            };
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER RESET expects at most 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let reset = if !args.is_empty() {
            match &args[0] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "HARD" => Some(Reset::HARD),
                    "SOFT" => Some(Reset::SOFT),
                    other => {
                        return Err(EpError::request(format!("CLUSTER RESET invalid option '{}', expected HARD or SOFT", other)));
                    }
                },
                _ => {
                    return Err(EpError::parse("CLUSTER RESET option must be a string (HARD or SOFT)"));
                }
            }
        } else {
            None
        };

        Ok(Self { reset })
    }
}

/// Output for Redis CLUSTER RESET command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterResetOutput {
    /// Always "OK" on success
    status: String,
}

impl ClusterResetOutput {
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

impl Default for ClusterResetOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterResetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterResetOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterResetOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterReset
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
                other => Err(EpError::parse(format!("unexpected CLUSTER RESET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER RESET response: {:?}", other))),
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
        fn test_encode_command_no_args() {
            let input = ClusterResetInput { reset: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("RESET"));
            assert!(!cmd_str.contains("HARD"));
            assert!(!cmd_str.contains("SOFT"));
        }

        #[test]
        fn test_encode_command_hard() {
            let input = ClusterResetInput { reset: Some(Reset::HARD) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("RESET"));
            assert!(cmd_str.contains("HARD"));
        }

        #[test]
        fn test_encode_command_soft() {
            let input = ClusterResetInput { reset: Some(Reset::SOFT) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("RESET"));
            assert!(cmd_str.contains("SOFT"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterResetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterResetOutput::decode(b"-ERR cluster not enabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster not enabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterResetInput::decode(args).unwrap();
            assert!(input.reset.is_none());
        }

        #[test]
        fn test_decode_input_hard() {
            let args = vec![RedisJsonValue::String("HARD".into())];
            let input = ClusterResetInput::decode(args).unwrap();
            assert_eq!(input.reset, Some(Reset::HARD));
        }

        #[test]
        fn test_decode_input_soft() {
            let args = vec![RedisJsonValue::String("SOFT".into())];
            let input = ClusterResetInput::decode(args).unwrap();
            assert_eq!(input.reset, Some(Reset::SOFT));
        }

        #[test]
        fn test_decode_input_case_insensitive() {
            let args = vec![RedisJsonValue::String("hard".into())];
            let input = ClusterResetInput::decode(args).unwrap();
            assert_eq!(input.reset, Some(Reset::HARD));
        }

        #[test]
        fn test_decode_input_invalid_option_fails() {
            let args = vec![RedisJsonValue::String("INVALID".into())];
            let err = ClusterResetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("invalid option"));
        }

        #[test]
        fn test_decode_input_non_string_fails() {
            let args = vec![RedisJsonValue::Integer(123)];
            let err = ClusterResetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("must be a string"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterResetInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization_with_reset() {
            let input = ClusterResetInput { reset: Some(Reset::HARD) };
            let json = serde_json::to_value(&input).unwrap();
            assert!(json.get("type").is_some());
            assert_eq!(json.get("reset").unwrap(), "HARD");
        }

        #[test]
        fn test_serialization_without_reset() {
            let input = ClusterResetInput { reset: None };
            let json = serde_json::to_value(&input).unwrap();
            assert!(json.get("type").is_some());
            assert!(json.get("reset").is_none());
        }

        #[test]
        fn test_default() {
            let input = ClusterResetInput::default();
            assert!(input.reset.is_none());
        }
    }
}
