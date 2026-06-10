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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterSetConfigEpochInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterSetConfigEpoch,
    "Sets the configuration epoch for a new node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER SET-CONFIG-EPOCH`
/// https://redis.io/docs/latest/commands/cluster-set-config-epoch/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct ClusterSetConfigEpochInput {
    /// The configuration epoch value (must be a non-negative integer)
    config_epoch: RedisJsonValue,
}

impl Serialize for ClusterSetConfigEpochInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSetConfigEpochInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("config_epoch", &self.config_epoch)?;
        state.end()
    }
}

impl_redis_operation!(ClusterSetConfigEpochInput, API_INFO, { config_epoch });

impl RedisCommandInput for ClusterSetConfigEpochInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.config_epoch);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLUSTER SET-CONFIG-EPOCH requires 1 argument, given 0"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER SET-CONFIG-EPOCH expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        // Validate that config_epoch is a valid non-negative integer
        let epoch_value = match &args[0] {
            RedisJsonValue::Integer(i) => {
                if *i < 0 {
                    return Err(EpError::request("CLUSTER SET-CONFIG-EPOCH config_epoch must be non-negative"));
                }
                *i
            }
            RedisJsonValue::String(s) => {
                let parsed: i64 =
                    s.parse().map_err(|_| EpError::request("CLUSTER SET-CONFIG-EPOCH config_epoch must be a valid integer"))?;
                if parsed < 0 {
                    return Err(EpError::request("CLUSTER SET-CONFIG-EPOCH config_epoch must be non-negative"));
                }
                parsed
            }
            _ => {
                return Err(EpError::request("CLUSTER SET-CONFIG-EPOCH config_epoch must be an integer"));
            }
        };

        Ok(Self { config_epoch: RedisJsonValue::Integer(epoch_value) })
    }
}

/// Output for Redis CLUSTER SET-CONFIG-EPOCH command
///
/// Returns OK on success. An error is returned if the epoch is invalid
/// or if the command is used on a node that is not in a fresh state.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterSetConfigEpochOutput {
    /// Always "OK" on success
    status: String,
}

impl ClusterSetConfigEpochOutput {
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

impl Default for ClusterSetConfigEpochOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterSetConfigEpochOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSetConfigEpochOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterSetConfigEpochOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterSetConfigEpoch
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
                other => Err(EpError::parse(format!("unexpected CLUSTER SET-CONFIG-EPOCH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER SET-CONFIG-EPOCH response: {:?}", other))),
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
            let input = ClusterSetConfigEpochInput { config_epoch: RedisJsonValue::Integer(42) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("SET-CONFIG-EPOCH"));
            assert!(cmd_str.contains("42"));
        }

        #[test]
        fn test_encode_command_zero_epoch() {
            let input = ClusterSetConfigEpochInput { config_epoch: RedisJsonValue::Integer(0) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SET-CONFIG-EPOCH"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterSetConfigEpochOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterSetConfigEpochOutput::decode(
                b"-ERR The user can assign a config epoch only when the node does not know any other node\r\n",
            )
            .unwrap_err();
            assert!(err.to_string().contains("config epoch"));
        }

        #[test]
        fn test_decode_input_integer() {
            let args = vec![RedisJsonValue::Integer(100)];
            let input = ClusterSetConfigEpochInput::decode(args).unwrap();
            assert_eq!(input.config_epoch, RedisJsonValue::Integer(100));
        }

        #[test]
        fn test_decode_input_string_integer() {
            let args = vec![RedisJsonValue::String("200".into())];
            let input = ClusterSetConfigEpochInput::decode(args).unwrap();
            assert_eq!(input.config_epoch, RedisJsonValue::Integer(200));
        }

        #[test]
        fn test_decode_input_zero() {
            let args = vec![RedisJsonValue::Integer(0)];
            let input = ClusterSetConfigEpochInput::decode(args).unwrap();
            assert_eq!(input.config_epoch, RedisJsonValue::Integer(0));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterSetConfigEpochInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_negative_fails() {
            let args = vec![RedisJsonValue::Integer(-1)];
            let err = ClusterSetConfigEpochInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_negative_string_fails() {
            let args = vec![RedisJsonValue::String("-5".into())];
            let err = ClusterSetConfigEpochInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_invalid_string_fails() {
            let args = vec![RedisJsonValue::String("not_a_number".into())];
            let err = ClusterSetConfigEpochInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("valid integer"));
        }

        #[test]
        fn test_decode_input_wrong_type_fails() {
            let args = vec![RedisJsonValue::Array(vec![])];
            let err = ClusterSetConfigEpochInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("must be an integer"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterSetConfigEpochInput { config_epoch: RedisJsonValue::Integer(1) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization() {
            let input = ClusterSetConfigEpochInput { config_epoch: RedisJsonValue::Integer(42) };
            let json = serde_json::to_value(&input).unwrap();
            assert!(json.get("type").is_some());
            assert_eq!(json.get("config_epoch").unwrap(), 42);
        }

        #[test]
        fn test_output_serialization() {
            let output = ClusterSetConfigEpochOutput::new();
            let json = serde_json::to_value(&output).unwrap();
            assert_eq!(json.get("status").unwrap(), "OK");
        }
    }
}
