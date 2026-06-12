use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{SetslotSubcommand, key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterSetslotInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterSetslot, "Binds a hash slot to a node", ReqType::Write, true);

/// See official Redis documentation for `CLUSTER SETSLOT`
/// https://redis.io/docs/latest/commands/cluster-setslot/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterSetslotInput {
    /// The hash slot number (0-16383)
    slot: RedisJsonValue,
    /// The subcommand specifying the operation
    subcommand: SetslotSubcommand,
}

impl Serialize for ClusterSetslotInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSetslotInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("subcommand", &self.subcommand)?;
        state.end()
    }
}

impl_redis_operation!(
    ClusterSetslotInput,
    API_INFO,
    { slot, subcommand }
);

impl RedisCommandInput for ClusterSetslotInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.slot);

        match &self.subcommand {
            SetslotSubcommand::Importing(node_id) => {
                command.arg("IMPORTING").arg(node_id);
            }
            SetslotSubcommand::Migrating(node_id) => {
                command.arg("MIGRATING").arg(node_id);
            }
            SetslotSubcommand::Node(node_id) => {
                command.arg("NODE").arg(node_id);
            }
            SetslotSubcommand::Stable => {
                command.arg("STABLE");
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("CLUSTER SETSLOT requires at least 2 arguments, given {}", args.len())));
        }

        let slot = args[0].clone();

        let subcommand = match &args[1] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "IMPORTING" => {
                    if args.len() < 3 {
                        return Err(EpError::request("CLUSTER SETSLOT IMPORTING requires node-id argument"));
                    }
                    SetslotSubcommand::Importing(args[2].clone())
                }
                "MIGRATING" => {
                    if args.len() < 3 {
                        return Err(EpError::request("CLUSTER SETSLOT MIGRATING requires node-id argument"));
                    }
                    SetslotSubcommand::Migrating(args[2].clone())
                }
                "NODE" => {
                    if args.len() < 3 {
                        return Err(EpError::request("CLUSTER SETSLOT NODE requires node-id argument"));
                    }
                    SetslotSubcommand::Node(args[2].clone())
                }
                "STABLE" => SetslotSubcommand::Stable,
                _ => {
                    return Err(EpError::request(format!("Unknown CLUSTER SETSLOT subcommand: {}", s)));
                }
            },
            _ => {
                return Err(EpError::request("CLUSTER SETSLOT subcommand must be a string"));
            }
        };

        Ok(Self { slot, subcommand })
    }
}

/// Output for Redis CLUSTER SETSLOT command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterSetslotOutput {
    status: String,
}

impl ClusterSetslotOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CLUSTER SETSLOT response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER SETSLOT response: {:?}", other))),
            },
        }
    }
}

impl Default for ClusterSetslotOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterSetslotOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSetslotOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_importing() {
            let input = ClusterSetslotInput {
                slot: RedisJsonValue::Integer(100),
                subcommand: SetslotSubcommand::Importing(RedisJsonValue::String("node123".to_string())),
            };
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("CLUSTER"));
            assert!(cmd.contains("SETSLOT"));
            assert!(cmd.contains("100"));
            assert!(cmd.contains("IMPORTING"));
            assert!(cmd.contains("node123"));
        }

        #[test]
        fn test_encode_command_migrating() {
            let input = ClusterSetslotInput {
                slot: RedisJsonValue::Integer(200),
                subcommand: SetslotSubcommand::Migrating(RedisJsonValue::String("node456".to_string())),
            };
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("MIGRATING"));
            assert!(cmd.contains("node456"));
        }

        #[test]
        fn test_encode_command_node() {
            let input = ClusterSetslotInput {
                slot: RedisJsonValue::Integer(300),
                subcommand: SetslotSubcommand::Node(RedisJsonValue::String("node789".to_string())),
            };
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("NODE"));
            assert!(cmd.contains("node789"));
        }

        #[test]
        fn test_encode_command_stable() {
            let input = ClusterSetslotInput {
                slot: RedisJsonValue::Integer(400),
                subcommand: SetslotSubcommand::Stable,
            };
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("STABLE"));
            assert!(!cmd.contains("IMPORTING"));
            assert!(!cmd.contains("MIGRATING"));
            assert!(!cmd.contains("NODE"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterSetslotOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterSetslotOutput::decode(b"-ERR Invalid slot\r\n").unwrap_err();
            assert!(err.to_string().contains("Invalid slot"));
        }

        #[test]
        fn test_decode_input_importing() {
            let args = vec![
                RedisJsonValue::Integer(100),
                RedisJsonValue::String("IMPORTING".to_string()),
                RedisJsonValue::String("node123".to_string()),
            ];
            let input = ClusterSetslotInput::decode(args).unwrap();
            assert!(matches!(input.subcommand, SetslotSubcommand::Importing(_)));
        }

        #[test]
        fn test_decode_input_stable() {
            let args = vec![RedisJsonValue::Integer(100), RedisJsonValue::String("STABLE".to_string())];
            let input = ClusterSetslotInput::decode(args).unwrap();
            assert!(matches!(input.subcommand, SetslotSubcommand::Stable));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(100)];
            let err = ClusterSetslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_importing_missing_node() {
            let args = vec![RedisJsonValue::Integer(100), RedisJsonValue::String("IMPORTING".to_string())];
            let err = ClusterSetslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("node-id"));
        }

        #[test]
        fn test_decode_input_unknown_subcommand() {
            let args = vec![RedisJsonValue::Integer(100), RedisJsonValue::String("UNKNOWN".to_string())];
            let err = ClusterSetslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterSetslotInput {
                slot: RedisJsonValue::Integer(100),
                subcommand: SetslotSubcommand::Stable,
            };
            assert!(input.keys().is_empty());
        }
    }
}
