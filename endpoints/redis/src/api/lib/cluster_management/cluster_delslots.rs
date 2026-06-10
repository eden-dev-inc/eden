use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterDelslotsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterDelslots,
    "Sets hash slots as unbound for a node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER DELSLOTS`
/// https://redis.io/docs/latest/commands/cluster-delslots/
///
/// Official example: `CLUSTER DELSLOTS 5000 5001`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterDelslotsInput {
    slots: Vec<RedisJsonValue>,
}

impl Serialize for ClusterDelslotsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterDelslotsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl_redis_operation!(ClusterDelslotsInput, API_INFO, { slots });

impl RedisCommandInput for ClusterDelslotsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        for slot in &self.slots {
            command.arg(slot);
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("CLUSTER DELSLOTS requires at least 1 argument, given none"));
        }

        Ok(Self { slots: args })
    }
}

/// Output for Redis CLUSTER DELSLOTS command
///
/// Returns OK on success, or an error if the slots couldn't be deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterDelslotsOutput {
    /// Status message (typically "OK")
    status: String,
}

impl ClusterDelslotsOutput {
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

impl Default for ClusterDelslotsOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterDelslotsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterDelslotsOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterDelslotsOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterDelslots
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
                other => Err(EpError::parse(format!("unexpected CLUSTER DELSLOTS response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER DELSLOTS response: {:?}", other))),
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
        fn test_encode_command_single_slot() {
            let input = ClusterDelslotsInput { slots: vec![RedisJsonValue::Integer(5000)] };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("DELSLOTS"));
            assert!(cmd_str.contains("5000"));
        }

        #[test]
        fn test_encode_command_multiple_slots() {
            let input = ClusterDelslotsInput {
                slots: vec![
                    RedisJsonValue::Integer(5000),
                    RedisJsonValue::Integer(5001),
                    RedisJsonValue::Integer(5002),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("DELSLOTS"));
            assert!(cmd_str.contains("5000"));
            assert!(cmd_str.contains("5001"));
            assert!(cmd_str.contains("5002"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterDelslotsInput { slots: vec![RedisJsonValue::Integer(0)] };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_single_slot() {
            let args = vec![RedisJsonValue::Integer(5000)];
            let input = ClusterDelslotsInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 1);
            assert_eq!(input.slots[0], RedisJsonValue::Integer(5000));
        }

        #[test]
        fn test_decode_input_multiple_slots() {
            let args = vec![RedisJsonValue::Integer(5000), RedisJsonValue::Integer(5001)];
            let input = ClusterDelslotsInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterDelslotsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = ClusterDelslotsOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = ClusterDelslotsOutput::decode(b"-ERR Slot 5000 is already unbound\r\n").unwrap_err();
            assert!(err.to_string().contains("already unbound"));
        }

        #[test]
        fn test_default_output() {
            let output = ClusterDelslotsOutput::default();
            assert!(output.is_ok());
        }
    }
}
