use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
use crate::api::{Slot, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterDelslotsrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterDelslotsrange,
    "Sets hash slot ranges as unbound for a node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER DELSLOTSRANGE`
/// https://redis.io/docs/latest/commands/cluster-delslotsrange/
///
/// Official example: `CLUSTER DELSLOTSRANGE 1 5 10 20`
///
/// Available since Redis 7.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterDelslotsrangeInput {
    slots: Vec<Slot>,
}

impl Serialize for ClusterDelslotsrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterDelslotsrangeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl_redis_operation!(ClusterDelslotsrangeInput, API_INFO, { slots });

impl RedisCommandInput for ClusterDelslotsrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        for slot in &self.slots {
            command.arg(&slot.start).arg(&slot.end);
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("CLUSTER DELSLOTSRANGE requires at least 2 arguments, given none"));
        }

        if !args.len().is_multiple_of(2) {
            return Err(EpError::parse(format!(
                "CLUSTER DELSLOTSRANGE requires pairs of start/end values, given {} arguments",
                args.len()
            )));
        }

        let mut slots = Vec::with_capacity(args.len() / 2);
        for chunk in args.chunks(2) {
            slots.push(Slot { start: chunk[0].clone(), end: chunk[1].clone() });
        }

        Ok(Self { slots })
    }
}

/// Output for Redis CLUSTER DELSLOTSRANGE command
///
/// Returns OK on success, or an error if the slot ranges couldn't be deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterDelslotsrangeOutput {
    /// Status message (typically "OK")
    status: String,
}

impl ClusterDelslotsrangeOutput {
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

impl Default for ClusterDelslotsrangeOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterDelslotsrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterDelslotsrangeOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterDelslotsrangeOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterDelslotsrange
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
                other => Err(EpError::parse(format!("unexpected CLUSTER DELSLOTSRANGE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER DELSLOTSRANGE response: {:?}", other))),
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
        fn test_encode_command_single_range() {
            let input = ClusterDelslotsrangeInput {
                slots: vec![Slot {
                    start: RedisJsonValue::Integer(1),
                    end: RedisJsonValue::Integer(5),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("DELSLOTSRANGE"));
            assert!(cmd_str.contains("1"));
            assert!(cmd_str.contains("5"));
        }

        #[test]
        fn test_encode_command_multiple_ranges() {
            let input = ClusterDelslotsrangeInput {
                slots: vec![
                    Slot {
                        start: RedisJsonValue::Integer(1),
                        end: RedisJsonValue::Integer(5),
                    },
                    Slot {
                        start: RedisJsonValue::Integer(10),
                        end: RedisJsonValue::Integer(20),
                    },
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("1"));
            assert!(cmd_str.contains("5"));
            assert!(cmd_str.contains("10"));
            assert!(cmd_str.contains("20"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterDelslotsrangeInput {
                slots: vec![Slot {
                    start: RedisJsonValue::Integer(0),
                    end: RedisJsonValue::Integer(10),
                }],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_single_range() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(5)];
            let input = ClusterDelslotsrangeInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 1);
            assert_eq!(input.slots[0].start, RedisJsonValue::Integer(1));
            assert_eq!(input.slots[0].end, RedisJsonValue::Integer(5));
        }

        #[test]
        fn test_decode_input_multiple_ranges() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(5),
                RedisJsonValue::Integer(10),
                RedisJsonValue::Integer(20),
            ];
            let input = ClusterDelslotsrangeInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 2);
            assert_eq!(input.slots[0].start, RedisJsonValue::Integer(1));
            assert_eq!(input.slots[0].end, RedisJsonValue::Integer(5));
            assert_eq!(input.slots[1].start, RedisJsonValue::Integer(10));
            assert_eq!(input.slots[1].end, RedisJsonValue::Integer(20));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterDelslotsrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_odd_args_fails() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(5), RedisJsonValue::Integer(10)];
            let err = ClusterDelslotsrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("pairs"));
        }

        #[test]
        fn test_decode_input_single_arg_fails() {
            let args = vec![RedisJsonValue::Integer(1)];
            let err = ClusterDelslotsrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("pairs"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = ClusterDelslotsrangeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = ClusterDelslotsrangeOutput::decode(b"-ERR Invalid slot range\r\n").unwrap_err();
            assert!(err.to_string().contains("Invalid slot range"));
        }

        #[test]
        fn test_default_output() {
            let output = ClusterDelslotsrangeOutput::default();
            assert!(output.is_ok());
        }

        #[test]
        fn test_slot_accessors() {
            let slot = Slot::new(RedisJsonValue::Integer(100), RedisJsonValue::Integer(200));
            assert_eq!(slot.start(), &RedisJsonValue::Integer(100));
            assert_eq!(slot.end(), &RedisJsonValue::Integer(200));
        }
    }
}
