use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ReplconfInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Replconf,
    "An internal command for configuring the replication stream",
    ReqType::Write,
    false,
);

/// Subcommands for REPLCONF
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum ReplconfSubcommand {
    /// Report the listening port
    ListeningPort(u16),
    /// Acknowledge processed replication offset
    Ack(i64),
    /// Report capabilities (eof, psync2)
    Capa(String),
    /// Get acknowledgment from master
    GetAck,
    /// Generic subcommand with arguments
    Generic { subcommand: String, args: Vec<RedisJsonValue> },
}

/// See official Redis documentation for `REPLCONF`
/// https://redis.io/docs/latest/commands/replconf/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ReplconfInput {
    #[builder(default)]
    subcommand: Option<ReplconfSubcommand>,
}

impl ReplconfInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn listening_port(port: u16) -> Self {
        Self { subcommand: Some(ReplconfSubcommand::ListeningPort(port)) }
    }

    pub fn ack(offset: i64) -> Self {
        Self { subcommand: Some(ReplconfSubcommand::Ack(offset)) }
    }

    pub fn capa(capability: impl Into<String>) -> Self {
        Self {
            subcommand: Some(ReplconfSubcommand::Capa(capability.into())),
        }
    }

    pub fn getack() -> Self {
        Self { subcommand: Some(ReplconfSubcommand::GetAck) }
    }

    pub fn subcommand(&self) -> Option<&ReplconfSubcommand> {
        self.subcommand.as_ref()
    }
}

impl Serialize for ReplconfInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if self.subcommand.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ReplconfInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(subcmd) = &self.subcommand {
            state.serialize_field("subcommand", subcmd)?;
        }
        state.end()
    }
}

impl_redis_operation!(ReplconfInput, API_INFO, { subcommand });

impl RedisCommandInput for ReplconfInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(subcmd) = &self.subcommand {
            match subcmd {
                ReplconfSubcommand::ListeningPort(port) => {
                    command.arg("LISTENING-PORT").arg(*port);
                }
                ReplconfSubcommand::Ack(offset) => {
                    command.arg("ACK").arg(*offset);
                }
                ReplconfSubcommand::Capa(capa) => {
                    command.arg("CAPA").arg(capa);
                }
                ReplconfSubcommand::GetAck => {
                    command.arg("GETACK").arg("*");
                }
                ReplconfSubcommand::Generic { subcommand, args } => {
                    command.arg(subcommand);
                    for arg in args {
                        command.arg(arg);
                    }
                }
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Ok(Self::default());
        }

        let subcmd_str = match &args[0] {
            RedisJsonValue::String(s) => s.to_uppercase(),
            _ => return Err(EpError::parse("REPLCONF subcommand must be a string")),
        };

        let subcommand = match subcmd_str.as_str() {
            "LISTENING-PORT" => {
                if args.len() < 2 {
                    return Err(EpError::parse("LISTENING-PORT requires a port argument"));
                }
                let port = parse_u16(&args[1])?;
                Some(ReplconfSubcommand::ListeningPort(port))
            }
            "ACK" => {
                if args.len() < 2 {
                    return Err(EpError::parse("ACK requires an offset argument"));
                }
                let offset = parse_i64(&args[1])?;
                Some(ReplconfSubcommand::Ack(offset))
            }
            "CAPA" => {
                if args.len() < 2 {
                    return Err(EpError::parse("CAPA requires a capability argument"));
                }
                let capa = match &args[1] {
                    RedisJsonValue::String(s) => s.clone(),
                    _ => return Err(EpError::parse("CAPA value must be a string")),
                };
                Some(ReplconfSubcommand::Capa(capa))
            }
            "GETACK" => Some(ReplconfSubcommand::GetAck),
            _ => Some(ReplconfSubcommand::Generic { subcommand: subcmd_str, args: args[1..].to_vec() }),
        };

        Ok(Self { subcommand })
    }
}

fn parse_u16(value: &RedisJsonValue) -> Result<u16, EpError> {
    match value {
        RedisJsonValue::Integer(i) => {
            if *i < 0 || *i > u16::MAX as i64 {
                return Err(EpError::parse("Port must be in range 0-65535"));
            }
            Ok(*i as u16)
        }
        RedisJsonValue::String(s) => s.parse::<u16>().map_err(|_| EpError::parse("Invalid port number")),
        _ => Err(EpError::parse("Port must be an integer")),
    }
}

fn parse_i64(value: &RedisJsonValue) -> Result<i64, EpError> {
    match value {
        RedisJsonValue::Integer(i) => Ok(*i),
        RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("Invalid integer")),
        _ => Err(EpError::parse("Value must be an integer")),
    }
}

/// Output for Redis REPLCONF command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ReplconfOutput {
    /// The response (typically "OK" or an error)
    message: String,
}

impl ReplconfOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.message == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected REPLCONF response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected REPLCONF response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for ReplconfOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ReplconfOutput", 1)?;
        state.serialize_field("message", &self.message)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_subcommand() {
            let input = ReplconfInput::new();
            assert_eq!(input.command().to_vec(), b"*1\r\n$8\r\nREPLCONF\r\n");
        }

        #[test]
        fn test_encode_command_listening_port() {
            let input = ReplconfInput::listening_port(6379);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLCONF"));
            assert!(cmd_str.contains("LISTENING-PORT"));
            assert!(cmd_str.contains("6379"));
        }

        #[test]
        fn test_encode_command_ack() {
            let input = ReplconfInput::ack(12345);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLCONF"));
            assert!(cmd_str.contains("ACK"));
            assert!(cmd_str.contains("12345"));
        }

        #[test]
        fn test_encode_command_capa() {
            let input = ReplconfInput::capa("psync2");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLCONF"));
            assert!(cmd_str.contains("CAPA"));
            assert!(cmd_str.contains("psync2"));
        }

        #[test]
        fn test_encode_command_getack() {
            let input = ReplconfInput::getack();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLCONF"));
            assert!(cmd_str.contains("GETACK"));
        }

        #[test]
        fn test_decode_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ReplconfInput::decode(args).unwrap();
            assert!(input.subcommand().is_none());
        }

        #[test]
        fn test_decode_listening_port() {
            let args = vec![RedisJsonValue::String("LISTENING-PORT".into()), RedisJsonValue::Integer(6379)];
            let input = ReplconfInput::decode(args).unwrap();
            assert!(matches!(input.subcommand(), Some(ReplconfSubcommand::ListeningPort(6379))));
        }

        #[test]
        fn test_decode_ack() {
            let args = vec![RedisJsonValue::String("ACK".into()), RedisJsonValue::Integer(12345)];
            let input = ReplconfInput::decode(args).unwrap();
            assert!(matches!(input.subcommand(), Some(ReplconfSubcommand::Ack(12345))));
        }

        #[test]
        fn test_decode_capa() {
            let args = vec![RedisJsonValue::String("capa".into()), RedisJsonValue::String("eof".into())];
            let input = ReplconfInput::decode(args).unwrap();
            match input.subcommand() {
                Some(ReplconfSubcommand::Capa(c)) => assert_eq!(c, "eof"),
                _ => panic!("Expected Capa subcommand"),
            }
        }

        #[test]
        fn test_decode_getack() {
            let args = vec![RedisJsonValue::String("GETACK".into())];
            let input = ReplconfInput::decode(args).unwrap();
            assert!(matches!(input.subcommand(), Some(ReplconfSubcommand::GetAck)));
        }

        #[test]
        fn test_decode_listening_port_missing_value() {
            let args = vec![RedisJsonValue::String("LISTENING-PORT".into())];
            let err = ReplconfInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires a port"));
        }

        #[test]
        fn test_decode_ack_missing_value() {
            let args = vec![RedisJsonValue::String("ACK".into())];
            let err = ReplconfInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires an offset"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ReplconfInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ReplconfInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Replconf);
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ReplconfOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_response() {
            let err = ReplconfOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    // Note: Integration tests for REPLCONF require a replica setup.
    // REPLCONF is an internal replication command not typically called directly.
}
