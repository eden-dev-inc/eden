use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, PsyncInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Psync,
    "An internal command for configuring the replication stream",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `PSYNC`
/// https://redis.io/docs/latest/commands/psync/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PsyncInput {
    replication_id: RedisJsonValue,
    offset: RedisJsonValue,
}

impl PsyncInput {
    pub fn new(replication_id: impl Into<RedisJsonValue>, offset: impl Into<RedisJsonValue>) -> Self {
        Self { replication_id: replication_id.into(), offset: offset.into() }
    }

    pub fn replication_id(&self) -> &RedisJsonValue {
        &self.replication_id
    }
    pub fn offset(&self) -> &RedisJsonValue {
        &self.offset
    }
}

impl Serialize for PsyncInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PsyncInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("replication_id", &self.replication_id)?;
        state.serialize_field("offset", &self.offset)?;
        state.end()
    }
}

impl_redis_operation!(
    PsyncInput,
    API_INFO,
    {replication_id, offset}
);

impl RedisCommandInput for PsyncInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.replication_id).arg(&self.offset);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::parse(format!("PSYNC requires 2 arguments (replication_id, offset), given {}", args.len())));
        }

        Ok(Self { replication_id: args[0].clone(), offset: args[1].clone() })
    }
}

/// Output for Redis PSYNC command
#[derive(Debug, Clone)]
pub struct PsyncOutput {
    pub response: PsyncResponse,
}

#[derive(Debug, Clone)]
pub enum PsyncResponse {
    FullResync {
        replication_id: String,
        offset: i64,
        rdb_data: Vec<u8>,
    },
    Continue {
        replication_id: String,
    },
}

impl PsyncOutput {
    pub fn parse(raw: &[u8]) -> Result<Self, EpError> {
        // Check for +FULLRESYNC (search in raw bytes to avoid index mismatch with lossy UTF-8)
        if raw.starts_with(b"+FULLRESYNC") {
            // Find the first \r\n to get the FULLRESYNC line
            let first_crlf =
                raw.windows(2).position(|w| w == b"\r\n").ok_or_else(|| EpError::parse("Invalid FULLRESYNC format: no CRLF"))?;

            let first_line = std::str::from_utf8(&raw[..first_crlf]).map_err(|_| EpError::parse("Invalid FULLRESYNC format: not UTF-8"))?;

            let parts: Vec<&str> = first_line.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(EpError::parse("Invalid FULLRESYNC format"));
            }

            let replication_id = parts[1].to_string();
            let offset = parts[2].parse::<i64>().map_err(|_| EpError::parse("Invalid offset in FULLRESYNC"))?;

            // RDB data follows after the +FULLRESYNC line
            // Format: +FULLRESYNC <replid> <offset>\r\n$<rdb_len>\r\n<rdb_data>
            let rdb_portion = raw.get(first_crlf + 2..).ok_or_else(|| EpError::parse("No RDB data found"))?;

            // Parse bulk string: $<len>\r\n<data>
            let rdb_data = parse_bulk_string(rdb_portion)?;

            return Ok(Self {
                response: PsyncResponse::FullResync { replication_id, offset, rdb_data },
            });
        }

        // Check for +CONTINUE (search in raw bytes)
        if raw.starts_with(b"+CONTINUE") {
            // Find the first \r\n
            let first_crlf = raw.windows(2).position(|w| w == b"\r\n").ok_or_else(|| EpError::parse("Invalid CONTINUE format: no CRLF"))?;

            let first_line = std::str::from_utf8(&raw[..first_crlf]).map_err(|_| EpError::parse("Invalid CONTINUE format: not UTF-8"))?;

            let parts: Vec<&str> = first_line.split_whitespace().collect();
            if parts.len() < 2 {
                return Err(EpError::parse("Invalid CONTINUE format"));
            }

            let replication_id = parts[1].to_string();

            return Ok(Self { response: PsyncResponse::Continue { replication_id } });
        }

        Err(EpError::parse(format!("Unknown PSYNC response: {}", String::from_utf8_lossy(raw))))
    }

    pub fn is_full_resync(&self) -> bool {
        matches!(self.response, PsyncResponse::FullResync { .. })
    }

    pub fn is_continue(&self) -> bool {
        matches!(self.response, PsyncResponse::Continue { .. })
    }

    pub fn replication_id(&self) -> &str {
        match &self.response {
            PsyncResponse::FullResync { replication_id, .. } => replication_id,
            PsyncResponse::Continue { replication_id } => replication_id,
        }
    }

    pub fn offset(&self) -> Option<i64> {
        match &self.response {
            PsyncResponse::FullResync { offset, .. } => Some(*offset),
            PsyncResponse::Continue { .. } => None,
        }
    }

    pub fn rdb_data(&self) -> Option<&[u8]> {
        match &self.response {
            PsyncResponse::FullResync { rdb_data, .. } => Some(rdb_data),
            PsyncResponse::Continue { .. } => None,
        }
    }
}

fn parse_bulk_string(data: &[u8]) -> Result<Vec<u8>, EpError> {
    // Format: $<length>\r\n<data>
    if data.is_empty() || data[0] != b'$' {
        return Err(EpError::parse("Expected bulk string"));
    }

    let newline_pos = data.iter().position(|&b| b == b'\r').ok_or_else(|| EpError::parse("Malformed bulk string"))?;

    let len_str = String::from_utf8_lossy(&data[1..newline_pos]);
    let len = len_str.parse::<usize>().map_err(|_| EpError::parse("Invalid bulk string length"))?;

    let data_start = newline_pos + 2; // Skip \r\n
    let data_end = data_start + len;

    if data_end > data.len() {
        return Err(EpError::parse("Incomplete bulk string data"));
    }

    Ok(data[data_start..data_end].to_vec())
}

impl Serialize for PsyncOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PsyncOutput", 2)?;
        match &self.response {
            PsyncResponse::FullResync { replication_id, offset, rdb_data } => {
                state.serialize_field("type", "FULLRESYNC")?;
                state.serialize_field("replication_id", replication_id)?;
                state.serialize_field("offset", offset)?;
                state.serialize_field("rdb_size", &rdb_data.len())?;
            }
            PsyncResponse::Continue { replication_id } => {
                state.serialize_field("type", "CONTINUE")?;
                state.serialize_field("replication_id", replication_id)?;
            }
        }
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
            let input = PsyncInput::new(RedisJsonValue::String("?".into()), RedisJsonValue::Integer(-1));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("PSYNC"));
            assert!(cmd_str.contains("?"));
            assert!(cmd_str.contains("-1"));
        }

        #[test]
        fn test_encode_command_with_replication_id() {
            let input = PsyncInput::new(RedisJsonValue::String("abc123def456".into()), RedisJsonValue::Integer(12345));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("abc123def456"));
            assert!(cmd_str.contains("12345"));
        }

        #[test]
        fn test_decode_requires_two_args() {
            let one_arg = vec![RedisJsonValue::String("?".into())];
            let err = PsyncInput::decode(one_arg).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_two_args_succeeds() {
            let two_args = vec![RedisJsonValue::String("?".into()), RedisJsonValue::Integer(-1)];
            let input = PsyncInput::decode(two_args).unwrap();
            assert_eq!(input.replication_id(), &RedisJsonValue::String("?".into()));
            assert_eq!(input.offset(), &RedisJsonValue::Integer(-1));
        }

        #[test]
        fn test_decode_three_args_fails() {
            let three_args = vec![
                RedisJsonValue::String("?".into()),
                RedisJsonValue::Integer(-1),
                RedisJsonValue::String("extra".into()),
            ];
            let err = PsyncInput::decode(three_args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PsyncInput::new(RedisJsonValue::String("?".into()), RedisJsonValue::Integer(-1));
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = PsyncInput::new(RedisJsonValue::String("?".into()), RedisJsonValue::Integer(-1));
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Psync);
        }

        #[test]
        fn test_output_parses_full_resync() {
            let repl_id = "abc123";
            let offset = 42;
            let rdb = vec![1_u8, 2, 3];
            let mut raw = format!("+FULLRESYNC {repl_id} {offset}\r\n${}\r\n", rdb.len()).into_bytes();
            raw.extend_from_slice(&rdb);

            let parsed = PsyncOutput::parse(&raw).expect("parses FULLRESYNC");

            assert!(parsed.is_full_resync());
            assert!(!parsed.is_continue());
            assert_eq!(parsed.replication_id(), repl_id);
            assert_eq!(parsed.offset(), Some(offset));
            assert_eq!(parsed.rdb_data(), Some(rdb.as_slice()));
        }

        #[test]
        fn test_output_parses_continue() {
            let repl_id = "repl-1";
            let raw = format!("+CONTINUE {repl_id}\r\n").into_bytes();

            let parsed = PsyncOutput::parse(&raw).expect("parses CONTINUE");

            assert!(!parsed.is_full_resync());
            assert!(parsed.is_continue());
            assert_eq!(parsed.replication_id(), repl_id);
            assert_eq!(parsed.offset(), None);
            assert_eq!(parsed.rdb_data(), None);
        }

        #[test]
        fn test_output_rejects_unknown_response() {
            let raw = b"-ERR unknown\r\n".to_vec();
            let err = PsyncOutput::parse(&raw).expect_err("expected malformed fullresync");
            assert!(err.to_string().contains("Unknown PSYNC response"));
        }

        #[test]
        fn test_output_rejects_malformed_fullresync() {
            let raw = b"+FULLRESYNC\r\n".to_vec(); // Missing replication_id and offset
            let err = PsyncOutput::parse(&raw).expect_err("expected malformed fullresync");
            assert!(err.to_string().contains("Invalid FULLRESYNC format"));
        }

        #[test]
        fn test_output_rejects_malformed_continue() {
            let raw = b"+CONTINUE\r\n".to_vec(); // Missing replication_id
            let err = PsyncOutput::parse(&raw).expect_err("expected malformed fullresync");
            assert!(err.to_string().contains("Invalid CONTINUE format"));
        }

        #[test]
        fn test_parse_bulk_string_empty_data() {
            let err = parse_bulk_string(&[]).unwrap_err();
            assert!(err.to_string().contains("Expected bulk string"));
        }

        #[test]
        fn test_parse_bulk_string_invalid_prefix() {
            let err = parse_bulk_string(b"+OK\r\n").unwrap_err();
            assert!(err.to_string().contains("Expected bulk string"));
        }
    }

    // Note: Integration tests for PSYNC require a replica setup which is complex.
    // PSYNC is an internal replication command not typically called directly.
}
