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
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, SyncInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Sync, "An internal command used in replication", ReqType::Write, false);

/// See official Redis documentation for `SYNC`
/// https://redis.io/docs/latest/commands/sync/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SyncInput {}

impl Serialize for SyncInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SyncInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(SyncInput, API_INFO);

impl RedisCommandInput for SyncInput {
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
            log_warn!(_ctx, "SYNC expects no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }
        Ok(Self::default())
    }
}

/// Output for Redis SYNC command
///
/// SYNC is an internal command used for replication. When executed on a
/// non-replica connection, it initiates full synchronization and streams
/// the RDB file followed by incremental commands.
///
/// Note: This command is typically not used directly by clients.
/// The response is a bulk string containing either:
/// - The full RDB dump when full sync starts
/// - An error if replication cannot proceed
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SyncOutput {
    /// The raw response data (typically RDB header or error message)
    data: Option<Vec<u8>>,
    /// Whether an error occurred
    error: Option<String>,
}

impl SyncOutput {
    pub fn new(data: Option<Vec<u8>>) -> Self {
        Self { data, error: None }
    }

    pub fn error(message: String) -> Self {
        Self { data: None, error: Some(message) }
    }

    /// Get the raw data if available
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Check if the response indicates an error
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// Get the error message if any
    pub fn error_message(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Check if RDB data was received (starts with REDIS magic)
    pub fn has_rdb_data(&self) -> bool {
        self.data.as_ref().map(|d| d.starts_with(b"REDIS")).unwrap_or(false)
    }

    /// Decode the Redis protocol response into a SyncOutput
    ///
    /// Note: SYNC response is complex - it can be a bulk string with RDB data,
    /// or an error. In production, this streams data continuously.
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        // SYNC can return various responses:
        // 1. Bulk string with RDB data (starts with $)
        // 2. Error (starts with -)
        // 3. Simple string FULLRESYNC (RESP3)

        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Ok(Self::new(Some(data))),
                Resp2Frame::SimpleString(s) => Ok(Self::new(Some(s))),
                Resp2Frame::Error(e) => Ok(Self::error(e)),
                Resp2Frame::Null => Ok(Self::new(None)),
                other => Err(EpError::parse(format!("unexpected SYNC response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Ok(Self::new(Some(data))),
                Resp3Frame::SimpleString { data, .. } => Ok(Self::new(Some(data))),
                Resp3Frame::SimpleError { data, .. } => Ok(Self::error(data)),
                Resp3Frame::BlobError { data, .. } => Ok(Self::error(String::from_utf8_lossy(&data).to_string())),
                Resp3Frame::Null => Ok(Self::new(None)),
                other => Err(EpError::parse(format!("unexpected SYNC response: {:?}", other))),
            },
        }
    }
}

impl Serialize for SyncOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SyncOutput", 2)?;
        // Serialize data as base64 if present (binary data)
        if let Some(ref data) = self.data {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(data);
            state.serialize_field("data", &encoded)?;
        } else {
            state.serialize_field("data", &Option::<String>::None)?;
        }
        state.serialize_field("error", &self.error)?;
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
            let input = SyncInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nSYNC\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = SyncOutput::decode(b"$5\r\nREDIS\r\n").unwrap();
            assert!(!output.is_error());
            assert_eq!(output.data(), Some(b"REDIS".as_slice()));
        }

        #[test]
        fn test_decode_error() {
            let output = SyncOutput::decode(b"-ERR Can't SYNC\r\n").unwrap();
            assert!(output.is_error());
            assert!(output.error_message().unwrap().contains("Can't SYNC"));
        }

        #[test]
        fn test_has_rdb_data() {
            let output = SyncOutput::new(Some(b"REDIS0009".to_vec()));
            assert!(output.has_rdb_data());

            let output = SyncOutput::new(Some(b"OTHER".to_vec()));
            assert!(!output.has_rdb_data());

            let output = SyncOutput::new(None);
            assert!(!output.has_rdb_data());
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = SyncInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SyncInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SyncInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Sync);
        }

        #[test]
        fn test_serialize_output_with_data() {
            let output = SyncOutput::new(Some(b"test".to_vec()));
            let json = serde_json::to_string(&output).unwrap();
            // Data should be base64 encoded
            assert!(json.contains("\"data\":"));
            assert!(json.contains("\"error\":null"));
        }

        #[test]
        fn test_serialize_output_with_error() {
            let output = SyncOutput::error("test error".to_string());
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("test error"));
        }
    }

    // Note: Integration tests for SYNC are limited because:
    // 1. SYNC initiates replication which has side effects
    // 2. The response is streaming and not easily captured
    // 3. It requires specific server configuration
    //
    // In a real test environment, you would need a replica setup
    // to properly test SYNC behavior.
}
