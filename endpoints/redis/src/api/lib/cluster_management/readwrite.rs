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

const API_INFO: ApiInfo<RedisApi, ReadwriteInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Readwrite,
    "Disables read-only mode for a connection to a Redis Cluster replica node",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `READWRITE`
/// https://redis.io/docs/latest/commands/readwrite/
///
/// Disables read queries on a Redis Cluster replica node.
/// This reverses the effect of the READONLY command.
/// After READWRITE, the replica will redirect read queries to the master.
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ReadwriteInput {}

impl Serialize for ReadwriteInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ReadwriteInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ReadwriteInput, API_INFO);

impl RedisCommandInput for ReadwriteInput {
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
                "READWRITE expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis READWRITE command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ReadwriteOutput {
    status: String,
}

impl ReadwriteOutput {
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
                other => Err(EpError::parse(format!("unexpected READWRITE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected READWRITE response: {:?}", other))),
            },
        }
    }
}

impl Default for ReadwriteOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ReadwriteOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ReadwriteOutput", 1)?;
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
        fn test_encode_command() {
            let input = ReadwriteInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("READWRITE"));
            // Should be a simple command with no arguments
            assert_eq!(cmd.to_vec(), b"*1\r\n$9\r\nREADWRITE\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ReadwriteOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ReadwriteOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ReadwriteInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ReadwriteInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ReadwriteInput {};
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Readwrite);
        }

        #[test]
        fn test_default() {
            let output = ReadwriteOutput::default();
            assert!(output.is_ok());
        }
    }
}
