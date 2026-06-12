#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
mod command_count;
mod command_docs;
mod command_getkeys;
mod command_getkeysandflags;
mod command_info;
mod command_list;

pub use command_count::*;
pub use command_docs::*;
pub use command_getkeys::*;
pub use command_getkeysandflags::*;
pub use command_info::*;
pub use command_list::*;

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

const API_INFO: ApiInfo<RedisApi, CommandInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Command,
    "Returns detailed information about all commands",
    ReqType::Read, // Fixed: COMMAND is a read operation
    true,
);

/// See official Redis documentation for `COMMAND`
/// https://redis.io/docs/latest/commands/command/
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandInput {}

impl Serialize for CommandInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CommandInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(CommandInput, API_INFO);

impl RedisCommandInput for CommandInput {
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
                "COMMAND expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis COMMAND
///
/// Returns an array of command information for all Redis commands.
#[derive(Debug, Clone)]
pub struct CommandOutput {
    /// Number of commands returned
    count: usize,
    /// Raw response data (array of command info)
    raw: Vec<u8>,
}

impl CommandOutput {
    pub fn new(count: usize, raw: Vec<u8>) -> Self {
        Self { count, raw }
    }

    /// Get the number of commands returned
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if the response is non-empty
    pub fn has_commands(&self) -> bool {
        self.count > 0
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.len(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.len(),
                Resp3Frame::Map { data, .. } => data.len(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND response: {:?}", other)));
                }
            },
        };

        Ok(Self { count, raw: bytes.to_vec() })
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, ToSchema, JsonSchema)]
enum Filter {
    MODULE(RedisJsonValue),
    ACLCAT(RedisJsonValue),
    PATTERN(RedisJsonValue),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::RedisCommandInput;
    use crate::protocol::decoder::{DecoderRespFrame, RedisCommandArgs};
    use endpoint_types::Operation;
    use redis_protocol::resp2::decode::decode as decode_resp2;
    use redis_protocol::resp3::decode::complete::decode as decode_resp3;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = CommandInput::default();
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*1\r\n$7\r\nCOMMAND\r\n");
        }

        #[test]
        fn test_kind() {
            let input = CommandInput::default();
            assert_eq!(Operation::kind(&input), RedisApi::Command);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_no_args() {
            let args = vec![];
            let result = CommandInput::decode(args).unwrap();
            assert_eq!(result, CommandInput::default());
        }

        #[test]
        fn test_decode_with_extra_args_succeeds() {
            let args = vec![RedisJsonValue::String("extra".to_string())];
            let result = CommandInput::decode(args).unwrap();
            assert_eq!(result, CommandInput::default());
        }

        #[test]
        fn test_decode_raw_resp2() {
            let raw = b"*1\r\n$7\r\nCOMMAND\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Command);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, CommandInput::default());
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*1\r\n$7\r\ncommand\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Command);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*1\r\n$7\r\nCOMMAND\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Command);
            assert_eq!(input.args().len(), 0);
        }

        #[test]
        fn test_serialization() {
            let input = CommandInput::default();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&CommandInput::default().command()).await.expect("raw failed");

                    let output = CommandOutput::decode(&result).expect("decode failed");
                    // Redis should have many commands
                    assert!(output.has_commands());
                    assert!(output.count() > 50, "Redis should have many commands");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&CommandInput::default().command()).await.expect("raw failed");

            // RESP2 array format starts with *
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = CommandOutput::decode(&result).expect("decode failed");
            assert!(output.has_commands());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&CommandInput::default().command()).await.expect("raw failed");

            // RESP3 may use array (*) or map (%)
            assert!(result.starts_with(b"*") || result.starts_with(b"%"), "RESP3 should return array or map");
            let output = CommandOutput::decode(&result).expect("decode failed");
            assert!(output.has_commands());
            ctx.stop().await;
        }
    }
}
