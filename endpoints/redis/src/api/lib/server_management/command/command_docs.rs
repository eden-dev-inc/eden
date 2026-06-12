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
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, CommandDocsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CommandDocs,
    "Returns documentary information about one, multiple or all commands",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `COMMAND DOCS`
/// https://redis.io/docs/latest/commands/command-docs/
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandDocsInput {
    command_name: Option<Vec<RedisJsonValue>>,
}

impl Serialize for CommandDocsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if self.command_name.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CommandDocsInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(command_name) = &self.command_name {
            state.serialize_field("command_name", command_name)?;
        }
        state.end()
    }
}

impl_redis_operation!(CommandDocsInput, API_INFO, { command_name });

impl RedisCommandInput for CommandDocsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let api_str = API_INFO.api.to_string();
        let args = api_str.split_whitespace().collect::<Vec<_>>();
        let mut command = crate::command::cmd(args[0]);
        command.arg(args[1]);

        // Include command names if specified
        if let Some(command_names) = &self.command_name {
            for name in command_names {
                command.arg(name);
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let command_name = if args.is_empty() { None } else { Some(args) };

        Ok(Self { command_name })
    }
}

/// Output for Redis COMMAND DOCS
#[derive(Debug, Clone)]
pub struct CommandDocsOutput {
    /// Number of command docs returned
    count: usize,
    /// Raw response data
    raw: Vec<u8>,
}

impl CommandDocsOutput {
    pub fn new(count: usize, raw: Vec<u8>) -> Self {
        Self { count, raw }
    }

    /// Get the number of command docs returned
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if docs were returned
    pub fn has_docs(&self) -> bool {
        self.count > 0
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.len() / 2, // Key-value pairs
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND DOCS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.len() / 2,
                Resp3Frame::Map { data, .. } => data.len(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND DOCS response: {:?}", other)));
                }
            },
        };

        Ok(Self { count, raw: bytes.to_vec() })
    }
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
        fn test_decode_command_docs_no_args() {
            let args = vec![];
            let result = CommandDocsInput::decode(args).unwrap();
            assert_eq!(result, CommandDocsInput::default());
        }

        #[test]
        fn test_decode_command_docs_with_args() {
            let args = vec![RedisJsonValue::String("GET".to_string()), RedisJsonValue::String("SET".to_string())];
            let result = CommandDocsInput::decode(args.clone()).unwrap();
            assert_eq!(result.command_name, Some(args));
        }

        #[test]
        fn test_command_generation_no_args() {
            let input = CommandDocsInput::default();
            let cmd = input.command();
            assert_eq!(Operation::kind(&input), RedisApi::CommandDocs);

            // Verify the command is properly formatted
            let expected = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_command_generation_with_args() {
            let input = CommandDocsInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string()), RedisJsonValue::String("SET".to_string())]),
            };
            let cmd = input.command();

            // Should include the command names
            assert!(cmd.windows(3).any(|w| w == b"GET"));
            assert!(cmd.windows(3).any(|w| w == b"SET"));
        }

        #[test]
        fn test_kind() {
            let input = CommandDocsInput::default();
            assert_eq!(Operation::kind(&input), RedisApi::CommandDocs);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandDocsInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_raw_resp2_command_docs() {
            // RESP2: *2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n";

            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandDocs);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandDocsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, CommandDocsInput::default());
        }

        #[test]
        fn test_decode_raw_resp2_command_docs_lowercase() {
            // RESP2: *2\r\n$7\r\ncommand\r\n$4\r\ndocs\r\n
            let raw = b"*2\r\n$7\r\ncommand\r\n$4\r\ndocs\r\n";

            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandDocs);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandDocsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, CommandDocsInput::default());
        }

        #[test]
        fn test_decode_raw_resp3_command_docs() {
            // RESP3: *2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n";

            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandDocs);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandDocsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, CommandDocsInput::default());
        }

        #[test]
        fn test_decode_raw_resp3_command_docs_lowercase() {
            // RESP3: *2\r\n$7\r\ncommand\r\n$4\r\ndocs\r\n
            let raw = b"*2\r\n$7\r\ncommand\r\n$4\r\ndocs\r\n";

            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandDocs);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandDocsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, CommandDocsInput::default());
        }

        #[test]
        fn test_decode_raw_resp2_command_docs_with_args() {
            // RESP2: *4\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$3\r\nGET\r\n$3\r\nSET\r\n
            let raw = b"*4\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$3\r\nGET\r\n$3\r\nSET\r\n";

            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandDocs);
            assert_eq!(input.args().len(), 2);

            let decoded = CommandDocsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.command_name.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_serialization() {
            let input = CommandDocsInput::default();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND DOCS"));
        }

        #[test]
        fn test_serialization_with_command_names() {
            let input = CommandDocsInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("command_name"));
            assert!(json.contains("GET"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // COMMAND DOCS requires Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_docs_basic() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&CommandDocsInput::default().command()).await.expect("raw failed");

                    let output = CommandDocsOutput::decode(&result).expect("decode failed");
                    assert!(output.has_docs(), "should return command docs");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_docs_specific_command() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandDocsInput {
                        command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandDocsOutput::decode(&result).expect("decode failed");
                    assert!(output.has_docs(), "should return docs for GET");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_docs_multiple_commands() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandDocsInput {
                        command_name: Some(vec![RedisJsonValue::String("GET".to_string()), RedisJsonValue::String("SET".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandDocsOutput::decode(&result).expect("decode failed");
                    assert!(output.count() >= 2, "should return docs for both commands");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_docs_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            let result = ctx.raw(&CommandDocsInput::default().command()).await.expect("raw failed");

            // RESP2 returns array format
            assert!(result.starts_with(b"*") || result.starts_with(b"-"), "RESP2 should return array or error");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_docs_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            let result = ctx.raw(&CommandDocsInput::default().command()).await.expect("raw failed");

            // RESP3 may use array (*) or map (%)
            assert!(
                result.starts_with(b"*") || result.starts_with(b"%") || result.starts_with(b"-"),
                "RESP3 should return array, map, or error"
            );
            ctx.stop().await;
        }
    }
}
