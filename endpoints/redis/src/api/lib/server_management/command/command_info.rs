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

const API_INFO: ApiInfo<RedisApi, CommandInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CommandInfo,
    "Returns information about one, multiple or all commands",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `COMMAND INFO`
/// https://redis.io/docs/latest/commands/command-info/
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandInfoInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    command_name: Option<Vec<RedisJsonValue>>,
}

impl Serialize for CommandInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if self.command_name.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CommandInfoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(command_name) = &self.command_name {
            state.serialize_field("command_name", command_name)?;
        }
        state.end()
    }
}

impl_redis_operation!(CommandInfoInput, API_INFO, { command_name });

impl RedisCommandInput for CommandInfoInput {
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

        if let Some(command_name) = &self.command_name {
            for name in command_name {
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

/// Output for Redis COMMAND INFO
#[derive(Debug, Clone)]
pub struct CommandInfoOutput {
    /// Number of command info entries returned
    count: usize,
    /// Raw response data
    raw: Vec<u8>,
}

impl CommandInfoOutput {
    pub fn new(count: usize, raw: Vec<u8>) -> Self {
        Self { count, raw }
    }

    /// Get the number of command info entries
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if info was returned
    pub fn has_info(&self) -> bool {
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
                    return Err(EpError::parse(format!("unexpected COMMAND INFO response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected COMMAND INFO response: {:?}", other)));
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
        fn test_encode_command_no_args() {
            let input = CommandInfoInput::default();
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n");
        }

        #[test]
        fn test_encode_command_with_single_command() {
            let input = CommandInfoInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"COMMAND"));
            assert!(cmd.windows(4).any(|w| w == b"INFO"));
            assert!(cmd.windows(3).any(|w| w == b"GET"));
        }

        #[test]
        fn test_encode_command_with_multiple_commands() {
            let input = CommandInfoInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string()), RedisJsonValue::String("SET".to_string())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"GET"));
            assert!(cmd.windows(3).any(|w| w == b"SET"));
        }

        #[test]
        fn test_kind() {
            let input = CommandInfoInput::default();
            assert_eq!(Operation::kind(&input), RedisApi::CommandInfo);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandInfoInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_no_args() {
            let args = vec![];
            let result = CommandInfoInput::decode(args).unwrap();
            assert_eq!(result.command_name, None);
        }

        #[test]
        fn test_decode_with_args() {
            let args = vec![RedisJsonValue::String("GET".to_string()), RedisJsonValue::String("SET".to_string())];
            let result = CommandInfoInput::decode(args.clone()).unwrap();
            assert_eq!(result.command_name, Some(args));
        }

        #[test]
        fn test_decode_raw_resp2() {
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandInfo);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandInfoInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.command_name, None);
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*2\r\n$7\r\ncommand\r\n$4\r\ninfo\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandInfo);
        }

        #[test]
        fn test_decode_raw_resp2_with_args() {
            let raw = b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandInfo);
            assert_eq!(input.args().len(), 1);

            let decoded = CommandInfoInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.command_name.as_ref().unwrap().len(), 1);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandInfo);
            assert_eq!(input.args().len(), 0);
        }

        #[test]
        fn test_serialization() {
            let input = CommandInfoInput::default();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND INFO"));
        }

        #[test]
        fn test_serialization_with_command_names() {
            let input = CommandInfoInput {
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

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_all() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandInfoInput::default().command();
                    let result = ctx.raw(&input).await.expect("raw failed");

                    let output = CommandInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.has_info());
                    assert!(output.count() > 50, "Redis should have many commands");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_specific() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandInfoInput {
                        command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.has_info());
                    assert_eq!(output.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_multiple() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandInfoInput {
                        command_name: Some(vec![
                            RedisJsonValue::String("GET".to_string()),
                            RedisJsonValue::String("SET".to_string()),
                            RedisJsonValue::String("DEL".to_string()),
                        ]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.has_info());
                    assert_eq!(output.count(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandInfoInput {
                        command_name: Some(vec![RedisJsonValue::String("NONEXISTENTCMD".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    // Should return array with null for unknown command
                    let output = CommandInfoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let input = CommandInfoInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP2 returns array format
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = CommandInfoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_info_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let input = CommandInfoInput {
                command_name: Some(vec![RedisJsonValue::String("GET".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP3 may use array (*) or map (%)
            assert!(result.starts_with(b"*") || result.starts_with(b"%"), "RESP3 should return array or map");
            let output = CommandInfoOutput::decode(&result).expect("decode failed");
            assert!(output.count() >= 1);
            ctx.stop().await;
        }
    }
}
