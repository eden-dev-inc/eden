use crate::api::lib::server_management::command::Filter;
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

const API_INFO: ApiInfo<RedisApi, CommandListInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::CommandList, "Returns a list of command names", ReqType::Read, true);

/// See official Redis documentation for `COMMAND LIST`
/// https://redis.io/docs/latest/commands/command-list/
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandListInput {
    filter: Option<Filter>,
}

impl Serialize for CommandListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if self.filter.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CommandListInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(filter) = &self.filter {
            match filter {
                Filter::ACLCAT(aclcat) => {
                    state.serialize_field("aclcat", aclcat)?;
                }
                Filter::MODULE(module) => {
                    state.serialize_field("module", module)?;
                }
                Filter::PATTERN(pattern) => {
                    state.serialize_field("pattern", pattern)?;
                }
            }
        }
        state.end()
    }
}

impl_redis_operation!(CommandListInput, API_INFO, { filter });

impl RedisCommandInput for CommandListInput {
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

        if let Some(filter) = &self.filter {
            command.arg("FILTERBY");
            match filter {
                Filter::MODULE(m) => {
                    command.arg("MODULE");
                    command.arg(m);
                }
                Filter::ACLCAT(c) => {
                    command.arg("ACLCAT");
                    command.arg(c);
                }
                Filter::PATTERN(p) => {
                    command.arg("PATTERN");
                    command.arg(p);
                }
            };
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut filter = None;

        if args.len() >= 3
            && let RedisJsonValue::String(s) = &args[0]
            && s.to_uppercase() == "FILTERBY"
            && let RedisJsonValue::String(filter_type) = &args[1]
        {
            match filter_type.to_uppercase().as_str() {
                "MODULE" => filter = Some(Filter::MODULE(args[2].clone())),
                "ACLCAT" => filter = Some(Filter::ACLCAT(args[2].clone())),
                "PATTERN" => filter = Some(Filter::PATTERN(args[2].clone())),
                _ => {}
            }
        }

        Ok(Self { filter })
    }
}

/// Output for Redis COMMAND LIST
#[derive(Debug, Clone)]
pub struct CommandListOutput {
    /// List of command names
    commands: Vec<String>,
}

impl CommandListOutput {
    pub fn new(commands: Vec<String>) -> Self {
        Self { commands }
    }

    /// Get the list of command names
    pub fn commands(&self) -> &[String] {
        &self.commands
    }

    /// Get the number of commands
    pub fn count(&self) -> usize {
        self.commands.len()
    }

    /// Check if a specific command exists
    pub fn has_command(&self, name: &str) -> bool {
        self.commands.iter().any(|c| c.eq_ignore_ascii_case(name))
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let commands = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .filter_map(|f| match f {
                        Resp2Frame::BulkString(b) => String::from_utf8(b).ok(),
                        Resp2Frame::SimpleString(s) => String::from_utf8(s).ok(),
                        _ => None,
                    })
                    .collect(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND LIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .filter_map(|f| match f {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).ok(),
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).ok(),
                        _ => None,
                    })
                    .collect(),
                Resp3Frame::Set { data, .. } => data
                    .into_iter()
                    .filter_map(|f| match f {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).ok(),
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).ok(),
                        _ => None,
                    })
                    .collect(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND LIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { commands })
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
        fn test_encode_command_no_filter() {
            let input = CommandListInput::default();
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n");
        }

        #[test]
        fn test_encode_command_with_module_filter() {
            let input = CommandListInput {
                filter: Some(Filter::MODULE(RedisJsonValue::String("mymodule".to_string()))),
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"FILTERBY"));
            assert!(cmd.windows(6).any(|w| w == b"MODULE"));
            assert!(cmd.windows(8).any(|w| w == b"mymodule"));
        }

        #[test]
        fn test_encode_command_with_aclcat_filter() {
            let input = CommandListInput {
                filter: Some(Filter::ACLCAT(RedisJsonValue::String("read".to_string()))),
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"FILTERBY"));
            assert!(cmd.windows(6).any(|w| w == b"ACLCAT"));
            assert!(cmd.windows(4).any(|w| w == b"read"));
        }

        #[test]
        fn test_encode_command_with_pattern_filter() {
            let input = CommandListInput {
                filter: Some(Filter::PATTERN(RedisJsonValue::String("get*".to_string()))),
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"FILTERBY"));
            assert!(cmd.windows(7).any(|w| w == b"PATTERN"));
            assert!(cmd.windows(4).any(|w| w == b"get*"));
        }

        #[test]
        fn test_kind() {
            let input = CommandListInput::default();
            assert_eq!(Operation::kind(&input), RedisApi::CommandList);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandListInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_no_args() {
            let args = vec![];
            let result = CommandListInput::decode(args).unwrap();
            assert_eq!(result.filter, None);
        }

        #[test]
        fn test_decode_with_filterby_module() {
            let args = vec![
                RedisJsonValue::String("FILTERBY".to_string()),
                RedisJsonValue::String("MODULE".to_string()),
                RedisJsonValue::String("mymodule".to_string()),
            ];
            let result = CommandListInput::decode(args).unwrap();
            assert!(matches!(result.filter, Some(Filter::MODULE(_))));
        }

        #[test]
        fn test_decode_with_filterby_aclcat() {
            let args = vec![
                RedisJsonValue::String("FILTERBY".to_string()),
                RedisJsonValue::String("ACLCAT".to_string()),
                RedisJsonValue::String("read".to_string()),
            ];
            let result = CommandListInput::decode(args).unwrap();
            assert!(matches!(result.filter, Some(Filter::ACLCAT(_))));
        }

        #[test]
        fn test_decode_with_filterby_pattern() {
            let args = vec![
                RedisJsonValue::String("FILTERBY".to_string()),
                RedisJsonValue::String("PATTERN".to_string()),
                RedisJsonValue::String("get*".to_string()),
            ];
            let result = CommandListInput::decode(args).unwrap();
            assert!(matches!(result.filter, Some(Filter::PATTERN(_))));
        }

        #[test]
        fn test_decode_with_filterby_lowercase() {
            let args = vec![
                RedisJsonValue::String("filterby".to_string()),
                RedisJsonValue::String("pattern".to_string()),
                RedisJsonValue::String("get*".to_string()),
            ];
            let result = CommandListInput::decode(args).unwrap();
            assert!(matches!(result.filter, Some(Filter::PATTERN(_))));
        }

        #[test]
        fn test_decode_raw_resp2() {
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandList);
            assert_eq!(input.args().len(), 0);

            let decoded = CommandListInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.filter, None);
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*2\r\n$7\r\ncommand\r\n$4\r\nlist\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandList);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandList);
            assert_eq!(input.args().len(), 0);
        }

        #[test]
        fn test_serialization() {
            let input = CommandListInput::default();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND LIST"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // COMMAND LIST requires Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_list_basic() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&CommandListInput::default().command()).await.expect("raw failed");

                    let output = CommandListOutput::decode(&result).expect("decode failed");
                    assert!(output.count() > 50, "Redis should have many commands");
                    assert!(output.has_command("GET"), "should include GET");
                    assert!(output.has_command("SET"), "should include SET");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_list_with_pattern() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandListInput {
                        filter: Some(Filter::PATTERN(RedisJsonValue::String("GET*".to_string()))),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandListOutput::decode(&result).expect("decode failed");
                    // Should have GET and possibly GETEX, GETDEL, etc.
                    assert!(output.count() >= 1, "should match at least GET");
                    for cmd in output.commands() {
                        assert!(cmd.to_uppercase().starts_with("GET"), "all commands should start with GET");
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_list_with_aclcat() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandListInput {
                        filter: Some(Filter::ACLCAT(RedisJsonValue::String("read".to_string()))),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandListOutput::decode(&result).expect("decode failed");
                    assert!(output.count() > 0, "should have read commands");
                    assert!(output.has_command("GET"), "GET should be a read command");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            let result = ctx.raw(&CommandListInput::default().command()).await.expect("raw failed");

            // RESP2 returns array format
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = CommandListOutput::decode(&result).expect("decode failed");
            assert!(output.count() > 0);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_list_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            let result = ctx.raw(&CommandListInput::default().command()).await.expect("raw failed");

            // RESP3 may use array (*) or set (~)
            assert!(result.starts_with(b"*") || result.starts_with(b"~"), "RESP3 should return array or set");
            let output = CommandListOutput::decode(&result).expect("decode failed");
            assert!(output.count() > 0);
            ctx.stop().await;
        }
    }
}
