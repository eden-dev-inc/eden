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

const API_INFO: ApiInfo<RedisApi, CommandGetkeysInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CommandGetkeys,
    "Extracts the key names from an arbitrary command",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `COMMAND GETKEYS`
/// https://redis.io/docs/latest/commands/command-getkeys/
#[derive(Debug, Deserialize, Clone, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandGetkeysInput {
    command: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    arg: Option<Vec<RedisJsonValue>>,
}

impl Serialize for CommandGetkeysInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, command
        if self.arg.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CommandGetkeysInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("command", &self.command)?;
        if let Some(arg) = &self.arg {
            state.serialize_field("arg", arg)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    CommandGetkeysInput,
    API_INFO,
    {command, arg}
);

impl RedisCommandInput for CommandGetkeysInput {
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

        command.arg(&self.command);

        if let Some(arg) = &self.arg {
            for a in arg {
                command.arg(a);
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("COMMAND GETKEYS requires at least 1 argument".to_string()));
        }

        let command = args[0].clone();
        let arg = if args.len() > 1 { Some(args[1..].to_vec()) } else { None };

        Ok(Self { command, arg })
    }
}

/// Output for Redis COMMAND GETKEYS
#[derive(Debug, Clone, PartialEq)]
pub struct CommandGetkeysOutput {
    /// List of key names extracted from the command
    keys: Vec<String>,
}

impl CommandGetkeysOutput {
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Get the list of keys
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Get the number of keys
    pub fn count(&self) -> usize {
        self.keys.len()
    }

    /// Check if any keys were found
    pub fn has_keys(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys = match frame {
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
                    return Err(EpError::parse(format!("unexpected COMMAND GETKEYS response: {:?}", other)));
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
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND GETKEYS response: {:?}", other)));
                }
            },
        };

        Ok(Self { keys })
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
        fn test_encode_command_basic() {
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("SET".to_string()),
                arg: Some(vec![
                    RedisJsonValue::String("mykey".to_string()),
                    RedisJsonValue::String("myvalue".to_string()),
                ]),
            };
            let cmd = input.command();
            // Should be: COMMAND GETKEYS SET mykey myvalue
            assert!(cmd.windows(7).any(|w| w == b"COMMAND"));
            assert!(cmd.windows(7).any(|w| w == b"GETKEYS"));
            assert!(cmd.windows(3).any(|w| w == b"SET"));
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_encode_command_no_extra_args() {
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"GETKEYS"));
            assert!(cmd.windows(3).any(|w| w == b"GET"));
        }

        #[test]
        fn test_kind() {
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: None,
            };
            assert_eq!(Operation::kind(&input), RedisApi::CommandGetkeys);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_with_command_only() {
            let args = vec![RedisJsonValue::String("GET".to_string())];
            let result = CommandGetkeysInput::decode(args).unwrap();
            assert_eq!(result.command, RedisJsonValue::String("GET".to_string()));
            assert_eq!(result.arg, None);
        }

        #[test]
        fn test_decode_with_command_and_args() {
            let args = vec![
                RedisJsonValue::String("SET".to_string()),
                RedisJsonValue::String("key".to_string()),
                RedisJsonValue::String("value".to_string()),
            ];
            let result = CommandGetkeysInput::decode(args).unwrap();
            assert_eq!(result.command, RedisJsonValue::String("SET".to_string()));
            assert_eq!(result.arg.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_empty_args_fails() {
            let args = vec![];
            let err = CommandGetkeysInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_output_array() {
            // Simulate array response with keys: ["mykey"]
            let output = CommandGetkeysOutput::decode(b"*1\r\n$5\r\nmykey\r\n").unwrap();
            assert_eq!(output.count(), 1);
            assert!(output.has_keys());
            assert_eq!(output.keys()[0], "mykey");
        }

        #[test]
        fn test_decode_output_multiple_keys() {
            // Simulate array response with keys: ["key1", "key2"]
            let output = CommandGetkeysOutput::decode(b"*2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n").unwrap();
            assert_eq!(output.count(), 2);
            assert_eq!(output.keys()[0], "key1");
            assert_eq!(output.keys()[1], "key2");
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = CommandGetkeysOutput::decode(b"*0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(!output.has_keys());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CommandGetkeysOutput::decode(b"-ERR Invalid command format\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_raw_resp2() {
            // COMMAND GETKEYS GET mykey
            let raw = b"*4\r\n$7\r\nCOMMAND\r\n$7\r\nGETKEYS\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeys);
            assert_eq!(input.args().len(), 2);

            let decoded = CommandGetkeysInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.command, RedisJsonValue::String("GET".to_string()));
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*4\r\n$7\r\ncommand\r\n$7\r\ngetkeys\r\n$3\r\nget\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeys);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*4\r\n$7\r\nCOMMAND\r\n$7\r\nGETKEYS\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeys);
            assert_eq!(input.args().len(), 2);
        }

        #[test]
        fn test_serialization() {
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND GETKEYS"));
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
        async fn test_command_getkeys_get() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysInput {
                        command: RedisJsonValue::String("GET".to_string()),
                        arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.keys()[0], "mykey");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeys_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysInput {
                        command: RedisJsonValue::String("SET".to_string()),
                        arg: Some(vec![
                            RedisJsonValue::String("mykey".to_string()),
                            RedisJsonValue::String("myvalue".to_string()),
                        ]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.keys()[0], "mykey");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeys_mset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysInput {
                        command: RedisJsonValue::String("MSET".to_string()),
                        arg: Some(vec![
                            RedisJsonValue::String("key1".to_string()),
                            RedisJsonValue::String("val1".to_string()),
                            RedisJsonValue::String("key2".to_string()),
                            RedisJsonValue::String("val2".to_string()),
                        ]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2);
                    assert!(output.keys().contains(&"key1".to_string()));
                    assert!(output.keys().contains(&"key2".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeys_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("testkey".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP2 returns array format
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = CommandGetkeysOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeys_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let input = CommandGetkeysInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("testkey".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP3 also uses array format for this command
            assert!(result.starts_with(b"*"), "RESP3 should return array");
            let output = CommandGetkeysOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }
    }
}
