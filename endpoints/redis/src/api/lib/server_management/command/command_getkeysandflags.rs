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

const API_INFO: ApiInfo<RedisApi, CommandGetkeysandflagsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CommandGetkeysandflags,
    "Extracts the key names and access flags for an arbitrary command",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `COMMAND GETKEYSANDFLAGS`
/// https://redis.io/docs/latest/commands/command-getkeysandflags/
#[derive(Debug, Deserialize, Clone, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CommandGetkeysandflagsInput {
    command: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    arg: Option<Vec<RedisJsonValue>>,
}

impl Serialize for CommandGetkeysandflagsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, command
        if self.arg.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CommandGetkeysandflagsInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("command", &self.command)?;
        if let Some(arg) = &self.arg {
            state.serialize_field("arg", arg)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    CommandGetkeysandflagsInput,
    API_INFO,
    {command, arg}
);

impl RedisCommandInput for CommandGetkeysandflagsInput {
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
            return Err(EpError::request("COMMAND GETKEYSANDFLAGS requires at least 1 argument".to_string()));
        }

        let command = args[0].clone();
        let arg = if args.len() > 1 { Some(args[1..].to_vec()) } else { None };

        Ok(Self { command, arg })
    }
}

/// A key with its access flags
#[derive(Debug, Clone, PartialEq)]
pub struct KeyWithFlags {
    pub key: String,
    pub flags: Vec<String>,
}

/// Output for Redis COMMAND GETKEYSANDFLAGS
#[derive(Debug, Clone)]
pub struct CommandGetkeysandflagsOutput {
    /// List of keys with their access flags
    keys_with_flags: Vec<KeyWithFlags>,
}

impl CommandGetkeysandflagsOutput {
    pub fn new(keys_with_flags: Vec<KeyWithFlags>) -> Self {
        Self { keys_with_flags }
    }

    /// Get the list of keys with flags
    pub fn keys_with_flags(&self) -> &[KeyWithFlags] {
        &self.keys_with_flags
    }

    /// Get just the key names
    pub fn keys(&self) -> Vec<&str> {
        self.keys_with_flags.iter().map(|k| k.key.as_str()).collect()
    }

    /// Get the number of keys
    pub fn count(&self) -> usize {
        self.keys_with_flags.len()
    }

    /// Check if any keys were found
    pub fn has_keys(&self) -> bool {
        !self.keys_with_flags.is_empty()
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys_with_flags = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut result = Vec::new();
                    for item in arr {
                        if let Resp2Frame::Array(pair) = item
                            && pair.len() >= 2
                        {
                            let key = match &pair[0] {
                                Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).unwrap_or_default(),
                                Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).unwrap_or_default(),
                                _ => continue,
                            };
                            let flags = match &pair[1] {
                                Resp2Frame::Array(flag_arr) => flag_arr
                                    .iter()
                                    .filter_map(|f| match f {
                                        Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).ok(),
                                        Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).ok(),
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            result.push(KeyWithFlags { key, flags });
                        }
                    }
                    result
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND GETKEYSANDFLAGS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut result = Vec::new();
                    for item in data {
                        if let Resp3Frame::Array { data: pair, .. } = item
                            && pair.len() >= 2
                        {
                            let key = match &pair[0] {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).unwrap_or_default(),
                                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).unwrap_or_default(),
                                _ => continue,
                            };
                            let flags = match &pair[1] {
                                Resp3Frame::Array { data: flag_arr, .. } => flag_arr
                                    .iter()
                                    .filter_map(|f| match f {
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).ok(),
                                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).ok(),
                                        _ => None,
                                    })
                                    .collect(),
                                Resp3Frame::Set { data: flag_arr, .. } => flag_arr
                                    .iter()
                                    .filter_map(|f| match f {
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).ok(),
                                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).ok(),
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            result.push(KeyWithFlags { key, flags });
                        }
                    }
                    result
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COMMAND GETKEYSANDFLAGS response: {:?}", other)));
                }
            },
        };

        Ok(Self { keys_with_flags })
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
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("SET".to_string()),
                arg: Some(vec![
                    RedisJsonValue::String("mykey".to_string()),
                    RedisJsonValue::String("myvalue".to_string()),
                ]),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"COMMAND"));
            assert!(cmd.windows(15).any(|w| w == b"GETKEYSANDFLAGS"));
            assert!(cmd.windows(3).any(|w| w == b"SET"));
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_encode_command_no_extra_args() {
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(15).any(|w| w == b"GETKEYSANDFLAGS"));
            assert!(cmd.windows(3).any(|w| w == b"GET"));
        }

        #[test]
        fn test_kind() {
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: None,
            };
            assert_eq!(Operation::kind(&input), RedisApi::CommandGetkeysandflags);
        }

        #[test]
        fn test_keys_empty() {
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_with_command_only() {
            let args = vec![RedisJsonValue::String("GET".to_string())];
            let result = CommandGetkeysandflagsInput::decode(args).unwrap();
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
            let result = CommandGetkeysandflagsInput::decode(args).unwrap();
            assert_eq!(result.command, RedisJsonValue::String("SET".to_string()));
            assert_eq!(result.arg.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_empty_args_fails() {
            let args = vec![];
            let err = CommandGetkeysandflagsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_raw_resp2() {
            let raw = b"*4\r\n$7\r\nCOMMAND\r\n$15\r\nGETKEYSANDFLAGS\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeysandflags);
            assert_eq!(input.args().len(), 2);

            let decoded = CommandGetkeysandflagsInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded.command, RedisJsonValue::String("GET".to_string()));
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*4\r\n$7\r\ncommand\r\n$15\r\ngetkeysandflags\r\n$3\r\nget\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeysandflags);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*4\r\n$7\r\nCOMMAND\r\n$15\r\nGETKEYSANDFLAGS\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::CommandGetkeysandflags);
            assert_eq!(input.args().len(), 2);
        }

        #[test]
        fn test_serialization() {
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("COMMAND GETKEYSANDFLAGS"));
            assert!(json.contains("GET"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // COMMAND GETKEYSANDFLAGS requires Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeysandflags_get() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysandflagsInput {
                        command: RedisJsonValue::String("GET".to_string()),
                        arg: Some(vec![RedisJsonValue::String("mykey".to_string())]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysandflagsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.keys()[0], "mykey");
                    // GET should have RO (read-only) flag
                    assert!(output.keys_with_flags()[0].flags.iter().any(|f| f == "RO" || f == "R"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeysandflags_set() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysandflagsInput {
                        command: RedisJsonValue::String("SET".to_string()),
                        arg: Some(vec![
                            RedisJsonValue::String("mykey".to_string()),
                            RedisJsonValue::String("myvalue".to_string()),
                        ]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysandflagsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.keys()[0], "mykey");
                    // SET should have OW (overwrite) or W flag
                    assert!(output.keys_with_flags()[0].flags.iter().any(|f| f == "OW" || f == "W"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeysandflags_mset() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let input = CommandGetkeysandflagsInput {
                        command: RedisJsonValue::String("MSET".to_string()),
                        arg: Some(vec![
                            RedisJsonValue::String("key1".to_string()),
                            RedisJsonValue::String("val1".to_string()),
                            RedisJsonValue::String("key2".to_string()),
                            RedisJsonValue::String("val2".to_string()),
                        ]),
                    };
                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = CommandGetkeysandflagsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2);
                    let keys = output.keys();
                    assert!(keys.contains(&"key1"));
                    assert!(keys.contains(&"key2"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeysandflags_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("testkey".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP2 returns array format
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = CommandGetkeysandflagsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_command_getkeysandflags_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            let input = CommandGetkeysandflagsInput {
                command: RedisJsonValue::String("GET".to_string()),
                arg: Some(vec![RedisJsonValue::String("testkey".to_string())]),
            };
            let result = ctx.raw(&input.command()).await.expect("raw failed");

            // RESP3 also uses array format for this command
            assert!(result.starts_with(b"*"), "RESP3 should return array");
            let output = CommandGetkeysandflagsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }
    }
}
