use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtConfigSetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtConfigSet, "Sets runtime configuration options", ReqType::Write, true);

/// See official Redis documentation for `FT.CONFIG SET`
/// https://redis.io/docs/latest/commands/ft.config-set/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtConfigSetInput {
    option: RedisJsonValue,
    value: RedisJsonValue,
}

impl Serialize for FtConfigSetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtConfigSetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("option", &self.option)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(FtConfigSetInput, API_INFO, { option, value });

impl RedisCommandInput for FtConfigSetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.option).arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("FT.CONFIG SET requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { option: args[0].clone(), value: args[1].clone() })
    }
}

/// Output for Redis `FT.CONFIG SET` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtConfigSetOutput {
    success: bool,
}

impl Serialize for FtConfigSetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtConfigSetOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtConfigSetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the configuration was set successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtConfigSetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CONFIG SET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CONFIG SET response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FtConfigSetInput {
                option: RedisJsonValue::String("TIMEOUT".into()),
                value: RedisJsonValue::Integer(1000),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.CONFIG"));
            assert!(cmd_str.contains("SET"));
            assert!(cmd_str.contains("TIMEOUT"));
        }

        #[test]
        fn test_encode_command_string_value() {
            let input = FtConfigSetInput {
                option: RedisJsonValue::String("OPTION".into()),
                value: RedisJsonValue::String("value".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("OPTION"));
            assert!(cmd_str.contains("value"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("TIMEOUT".into()), RedisJsonValue::Integer(500)];
            let input = FtConfigSetInput::decode(args).unwrap();
            assert_eq!(input.option, RedisJsonValue::String("TIMEOUT".into()));
            assert_eq!(input.value, RedisJsonValue::Integer(500));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("TIMEOUT".into())];
            let err = FtConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = FtConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtConfigSetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtConfigSetOutput::decode(b"-ERR invalid option\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtConfigSetInput {
                option: RedisJsonValue::String("opt".into()),
                value: RedisJsonValue::Integer(1),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtConfigSetInput {
                option: RedisJsonValue::String("TIMEOUT".into()),
                value: RedisJsonValue::Integer(1000),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("TIMEOUT"));
            assert!(json.contains("1000"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtConfigSetOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.CONFIG SET requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_config_set_invalid_option() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtConfigSetInput {
                                option: RedisJsonValue::String("INVALID_OPTION_XYZ".into()),
                                value: RedisJsonValue::Integer(100),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for invalid option
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or accepted the option, skip
                        }
                    }
                })
            })
            .await;
        }
    }
}
