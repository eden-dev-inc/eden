use crate::api::lib::server_management::module::Config;
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

const API_INFO: ApiInfo<RedisApi, ModuleLoadexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ModuleLoadex,
    "Loads a module using extended parameters",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `MODULE LOADEX`
/// https://redis.io/docs/latest/commands/module-loadex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ModuleLoadexInput {
    path: RedisJsonValue,
    configs: Option<Vec<Config>>,
    args: Option<Vec<RedisJsonValue>>,
}

impl Serialize for ModuleLoadexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.configs.is_some() {
            fields += 1;
        }
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ModuleLoadexInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("path", &self.path)?;
        if let Some(configs) = &self.configs {
            state.serialize_field("configs", &configs)?;
        }
        if let Some(args) = &self.args {
            state.serialize_field("args", &args)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ModuleLoadexInput,
    API_INFO,
    {path, configs, args}
);

impl RedisCommandInput for ModuleLoadexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.path);

        if let Some(configs) = &self.configs {
            for config in configs {
                command.arg("CONFIG").arg(&config.name).arg(&config.value);
            }
        }

        if let Some(args) = &self.args {
            command.arg("ARGS");
            for arg in args {
                command.arg(arg);
            }
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("MODULE LOADEX requires at least 1 argument".to_string()));
        }

        let path = args[0].clone();
        let mut configs = None;
        let mut module_args = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "CONFIG" => {
                        i += 1;
                        let mut config_list = Vec::new();
                        while i + 1 < args.len() {
                            if let RedisJsonValue::String(next) = &args[i]
                                && (next.to_uppercase() == "ARGS" || next.to_uppercase() == "CONFIG")
                            {
                                break;
                            }
                            config_list.push(Config { name: args[i].clone(), value: args[i + 1].clone() });
                            i += 2;
                        }
                        if !config_list.is_empty() {
                            configs = Some(config_list);
                        }
                    }
                    "ARGS" => {
                        i += 1;
                        if i < args.len() {
                            module_args = Some(args[i..].to_vec());
                        }
                        break;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { path, configs, args: module_args })
    }
}

/// Output for Redis MODULE LOADEX command
///
/// Returns OK when the module is successfully loaded with extended parameters.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ModuleLoadexOutput {
    /// Whether the module was loaded successfully
    success: bool,
}

impl ModuleLoadexOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the module was loaded successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a ModuleLoadexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE LOADEX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE LOADEX response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for ModuleLoadexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleLoadexOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: None,
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nMODULE\r\n$6\r\nLOADEX\r\n"));
        }

        #[test]
        fn test_encode_command_with_config() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: Some(vec![Config {
                    name: RedisJsonValue::String("setting1".into()),
                    value: RedisJsonValue::String("value1".into()),
                }]),
                args: None,
            };
            let cmd = input.command();
            // Should contain CONFIG keyword
            assert!(cmd.windows(6).any(|w| w == b"CONFIG"));
        }

        #[test]
        fn test_encode_command_with_args() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: None,
                args: Some(vec![RedisJsonValue::String("arg1".into())]),
            };
            let cmd = input.command();
            // Should contain ARGS keyword
            assert!(cmd.windows(4).any(|w| w == b"ARGS"));
        }

        #[test]
        fn test_encode_command_with_config_and_args() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: Some(vec![Config {
                    name: RedisJsonValue::String("opt".into()),
                    value: RedisJsonValue::String("val".into()),
                }]),
                args: Some(vec![RedisJsonValue::String("arg1".into())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"CONFIG"));
            assert!(cmd.windows(4).any(|w| w == b"ARGS"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ModuleLoadexOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ModuleLoadexOutput::decode(b"-ERR Module not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("/path/to/module.so".into())];
            let input = ModuleLoadexInput::decode(args).unwrap();
            assert_eq!(input.path, RedisJsonValue::String("/path/to/module.so".into()));
            assert!(input.configs.is_none());
            assert!(input.args.is_none());
        }

        #[test]
        fn test_decode_input_with_config() {
            let args = vec![
                RedisJsonValue::String("/path/to/module.so".into()),
                RedisJsonValue::String("CONFIG".into()),
                RedisJsonValue::String("setting1".into()),
                RedisJsonValue::String("value1".into()),
            ];
            let input = ModuleLoadexInput::decode(args).unwrap();
            assert!(input.configs.is_some());
            assert_eq!(input.configs.as_ref().unwrap().len(), 1);
        }

        #[test]
        fn test_decode_input_with_args() {
            let args = vec![
                RedisJsonValue::String("/path/to/module.so".into()),
                RedisJsonValue::String("ARGS".into()),
                RedisJsonValue::String("arg1".into()),
                RedisJsonValue::String("arg2".into()),
            ];
            let input = ModuleLoadexInput::decode(args).unwrap();
            assert!(input.args.is_some());
            assert_eq!(input.args.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ModuleLoadexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: None,
                args: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: None,
                args: None,
            };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ModuleLoadex);
        }

        #[test]
        fn test_serialize_no_duplicate_configs() {
            let input = ModuleLoadexInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                configs: Some(vec![Config {
                    name: RedisJsonValue::String("opt".into()),
                    value: RedisJsonValue::String("val".into()),
                }]),
                args: None,
            };
            let json = serde_json::to_string(&input).unwrap();
            // Count occurrences of "configs" - should be exactly 1
            let count = json.matches("configs").count();
            assert_eq!(count, 1, "configs should appear exactly once in serialized output");
        }
    }

    // Note: Integration tests for MODULE LOADEX are limited because:
    // 1. Loading modules requires file system access to module .so files
    // 2. Most test environments don't have modules available
    // 3. LOADEX is available since Redis 7.0
    // The unit tests above verify the command encoding/decoding logic.
}
