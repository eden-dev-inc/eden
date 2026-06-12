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

const API_INFO: ApiInfo<RedisApi, ModuleLoadInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ModuleLoad, "Loads a module", ReqType::Write, true);

/// See official Redis documentation for `MODULE LOAD`
/// https://redis.io/docs/latest/commands/module-load/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ModuleLoadInput {
    path: RedisJsonValue,
    args: Option<Vec<RedisJsonValue>>,
}

impl Serialize for ModuleLoadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ModuleLoadInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("path", &self.path)?;
        if let Some(args) = &self.args {
            state.serialize_field("args", &args)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ModuleLoadInput,
    API_INFO,
    {path, args}
);

impl RedisCommandInput for ModuleLoadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.path);

        if let Some(args) = &self.args {
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
            return Err(EpError::request("MODULE LOAD requires at least 1 argument".to_string()));
        }

        let path = args[0].clone();
        let args = if args.len() > 1 { Some(args[1..].to_vec()) } else { None };

        Ok(Self { path, args })
    }
}

/// Output for Redis MODULE LOAD command
///
/// Returns OK when the module is successfully loaded.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ModuleLoadOutput {
    /// Whether the module was loaded successfully
    success: bool,
}

impl ModuleLoadOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the module was loaded successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a ModuleLoadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE LOAD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE LOAD response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for ModuleLoadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleLoadOutput", 1)?;
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
            let input = ModuleLoadInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nMODULE\r\n$4\r\nLOAD\r\n"));
        }

        #[test]
        fn test_encode_command_with_args() {
            let input = ModuleLoadInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                args: Some(vec![RedisJsonValue::String("arg1".into()), RedisJsonValue::String("arg2".into())]),
            };
            let cmd = input.command();
            // Should have more elements due to args
            assert!(cmd.starts_with(b"*5\r\n"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ModuleLoadOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ModuleLoadOutput::decode(b"-ERR Module not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("/path/to/module.so".into())];
            let input = ModuleLoadInput::decode(args).unwrap();
            assert_eq!(input.path, RedisJsonValue::String("/path/to/module.so".into()));
            assert!(input.args.is_none());
        }

        #[test]
        fn test_decode_input_with_args() {
            let args = vec![
                RedisJsonValue::String("/path/to/module.so".into()),
                RedisJsonValue::String("arg1".into()),
                RedisJsonValue::String("arg2".into()),
            ];
            let input = ModuleLoadInput::decode(args).unwrap();
            assert_eq!(input.path, RedisJsonValue::String("/path/to/module.so".into()));
            assert_eq!(input.args.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ModuleLoadInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ModuleLoadInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                args: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ModuleLoadInput {
                path: RedisJsonValue::String("/path/to/module.so".into()),
                args: None,
            };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ModuleLoad);
        }
    }

    // Note: Integration tests for MODULE LOAD are limited because:
    // 1. Loading modules requires file system access to module .so files
    // 2. Most test environments don't have modules available
    // 3. Loading invalid modules can cause errors
    // The unit tests above verify the command encoding/decoding logic.
}
