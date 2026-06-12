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

const API_INFO: ApiInfo<RedisApi, ModuleUnloadInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ModuleUnload, "Unloads a module", ReqType::Write, true);

/// See official Redis documentation for `MODULE UNLOAD`
/// https://redis.io/docs/latest/commands/module-unload/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ModuleUnloadInput {
    name: RedisJsonValue,
}

impl Serialize for ModuleUnloadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleUnloadInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("name", &self.name)?;
        state.end()
    }
}

impl_redis_operation!(ModuleUnloadInput, API_INFO, { name });

impl RedisCommandInput for ModuleUnloadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.name);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("MODULE UNLOAD requires 1 argument, given {}", args.len())));
        }

        Ok(Self { name: args[0].clone() })
    }
}

/// Output for Redis MODULE UNLOAD command
///
/// Returns OK when the module is successfully unloaded.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ModuleUnloadOutput {
    /// Whether the module was unloaded successfully
    success: bool,
}

impl ModuleUnloadOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the module was unloaded successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a ModuleUnloadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE UNLOAD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MODULE UNLOAD response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for ModuleUnloadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleUnloadOutput", 1)?;
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
        fn test_encode_command() {
            let input = ModuleUnloadInput { name: RedisJsonValue::String("mymodule".into()) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nMODULE\r\n$6\r\nUNLOAD\r\n$8\r\nmymodule\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ModuleUnloadOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ModuleUnloadOutput::decode(b"-ERR No such module\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mymodule".into())];
            let input = ModuleUnloadInput::decode(args).unwrap();
            assert_eq!(input.name, RedisJsonValue::String("mymodule".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ModuleUnloadInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_fails() {
            let args = vec![RedisJsonValue::String("mod1".into()), RedisJsonValue::String("mod2".into())];
            let err = ModuleUnloadInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ModuleUnloadInput { name: RedisJsonValue::String("mymodule".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ModuleUnloadInput { name: RedisJsonValue::String("mymodule".into()) };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ModuleUnload);
        }
    }

    // Note: Integration tests for MODULE UNLOAD are limited because:
    // 1. Unloading modules requires a module to be loaded first
    // 2. Most test environments don't have modules available
    // The unit tests above verify the command encoding/decoding logic.
}
