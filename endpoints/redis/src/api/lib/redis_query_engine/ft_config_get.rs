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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtConfigGetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtConfigGet,
    "Retrieves runtime configuration options",
    ReqType::Read, // Fixed: This is a read operation, not write
    true,
);

/// See official Redis documentation for `FT.CONFIG GET`
/// https://redis.io/docs/latest/commands/ft.config-get/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtConfigGetInput {
    option: RedisJsonValue,
}

impl Serialize for FtConfigGetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtConfigGetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("option", &self.option)?;
        state.end()
    }
}

impl_redis_operation!(FtConfigGetInput, API_INFO, { option });

impl RedisCommandInput for FtConfigGetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.option);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FT.CONFIG GET requires 1 argument, given {}", args.len())));
        }

        Ok(Self { option: args[0].clone() })
    }
}

/// Output for Redis `FT.CONFIG GET` command.
///
/// Returns configuration option name-value pairs.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtConfigGetOutput {
    /// Configuration options as key-value pairs
    config: HashMap<String, RedisJsonValue>,
}

impl Serialize for FtConfigGetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtConfigGetOutput", 1)?;
        state.serialize_field("config", &self.config)?;
        state.end()
    }
}

impl FtConfigGetOutput {
    pub fn new(config: HashMap<String, RedisJsonValue>) -> Self {
        Self { config }
    }

    /// Get the configuration options
    pub fn config(&self) -> &HashMap<String, RedisJsonValue> {
        &self.config
    }

    /// Get a specific configuration value
    pub fn get(&self, key: &str) -> Option<&RedisJsonValue> {
        self.config.get(key)
    }

    /// Check if configuration is empty
    pub fn is_empty(&self) -> bool {
        self.config.is_empty()
    }

    /// Decode the Redis protocol response into a FtConfigGetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let config = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut config = HashMap::new();
                    // FT.CONFIG GET returns array of arrays: [[name, value], ...]
                    for item in arr {
                        if let Resp2Frame::Array(pair) = item
                            && pair.len() >= 2
                        {
                            let key = match &pair[0] {
                                Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                                Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
                                _ => continue,
                            };
                            let value = match &pair[1] {
                                Resp2Frame::BulkString(b) => RedisJsonValue::String(String::from_utf8(b.clone()).map_err(EpError::parse)?),
                                Resp2Frame::SimpleString(s) => {
                                    RedisJsonValue::String(String::from_utf8(s.clone()).map_err(EpError::parse)?)
                                }
                                Resp2Frame::Integer(i) => RedisJsonValue::Integer(*i),
                                Resp2Frame::Null => RedisJsonValue::Null,
                                _ => RedisJsonValue::Null,
                            };
                            config.insert(key, value);
                        }
                    }
                    config
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CONFIG GET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut config = HashMap::new();
                    for item in data {
                        if let Resp3Frame::Array { data: pair, .. } = item
                            && pair.len() >= 2
                        {
                            let key = match &pair[0] {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                                _ => continue,
                            };
                            let value = match &pair[1] {
                                Resp3Frame::BlobString { data, .. } => {
                                    RedisJsonValue::String(String::from_utf8(data.clone()).map_err(EpError::parse)?)
                                }
                                Resp3Frame::SimpleString { data, .. } => {
                                    RedisJsonValue::String(String::from_utf8(data.clone()).map_err(EpError::parse)?)
                                }
                                Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
                                Resp3Frame::Null => RedisJsonValue::Null,
                                _ => RedisJsonValue::Null,
                            };
                            config.insert(key, value);
                        }
                    }
                    config
                }
                Resp3Frame::Map { data, .. } => {
                    let mut config = HashMap::new();
                    for (k, v) in data {
                        let key = match k {
                            Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => continue,
                        };
                        let value = match v {
                            Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?),
                            Resp3Frame::SimpleString { data, .. } => {
                                RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)
                            }
                            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(data),
                            Resp3Frame::Null => RedisJsonValue::Null,
                            _ => RedisJsonValue::Null,
                        };
                        config.insert(key, value);
                    }
                    config
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CONFIG GET response: {:?}", other)));
                }
            },
        };

        Ok(Self { config })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FtConfigGetInput { option: RedisJsonValue::String("TIMEOUT".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.CONFIG"));
            assert!(cmd_str.contains("GET"));
            assert!(cmd_str.contains("TIMEOUT"));
        }

        #[test]
        fn test_encode_command_wildcard() {
            let input = FtConfigGetInput { option: RedisJsonValue::String("*".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("*"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("TIMEOUT".into())];
            let input = FtConfigGetInput::decode(args).unwrap();
            assert_eq!(input.option, RedisJsonValue::String("TIMEOUT".into()));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtConfigGetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = FtConfigGetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_output_empty_array() {
            // Empty config result
            let output = FtConfigGetOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtConfigGetOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtConfigGetInput { option: RedisJsonValue::String("*".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let mut config = HashMap::new();
            config.insert("TIMEOUT".into(), RedisJsonValue::Integer(500));
            let output = FtConfigGetOutput::new(config);
            assert!(!output.is_empty());
            assert_eq!(output.get("TIMEOUT"), Some(&RedisJsonValue::Integer(500)));
            assert_eq!(output.get("MISSING"), None);
        }

        #[test]
        fn test_serialize_input() {
            let input = FtConfigGetInput { option: RedisJsonValue::String("TIMEOUT".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("TIMEOUT"));
        }

        #[test]
        fn test_serialize_output() {
            let mut config = HashMap::new();
            config.insert("KEY".into(), RedisJsonValue::String("VALUE".into()));
            let output = FtConfigGetOutput::new(config);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("KEY"));
        }

        #[test]
        fn test_req_type_is_read() {
            // Verify that the API is correctly marked as Read
            assert_eq!(API_INFO.request_type, ReqType::Read);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.CONFIG GET requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_config_get_all() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtConfigGetInput { option: RedisJsonValue::String("*".into()) }.command()).await;

                    match result {
                        Ok(r) if !r.starts_with(b"-") => {
                            let _output = FtConfigGetOutput::decode(&r).expect("decode failed");
                        }
                        Ok(_) | Err(_) => {
                            // Module not available, skip
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_config_get_specific() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtConfigGetInput { option: RedisJsonValue::String("TIMEOUT".into()) }.command()).await;

                    match result {
                        Ok(r) if !r.starts_with(b"-") => {
                            let _output = FtConfigGetOutput::decode(&r).expect("decode failed");
                        }
                        Ok(_) | Err(_) => {
                            // Module not available, skip
                        }
                    }
                })
            })
            .await;
        }
    }
}
