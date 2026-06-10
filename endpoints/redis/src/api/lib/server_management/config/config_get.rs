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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ConfigGetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ConfigGet,
    "Returns the effective values of configuration parameters",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CONFIG GET`
/// https://redis.io/docs/latest/commands/config-get/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ConfigGetInput {
    pub(crate) parameter: Vec<RedisJsonValue>,
}

impl Serialize for ConfigGetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigGetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("parameter", &self.parameter)?;
        state.end()
    }
}

impl_redis_operation!(ConfigGetInput, API_INFO, { parameter });

impl ConfigGetInput {
    pub fn new(parameter: Vec<RedisJsonValue>) -> Self {
        Self { parameter }
    }
}

impl RedisCommandInput for ConfigGetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.parameter);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CONFIG GET requires at least 1 parameter".to_string()));
        }

        Ok(Self { parameter: args })
    }
}

/// Output for Redis CONFIG GET command
///
/// Returns configuration option name-value pairs.
///
/// See official Redis documentation for `CONFIG GET`
/// https://redis.io/docs/latest/commands/config-get/
#[derive(Debug, Clone)]
pub struct ConfigGetOutput {
    config: HashMap<String, String>,
}

impl Serialize for ConfigGetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigGetOutput", 1)?;
        state.serialize_field("config", &self.config)?;
        state.end()
    }
}

impl ConfigGetOutput {
    pub fn new(config: HashMap<String, String>) -> Self {
        Self { config }
    }

    /// Get a specific configuration value
    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    /// Get the entire configuration map
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Check if configuration is empty
    pub fn is_empty(&self) -> bool {
        self.config.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let pairs = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    if arr.len() % 2 != 0 {
                        return Err(EpError::parse("CONFIG GET must return even number of elements"));
                    }
                    arr.chunks(2)
                        .map(|chunk| {
                            let key = match &chunk[0] {
                                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).into_owned(),
                                _ => return Err(EpError::parse("expected string for config key")),
                            };
                            let value = match &chunk[1] {
                                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).into_owned(),
                                _ => {
                                    return Err(EpError::parse("expected string for config value"));
                                }
                            };
                            Ok((key, value))
                        })
                        .collect::<Result<HashMap<_, _>, _>>()?
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("expected array for CONFIG GET response, got: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Map { data, .. } => data
                    .into_iter()
                    .map(|(k, v)| {
                        let key = match k {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8_lossy(&data).into_owned()
                            }
                            other => {
                                return Err(EpError::parse(format!("expected string for config key, got: {:?}", other)));
                            }
                        };
                        let value = match v {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8_lossy(&data).into_owned()
                            }
                            other => {
                                return Err(EpError::parse(format!("expected string for config value, got: {:?}", other)));
                            }
                        };
                        Ok((key, value))
                    })
                    .collect::<Result<HashMap<_, _>, _>>()?,
                Resp3Frame::Array { data, .. } => {
                    if data.len() % 2 != 0 {
                        return Err(EpError::parse("CONFIG GET must return even number of elements"));
                    }
                    data.chunks(2)
                        .map(|chunk| {
                            let key = match &chunk[0] {
                                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                    String::from_utf8_lossy(data).into_owned()
                                }
                                _ => return Err(EpError::parse("expected string for config key")),
                            };
                            let value = match &chunk[1] {
                                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                    String::from_utf8_lossy(data).into_owned()
                                }
                                _ => {
                                    return Err(EpError::parse("expected string for config value"));
                                }
                            };
                            Ok((key, value))
                        })
                        .collect::<Result<HashMap<_, _>, _>>()?
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("expected array or map for CONFIG GET response, got: {:?}", other)));
                }
            },
        };

        Ok(Self { config: pairs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_param() {
            let input = ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CONFIG"));
            assert!(cmd_str.contains("GET"));
            assert!(cmd_str.contains("maxclients"));
        }

        #[test]
        fn test_encode_command_wildcard() {
            let input = ConfigGetInput { parameter: vec![RedisJsonValue::String("*".into())] };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("*"));
        }

        #[test]
        fn test_decode_output_resp2_array() {
            // RESP2 format: array of alternating key-value pairs
            let resp = b"*4\r\n$10\r\nmaxclients\r\n$5\r\n10000\r\n$7\r\ntimeout\r\n$1\r\n0\r\n";
            let output = ConfigGetOutput::decode(resp).unwrap();
            assert_eq!(output.get("maxclients"), Some(&"10000".to_string()));
            assert_eq!(output.get("timeout"), Some(&"0".to_string()));
        }

        #[test]
        fn test_decode_output_empty_array() {
            let output = ConfigGetOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = ConfigGetOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("maxclients".into())];
            let input = ConfigGetInput::decode(args).unwrap();
            assert_eq!(input.parameter.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_params() {
            let args = vec![
                RedisJsonValue::String("maxclients".into()),
                RedisJsonValue::String("timeout".into()),
            ];
            let input = ConfigGetInput::decode(args).unwrap();
            assert_eq!(input.parameter.len(), 2);
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ConfigGetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 parameter"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ConfigGetInput { parameter: vec![RedisJsonValue::String("*".into())] };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ConfigGetInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ConfigGet);
        }

        #[test]
        fn test_serialize_input() {
            let input = ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("parameter"));
            assert!(json.contains("maxclients"));
        }

        #[test]
        fn test_serialize_output() {
            let mut config = HashMap::new();
            config.insert("maxclients".into(), "10000".into());
            let output = ConfigGetOutput::new(config);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("config"));
            assert!(json.contains("maxclients"));
        }

        #[test]
        fn test_req_type_is_read() {
            assert_eq!(API_INFO.request_type, ReqType::Read);
        }

        #[test]
        fn test_new_constructor() {
            let input = ConfigGetInput::new(vec![RedisJsonValue::String("*".into())]);
            assert_eq!(input.parameter.len(), 1);
        }

        #[test]
        fn test_output_accessors() {
            let mut config = HashMap::new();
            config.insert("key1".into(), "value1".into());
            config.insert("key2".into(), "value2".into());
            let output = ConfigGetOutput::new(config);

            assert!(!output.is_empty());
            assert_eq!(output.get("key1"), Some(&"value1".to_string()));
            assert_eq!(output.get("nonexistent"), None);
            assert_eq!(output.config().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_single_param() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = ConfigGetOutput::decode(&result).expect("decode failed");
                    assert!(output.get("maxclients").is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_wildcard() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("*".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = ConfigGetOutput::decode(&result).expect("decode failed");
                    // Wildcard should return many config options
                    assert!(!output.is_empty());
                    // Should contain common config options
                    assert!(output.get("maxclients").is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("max*".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = ConfigGetOutput::decode(&result).expect("decode failed");
                    // All keys should start with "max"
                    for key in output.config().keys() {
                        assert!(key.starts_with("max"), "key '{}' should start with 'max'", key);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ConfigGetInput {
                                parameter: vec![RedisJsonValue::String("nonexistent_config_option_xyz".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ConfigGetOutput::decode(&result).expect("decode failed");
                    // Non-existent config should return empty
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ConfigGetOutput::decode(&result).expect("decode failed");
            assert!(output.get("maxclients").is_some());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_get_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] }.command())
                .await
                .expect("raw failed");

            // RESP3 can return map or array
            let output = ConfigGetOutput::decode(&result).expect("decode failed");
            assert!(output.get("maxclients").is_some());

            ctx.stop().await;
        }
    }
}
