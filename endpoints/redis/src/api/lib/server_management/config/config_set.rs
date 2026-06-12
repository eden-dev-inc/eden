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

const API_INFO: ApiInfo<RedisApi, ConfigSetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ConfigSet, "Sets configuration parameters in-flight", ReqType::Write, true);

/// A parameter-value pair for CONFIG SET
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Set {
    parameter: RedisJsonValue,
    value: RedisJsonValue,
}

impl Set {
    pub fn new(parameter: RedisJsonValue, value: RedisJsonValue) -> Self {
        Self { parameter, value }
    }

    fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.parameter).arg(&self.value);
    }
}

/// See official Redis documentation for `CONFIG SET`
/// https://redis.io/docs/latest/commands/config-set/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ConfigSetInput {
    set: Vec<Set>,
}

impl Serialize for ConfigSetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigSetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("set", &self.set)?;
        state.end()
    }
}

impl_redis_operation!(ConfigSetInput, API_INFO, { set });

impl ConfigSetInput {
    pub fn new(set: Vec<Set>) -> Self {
        Self { set }
    }
}

impl RedisCommandInput for ConfigSetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for set in &self.set {
            set.cmd(&mut command);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Err(EpError::request("CONFIG SET requires pairs of parameter and value arguments".to_string()));
        }

        let mut set = Vec::new();
        for chunk in args.chunks(2) {
            set.push(Set { parameter: chunk[0].clone(), value: chunk[1].clone() });
        }

        Ok(Self { set })
    }
}

/// Output for Redis CONFIG SET command
///
/// Returns OK when the configuration has been set successfully.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ConfigSetOutput {
    success: bool,
}

impl Serialize for ConfigSetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConfigSetOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl ConfigSetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the configuration was set successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

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
                    return Err(EpError::parse(format!("unexpected CONFIG SET response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected CONFIG SET response: {:?}", other)));
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
        fn test_encode_command_single_param() {
            let input = ConfigSetInput {
                set: vec![Set::new(
                    RedisJsonValue::String("maxclients".into()),
                    RedisJsonValue::String("100".into()),
                )],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CONFIG"));
            assert!(cmd_str.contains("SET"));
            assert!(cmd_str.contains("maxclients"));
            assert!(cmd_str.contains("100"));
        }

        #[test]
        fn test_encode_command_multiple_params() {
            let input = ConfigSetInput {
                set: vec![
                    Set::new(RedisJsonValue::String("maxclients".into()), RedisJsonValue::String("100".into())),
                    Set::new(RedisJsonValue::String("timeout".into()), RedisJsonValue::String("0".into())),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("maxclients"));
            assert!(cmd_str.contains("timeout"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = ConfigSetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = ConfigSetOutput::decode(b"-ERR Unsupported CONFIG parameter\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("maxclients".into()), RedisJsonValue::String("100".into())];
            let input = ConfigSetInput::decode(args).unwrap();
            assert_eq!(input.set.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_pairs() {
            let args = vec![
                RedisJsonValue::String("maxclients".into()),
                RedisJsonValue::String("100".into()),
                RedisJsonValue::String("timeout".into()),
                RedisJsonValue::String("0".into()),
            ];
            let input = ConfigSetInput::decode(args).unwrap();
            assert_eq!(input.set.len(), 2);
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires pairs"));
        }

        #[test]
        fn test_decode_input_odd_number() {
            let args = vec![
                RedisJsonValue::String("maxclients".into()),
                RedisJsonValue::String("100".into()),
                RedisJsonValue::String("timeout".into()),
            ];
            let err = ConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires pairs"));
        }

        #[test]
        fn test_decode_input_single_arg() {
            let args = vec![RedisJsonValue::String("maxclients".into())];
            let err = ConfigSetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires pairs"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ConfigSetInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ConfigSetInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ConfigSet);
        }

        #[test]
        fn test_serialize_input() {
            let input = ConfigSetInput {
                set: vec![Set::new(
                    RedisJsonValue::String("maxclients".into()),
                    RedisJsonValue::String("100".into()),
                )],
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
            assert!(json.contains("\"set\""));
            assert!(json.contains("maxclients"));
        }

        #[test]
        fn test_serialize_output() {
            let output = ConfigSetOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_req_type_is_write() {
            assert_eq!(API_INFO.request_type, ReqType::Write);
        }

        #[test]
        fn test_new_constructors() {
            let set = Set::new(RedisJsonValue::String("key".into()), RedisJsonValue::String("value".into()));
            let input = ConfigSetInput::new(vec![set]);
            assert_eq!(input.set.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{ConfigGetInput, ConfigGetOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_set_and_get() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Get current value
                    let get_result = ctx
                        .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] }.command())
                        .await
                        .expect("get failed");
                    let original = ConfigGetOutput::decode(&get_result).expect("decode failed");
                    let original_value = original.get("maxclients").cloned();

                    // Set new value
                    let set_result = ctx
                        .raw(
                            &ConfigSetInput {
                                set: vec![Set::new(
                                    RedisJsonValue::String("maxclients".into()),
                                    RedisJsonValue::String("5000".into()),
                                )],
                            }
                            .command(),
                        )
                        .await
                        .expect("set failed");

                    let output = ConfigSetOutput::decode(&set_result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify new value
                    let get_result2 = ctx
                        .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("maxclients".into())] }.command())
                        .await
                        .expect("get failed");
                    let new_config = ConfigGetOutput::decode(&get_result2).expect("decode failed");
                    assert_eq!(new_config.get("maxclients"), Some(&"5000".to_string()));

                    // Restore original value if there was one
                    if let Some(orig) = original_value {
                        let _ = ctx
                            .raw(
                                &ConfigSetInput {
                                    set: vec![Set::new(RedisJsonValue::String("maxclients".into()), RedisJsonValue::String(orig))],
                                }
                                .command(),
                            )
                            .await;
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_set_invalid_param() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ConfigSetInput {
                                set: vec![Set::new(
                                    RedisJsonValue::String("nonexistent_config_xyz".into()),
                                    RedisJsonValue::String("value".into()),
                                )],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should return an error for invalid config parameter
                    assert!(result.starts_with(b"-"), "invalid config should return error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_set_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Get current value first
            let get_result = ctx
                .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("timeout".into())] }.command())
                .await
                .expect("get failed");
            let original = ConfigGetOutput::decode(&get_result).expect("decode failed");
            let original_value = original.get("timeout").cloned().unwrap_or("0".to_string());

            let result = ctx
                .raw(
                    &ConfigSetInput {
                        set: vec![Set::new(
                            RedisJsonValue::String("timeout".into()),
                            RedisJsonValue::String("100".into()),
                        )],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 should return simple string");
            let output = ConfigSetOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            // Restore original
            let _ = ctx
                .raw(
                    &ConfigSetInput {
                        set: vec![Set::new(
                            RedisJsonValue::String("timeout".into()),
                            RedisJsonValue::String(original_value),
                        )],
                    }
                    .command(),
                )
                .await;

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_config_set_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            // Get current value first
            let get_result = ctx
                .raw(&ConfigGetInput { parameter: vec![RedisJsonValue::String("timeout".into())] }.command())
                .await
                .expect("get failed");
            let original = ConfigGetOutput::decode(&get_result).expect("decode failed");
            let original_value = original.get("timeout").cloned().unwrap_or("0".to_string());

            let result = ctx
                .raw(
                    &ConfigSetInput {
                        set: vec![Set::new(
                            RedisJsonValue::String("timeout".into()),
                            RedisJsonValue::String("100".into()),
                        )],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = ConfigSetOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            // Restore original
            let _ = ctx
                .raw(
                    &ConfigSetInput {
                        set: vec![Set::new(
                            RedisJsonValue::String("timeout".into()),
                            RedisJsonValue::String(original_value),
                        )],
                    }
                    .command(),
                )
                .await;

            ctx.stop().await;
        }
    }
}
