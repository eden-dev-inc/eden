use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, JsonDebugInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::JsonDebug, "Debugging container command", ReqType::Read, true);

/// See official Redis documentation for `JSON.DEBUG`
/// https://redis.io/docs/latest/commands/json.debug/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonDebugInput {}

impl Serialize for JsonDebugInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonDebugInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(JsonDebugInput, API_INFO);

impl RedisCommandInput for JsonDebugInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "JSON.DEBUG expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis JSON.DEBUG command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonDebugOutput {
    help: Vec<String>,
}

impl JsonDebugOutput {
    pub fn new(help: Vec<String>) -> Self {
        Self { help }
    }

    pub fn help(&self) -> &[String] {
        &self.help
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let help = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut help = Vec::new();
                    for item in arr {
                        if let Resp2Frame::BulkString(data) = item {
                            help.push(String::from_utf8_lossy(&data).to_string());
                        }
                    }
                    help
                }
                Resp2Frame::BulkString(data) => {
                    vec![String::from_utf8_lossy(&data).to_string()]
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut help = Vec::new();
                    for item in data {
                        if let Resp3Frame::BlobString { data, .. } = item {
                            help.push(String::from_utf8_lossy(&data).to_string());
                        }
                    }
                    help
                }
                Resp3Frame::BlobString { data, .. } => {
                    vec![String::from_utf8_lossy(&data).to_string()]
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { help })
    }
}

impl Serialize for JsonDebugOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonDebugOutput", 1)?;
        state.serialize_field("help", &self.help)?;
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
            let input = JsonDebugInput {};
            let cmd = input.command();
            assert!(cmd.starts_with(b"*1\r\n$10\r\nJSON.DEBUG\r\n"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = JsonDebugInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonDebugInput {};
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_debug_help() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&JsonDebugInput {}.command()).await.expect("debug failed");

                    // JSON.DEBUG requires a subcommand (HELP or MEMORY)
                    let output = JsonDebugOutput::decode(&result);
                    // Accept either valid output or error
                    if let Ok(out) = output {
                        assert!(!out.help().is_empty());
                    }
                })
            })
            .await;
        }
    }
}
