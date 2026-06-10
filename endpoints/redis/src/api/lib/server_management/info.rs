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

const API_INFO: ApiInfo<RedisApi, InfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Info,
    "Returns information and statistics about the server",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `INFO`
/// https://redis.io/docs/latest/commands/info/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct InfoInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    section: Option<Vec<RedisJsonValue>>,
}

impl Serialize for InfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.section.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("InfoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(section) = &self.section {
            state.serialize_field("section", section)?;
        }
        state.end()
    }
}

impl_redis_operation!(InfoInput, API_INFO, { section });

impl InfoInput {
    pub fn new(section: Option<Vec<RedisJsonValue>>) -> Self {
        Self { section }
    }
}

impl RedisCommandInput for InfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(section) = &self.section {
            command.arg(section);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let section = if args.is_empty() { None } else { Some(args) };

        Ok(Self { section })
    }
}

/// Output for Redis INFO command
///
/// Returns server information as a bulk string.
///
/// See official Redis documentation for `INFO`
/// https://redis.io/docs/latest/commands/info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct InfoOutput {
    bulk_string: String,
}

impl Serialize for InfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("InfoOutput", 1)?;
        state.serialize_field("info", &self.bulk_string)?;
        state.end()
    }
}

impl InfoOutput {
    pub fn new(bulk_string: String) -> Self {
        Self { bulk_string }
    }

    pub fn bulk_string(&self) -> &str {
        &self.bulk_string
    }

    pub fn get_field(&self, field: &str) -> Option<String> {
        self.bulk_string
            .lines()
            .find(|line| line.starts_with(field))
            .and_then(|line| line.split_once(':'))
            .map(|(_, value)| value.trim().to_string())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let bulk_string = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(&b).into_owned(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("INFO must return bulk or simple string: {:?}", other,)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
                    String::from_utf8_lossy(&data).into_owned()
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("INFO must return bulk or simple string: {:?}", other,)));
                }
            },
        };

        Ok(Self { bulk_string })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_section() {
            let input = InfoInput::default();
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nINFO\r\n");
        }

        #[test]
        fn test_encode_command_with_section() {
            let input = InfoInput { section: Some(vec![RedisJsonValue::String("server".into())]) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("INFO"));
            assert!(cmd_str.contains("server"));
        }

        #[test]
        fn test_decode_output_bulk_string() {
            let info_response = b"$50\r\n# Server\r\nredis_version:7.0.0\r\nredis_git_sha1:00000000\r\n";
            let output = InfoOutput::decode(info_response).unwrap();
            assert!(output.bulk_string().contains("redis_version"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = InfoOutput::decode(b"-ERR unknown section\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_get_field() {
            let output = InfoOutput::new("redis_version:7.0.0\nredis_mode:standalone".into());
            assert_eq!(output.get_field("redis_version"), Some("7.0.0".into()));
            assert_eq!(output.get_field("redis_mode"), Some("standalone".into()));
            assert_eq!(output.get_field("nonexistent"), None);
        }

        #[test]
        fn test_get_field_with_spaces() {
            let output = InfoOutput::new("some_field:  value with spaces  ".into());
            assert_eq!(output.get_field("some_field"), Some("value with spaces".into()));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = InfoInput::decode(vec![]).unwrap();
            assert!(input.section.is_none());
        }

        #[test]
        fn test_decode_input_with_section() {
            let input = InfoInput::decode(vec![RedisJsonValue::String("replication".into())]).unwrap();
            assert!(input.section.is_some());
            assert_eq!(input.section.unwrap().len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_sections() {
            let input = InfoInput::decode(vec![RedisJsonValue::String("server".into()), RedisJsonValue::String("clients".into())]).unwrap();
            assert!(input.section.is_some());
            assert_eq!(input.section.unwrap().len(), 2);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = InfoInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = InfoInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Info);
        }

        #[test]
        fn test_serialize_input_no_section() {
            let input = InfoInput { section: None };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("INFO") || json.contains("Info"));
            assert!(!json.contains("section"));
        }

        #[test]
        fn test_serialize_input_with_section() {
            let input = InfoInput { section: Some(vec![RedisJsonValue::String("server".into())]) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("section"));
        }

        #[test]
        fn test_serialize_output() {
            let output = InfoOutput::new("redis_version:7.0.0".into());
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("info"));
        }

        #[test]
        fn test_req_type_is_read() {
            assert_eq!(API_INFO.request_type, ReqType::Read);
        }

        #[test]
        fn test_new_constructor() {
            let input = InfoInput::new(Some(vec![RedisJsonValue::String("memory".into())]));
            assert!(input.section.is_some());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_default() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&InfoInput::default().command()).await.expect("raw failed");

                    let output = InfoOutput::decode(&result).expect("decode failed");

                    // Default INFO should contain server section
                    assert!(output.bulk_string().contains("redis_version"), "should contain redis_version");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_server_section() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&InfoInput { section: Some(vec![RedisJsonValue::String("server".into())]) }.command())
                        .await
                        .expect("raw failed");

                    let output = InfoOutput::decode(&result).expect("decode failed");

                    // Server section should contain redis_version
                    assert!(output.get_field("redis_version").is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_memory_section() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&InfoInput { section: Some(vec![RedisJsonValue::String("memory".into())]) }.command())
                        .await
                        .expect("raw failed");

                    let output = InfoOutput::decode(&result).expect("decode failed");

                    // Memory section should contain used_memory
                    assert!(output.bulk_string().contains("used_memory"), "should contain used_memory");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_get_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&InfoInput { section: Some(vec![RedisJsonValue::String("server".into())]) }.command())
                        .await
                        .expect("raw failed");

                    let output = InfoOutput::decode(&result).expect("decode failed");
                    let version = output.get_field("redis_version");
                    assert!(version.is_some(), "should have redis_version field");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&InfoInput::default().command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = InfoOutput::decode(&result).expect("decode failed");
            assert!(!output.bulk_string().is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_info_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&InfoInput::default().command()).await.expect("raw failed");

            let output = InfoOutput::decode(&result).expect("decode failed");
            assert!(!output.bulk_string().is_empty());

            ctx.stop().await;
        }
    }
}
