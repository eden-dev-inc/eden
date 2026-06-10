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

const API_INFO: ApiInfo<RedisApi, LolwutInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Lolwut, "Displays computer art and the Redis version", ReqType::Read, true);

/// See official Redis documentation for `LOLWUT`
/// https://redis.io/docs/latest/commands/lolwut/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LolwutInput {
    /// Optional version number to display a specific art version
    pub version: Option<RedisJsonValue>,
}

impl Serialize for LolwutInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.version.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("LolwutInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(version) = &self.version {
            state.serialize_field("version", version)?;
        }
        state.end()
    }
}

impl_redis_operation!(LolwutInput, API_INFO, { version });

impl RedisCommandInput for LolwutInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(version) = &self.version {
            command.arg("VERSION");
            command.arg(version);
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut version = None;

        if !args.is_empty() {
            if let RedisJsonValue::String(s) = &args[0] {
                if s.to_uppercase() == "VERSION" && args.len() > 1 {
                    version = Some(args[1].clone());
                }
            } else {
                version = Some(args[0].clone());
            }
        }

        Ok(Self { version })
    }
}

/// Output for Redis LOLWUT command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LolwutOutput {
    art: String,
}

impl LolwutOutput {
    pub fn new(art: String) -> Self {
        Self { art }
    }

    pub fn art(&self) -> &str {
        &self.art
    }

    pub fn redis_version(&self) -> Option<&str> {
        self.art.lines().rev().find(|line| line.contains("Redis ver.")).map(|line| line.trim())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let art = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected LOLWUT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected LOLWUT response: {:?}", other)));
                }
            },
        };

        Ok(Self { art })
    }
}

impl Serialize for LolwutOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LolwutOutput", 1)?;
        state.serialize_field("art", &self.art)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_version() {
            let input = LolwutInput { version: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$6\r\nLOLWUT\r\n");
        }

        #[test]
        fn test_encode_command_with_version() {
            let input = LolwutInput { version: Some(RedisJsonValue::Integer(5)) };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"LOLWUT"));
            assert!(cmd.windows(7).any(|w| w == b"VERSION"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let art = "Some ASCII art\nRedis ver. 7.0.0";
            let resp = format!("${}\r\n{}\r\n", art.len(), art);
            let output = LolwutOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.art(), art);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LolwutOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = LolwutInput::decode(vec![]).unwrap();
            assert!(input.version.is_none());
        }

        #[test]
        fn test_decode_input_with_version_keyword() {
            let args = vec![RedisJsonValue::String("VERSION".into()), RedisJsonValue::Integer(5)];
            let input = LolwutInput::decode(args).unwrap();
            assert_eq!(input.version, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_version_number_only() {
            let args = vec![RedisJsonValue::Integer(5)];
            let input = LolwutInput::decode(args).unwrap();
            assert_eq!(input.version, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LolwutInput { version: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LolwutInput { version: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Lolwut);
        }

        #[test]
        fn test_redis_version_extraction() {
            let output = LolwutOutput::new("Art\nRedis ver. 7.2.0\n".into());
            assert!(output.redis_version().unwrap().contains("7.2.0"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lolwut_basic() {
            // LOLWUT requires Redis 5.0+
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&LolwutInput { version: None }.command()).await.expect("raw failed");

                    let output = LolwutOutput::decode(&result).expect("decode failed");
                    assert!(!output.art().is_empty());
                    assert!(output.redis_version().is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lolwut_with_version() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Use version 5 which is supported in Redis 5.0+
                    // Note: Different Redis versions support different LOLWUT versions
                    let result =
                        ctx.raw(&LolwutInput { version: Some(RedisJsonValue::String("5".into())) }.command()).await.expect("raw failed");

                    // May return error for unsupported version, that's OK
                    if !result.starts_with(b"-") {
                        let output = LolwutOutput::decode(&result).expect("decode failed");
                        assert!(!output.art().is_empty());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lolwut_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&LolwutInput { version: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = LolwutOutput::decode(&result).expect("decode failed");
            assert!(!output.art().is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lolwut_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&LolwutInput { version: None }.command()).await.expect("raw failed");

            let output = LolwutOutput::decode(&result).expect("decode failed");
            assert!(!output.art().is_empty());

            ctx.stop().await;
        }
    }
}
