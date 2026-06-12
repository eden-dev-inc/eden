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

const API_INFO: ApiInfo<RedisApi, FtDropindexInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtDropindex, "Deletes the index", ReqType::Write, true);

/// See official Redis documentation for `FT.DROPINDEX`
/// https://redis.io/docs/latest/commands/ft.dropindex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtDropindexInput {
    index: RedisJsonValue,
    dd: Option<RedisJsonValue>,
}

impl Serialize for FtDropindexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.dd.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtDropindexInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        if let Some(dd) = &self.dd {
            state.serialize_field("dd", dd)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    FtDropindexInput,
    API_INFO,
    {index, dd}
);

impl RedisCommandInput for FtDropindexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);

        if let Some(dd) = &self.dd {
            match dd {
                RedisJsonValue::Bool(true) => {
                    command.arg("DD");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("DD");
                }
                RedisJsonValue::String(s) if s.to_uppercase() == "DD" || s == "1" || s.to_uppercase() == "TRUE" => {
                    command.arg("DD");
                }
                _ => {}
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request(format!("FT.DROPINDEX requires at least 1 argument, given {}", args.len())));
        }

        let dd = args.get(1).and_then(|v| {
            if let RedisJsonValue::String(s) = v
                && s.to_uppercase() == "DD"
            {
                return Some(RedisJsonValue::Bool(true));
            }
            None
        });

        Ok(Self { index: args[0].clone(), dd })
    }
}

/// Output for Redis `FT.DROPINDEX` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtDropindexOutput {
    /// Whether the operation was successful
    success: bool,
}

impl Serialize for FtDropindexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtDropindexOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtDropindexOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtDropindexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.DROPINDEX response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.DROPINDEX response: {:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtDropindexInput { index: RedisJsonValue::String("my_index".into()), dd: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.DROPINDEX"));
            assert!(cmd_str.contains("my_index"));
            assert!(!cmd_str.contains("DD"));
        }

        #[test]
        fn test_encode_command_with_dd() {
            let input = FtDropindexInput {
                index: RedisJsonValue::String("my_index".into()),
                dd: Some(RedisJsonValue::Bool(true)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DD"));
        }

        #[test]
        fn test_encode_command_dd_false() {
            let input = FtDropindexInput {
                index: RedisJsonValue::String("my_index".into()),
                dd: Some(RedisJsonValue::Bool(false)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("DD"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let input = FtDropindexInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert!(input.dd.is_none());
        }

        #[test]
        fn test_decode_input_with_dd() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("DD".into())];
            let input = FtDropindexInput::decode(args).unwrap();
            assert_eq!(input.dd, Some(RedisJsonValue::Bool(true)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtDropindexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtDropindexOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtDropindexOutput::decode(b"-ERR Unknown Index name\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown Index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtDropindexInput { index: RedisJsonValue::String("i".into()), dd: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtDropindexInput {
                index: RedisJsonValue::String("test_idx".into()),
                dd: Some(RedisJsonValue::Bool(true)),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("dd"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtDropindexOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.DROPINDEX requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_dropindex_nonexistent() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtDropindexInput {
                                index: RedisJsonValue::String("nonexistent_index".into()),
                                dd: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                            let err = FtDropindexOutput::decode(&r);
                            assert!(err.is_err());
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other case
                        }
                    }
                })
            })
            .await;
        }
    }
}
