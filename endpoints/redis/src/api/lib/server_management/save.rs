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
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, SaveInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Save, "Synchronously saves the database(s) to disk", ReqType::Write, true);

/// See official Redis documentation for `SAVE`
/// https://redis.io/docs/latest/commands/save/
///
/// Note: SAVE blocks the server while saving. For production use, prefer BGSAVE.
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SaveInput {}

impl SaveInput {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for SaveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SaveInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(SaveInput, API_INFO);

impl RedisCommandInput for SaveInput {
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
            log_warn!(_ctx, "SAVE expects no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }
        Ok(Self::default())
    }
}

/// Output for Redis SAVE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SaveOutput {
    /// The response message (typically "OK" on success)
    message: String,
}

impl SaveOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    /// Get the response message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Check if the save was successful
    pub fn is_ok(&self) -> bool {
        self.message == "OK"
    }

    /// Decode the Redis protocol response into a SaveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SAVE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SAVE response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for SaveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SaveOutput", 1)?;
        state.serialize_field("message", &self.message)?;
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
            let input = SaveInput::new();
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nSAVE\r\n");
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = SaveInput::decode(args).unwrap();
            assert_eq!(format!("{:?}", input), "SaveInput");
        }

        #[test]
        fn test_decode_input_with_extra_args_succeeds() {
            // SAVE ignores extra args with a warning
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = SaveInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SaveInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SaveInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Save);
        }

        #[test]
        fn test_decode_ok_response() {
            let output = SaveOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_response() {
            let err = SaveOutput::decode(b"-ERR Background save already in progress\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = SaveOutput::new("OK".to_string());
            assert!(output.is_ok());
        }

        #[test]
        fn test_output_not_ok() {
            let output = SaveOutput::new("ERR".to_string());
            assert!(!output.is_ok());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_save_succeeds() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SaveInput::new().command()).await.expect("raw failed");

                    let output = SaveOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }
    }
}
