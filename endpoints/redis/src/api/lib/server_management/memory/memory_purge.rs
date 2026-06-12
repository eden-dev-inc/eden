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

const API_INFO: ApiInfo<RedisApi, MemoryPurgeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::MemoryPurge, "Asks the allocator to release memory", ReqType::Write, true);

/// See official Redis documentation for `MEMORY PURGE`
/// https://redis.io/docs/latest/commands/memory-purge/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MemoryPurgeInput {}

impl Serialize for MemoryPurgeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryPurgeInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MemoryPurgeInput, API_INFO);

impl RedisCommandInput for MemoryPurgeInput {
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
                "MEMORY PURGE expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis MEMORY PURGE command
///
/// Returns OK when the memory purge operation completes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MemoryPurgeOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl MemoryPurgeOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the purge was successful
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a MemoryPurgeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY PURGE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.to_uppercase() == "OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY PURGE response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for MemoryPurgeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryPurgeOutput", 1)?;
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
            let input = MemoryPurgeInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nMEMORY\r\n$5\r\nPURGE\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = MemoryPurgeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MemoryPurgeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = MemoryPurgeInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns() {
            // Should succeed but log a warning
            let input = MemoryPurgeInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MemoryPurgeInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = MemoryPurgeInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::MemoryPurge);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_purge_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryPurgeInput {}.command()).await.expect("raw failed");

                    let output = MemoryPurgeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_purge_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&MemoryPurgeInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return simple string OK");
            let output = MemoryPurgeOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_purge_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&MemoryPurgeInput {}.command()).await.expect("raw failed");

            let output = MemoryPurgeOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
