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

const API_INFO: ApiInfo<RedisApi, MemoryMallocStatsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::MemoryMallocStats, "Returns the allocator statistics", ReqType::Read, true);

/// See official Redis documentation for `MEMORY MALLOC-STATS`
/// https://redis.io/docs/latest/commands/memory-malloc-stats/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MemoryMallocStatsInput {}

impl Serialize for MemoryMallocStatsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryMallocStatsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MemoryMallocStatsInput, API_INFO);

impl RedisCommandInput for MemoryMallocStatsInput {
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
                "MEMORY MALLOC-STATS expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis MEMORY MALLOC-STATS command
///
/// Returns internal allocator statistics as a string.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MemoryMallocStatsOutput {
    /// The allocator statistics string
    stats: String,
}

impl MemoryMallocStatsOutput {
    pub fn new(stats: String) -> Self {
        Self { stats }
    }

    /// Get the raw allocator statistics string
    pub fn stats(&self) -> &str {
        &self.stats
    }

    /// Check if stats are available (non-empty)
    pub fn has_stats(&self) -> bool {
        !self.stats.is_empty()
    }

    /// Decode the Redis protocol response into a MemoryMallocStatsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let stats = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY MALLOC-STATS response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected MEMORY MALLOC-STATS response: {:?}", other)));
                }
            },
        };

        Ok(Self { stats })
    }
}

impl Serialize for MemoryMallocStatsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryMallocStatsOutput", 1)?;
        state.serialize_field("stats", &self.stats)?;
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
            let input = MemoryMallocStatsInput {};
            // MALLOC-STATS is 12 characters
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nMEMORY\r\n$12\r\nMALLOC-STATS\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let stats_text = "jemalloc stats here";
            let response = format!("${}\r\n{}\r\n", stats_text.len(), stats_text);
            let output = MemoryMallocStatsOutput::decode(response.as_bytes()).unwrap();
            assert!(output.has_stats());
            assert_eq!(output.stats(), stats_text);
        }

        #[test]
        fn test_decode_empty_string() {
            let output = MemoryMallocStatsOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(!output.has_stats());
            assert_eq!(output.stats(), "");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MemoryMallocStatsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = MemoryMallocStatsInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns() {
            let input = MemoryMallocStatsInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MemoryMallocStatsInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = MemoryMallocStatsInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::MemoryMallocStats);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_malloc_stats_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryMallocStatsInput {}.command()).await.expect("raw failed");

                    let output = MemoryMallocStatsOutput::decode(&result).expect("decode failed");
                    // Should return some allocator stats (may be empty on some builds)
                    // Just verify decode works
                    let _ = output.stats();
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_malloc_stats_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&MemoryMallocStatsInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = MemoryMallocStatsOutput::decode(&result).expect("decode failed");
            let _ = output.stats();

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_malloc_stats_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&MemoryMallocStatsInput {}.command()).await.expect("raw failed");

            let output = MemoryMallocStatsOutput::decode(&result).expect("decode failed");
            let _ = output.stats();

            ctx.stop().await;
        }
    }
}
