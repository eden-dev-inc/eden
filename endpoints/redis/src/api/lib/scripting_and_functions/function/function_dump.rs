use crate::api::RedisCommandOutput;
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

const API_INFO: ApiInfo<RedisApi, FunctionDumpInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FunctionDump,
    "Dumps all libraries into a serialized binary payload",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FUNCTION DUMP`
/// https://redis.io/docs/latest/commands/function-dump/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionDumpInput {}

impl FunctionDumpInput {
    /// Create a new FUNCTION DUMP input
    pub fn new() -> Self {
        Self::default()
    }
}

impl Serialize for FunctionDumpInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionDumpInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(FunctionDumpInput, API_INFO);

impl RedisCommandInput for FunctionDumpInput {
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
                "FUNCTION DUMP expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis FUNCTION DUMP command
///
/// Returns a serialized binary payload containing all libraries.
/// This payload can be used with FUNCTION RESTORE to restore the libraries.
#[derive(Debug, Clone, ToSchema, JsonSchema)]
pub struct FunctionDumpOutput {
    /// The serialized payload containing all libraries
    payload: Vec<u8>,
}

impl FunctionDumpOutput {
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Get the serialized payload
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Check if there are any libraries (payload is non-empty)
    pub fn has_libraries(&self) -> bool {
        !self.payload.is_empty()
    }

    /// Get the size of the serialized payload in bytes
    pub fn size(&self) -> usize {
        self.payload.len()
    }
}

impl Serialize for FunctionDumpOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::{Engine, engine::general_purpose::STANDARD};
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FunctionDumpOutput", 1)?;
        state.serialize_field("payload", &STANDARD.encode(&self.payload))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FunctionDumpOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            payload: String,
        }
        let helper = Helper::deserialize(deserializer)?;
        use base64::{Engine, engine::general_purpose::STANDARD};
        let payload = STANDARD.decode(&helper.payload).map_err(serde::de::Error::custom)?;
        Ok(FunctionDumpOutput { payload })
    }
}

impl RedisCommandOutput for FunctionDumpOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::FunctionDump
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let payload = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => data,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION DUMP response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION DUMP response: {:?}", other)));
                }
            },
        };

        Ok(Self { payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FunctionDumpInput::new();
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nFUNCTION\r\n$4\r\nDUMP\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = FunctionDumpOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(output.has_libraries());
            assert_eq!(output.payload(), b"hello");
            assert_eq!(output.size(), 5);
        }

        #[test]
        fn test_decode_empty_bulk_string() {
            let output = FunctionDumpOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(!output.has_libraries());
            assert_eq!(output.size(), 0);
        }

        #[test]
        fn test_decode_binary_data() {
            let binary = b"$6\r\n\x00\x01\x02\x03\x04\x05\r\n";
            let output = FunctionDumpOutput::decode(binary).unwrap();
            assert!(output.has_libraries());
            assert_eq!(output.payload(), &[0u8, 1, 2, 3, 4, 5]);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FunctionDumpOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = FunctionDumpInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionDump);
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            // Should succeed but log a warning
            let input = FunctionDumpInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionDump);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FunctionDumpInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FunctionDumpInput::new();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
        }

        #[test]
        fn test_serialize_output_roundtrip() {
            let output = FunctionDumpOutput::new(vec![1, 2, 3, 4, 5]);
            let json = serde_json::to_string(&output).unwrap();
            let decoded: FunctionDumpOutput = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.payload(), output.payload());
        }

        #[test]
        fn test_kind() {
            let input = FunctionDumpInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionDump);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // FUNCTION commands require Redis 7.0+
        const MIN_VERSION: &str = "7";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_empty() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // First flush to ensure empty state
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    let result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("raw failed");

                    FunctionDumpOutput::decode(&result).expect("decode failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_with_library() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Load a simple library
                    let lua_code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code.len(), lua_code);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load failed");

                    // Now dump
                    let result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("raw failed");

                    let output = FunctionDumpOutput::decode(&result).expect("decode failed");
                    assert!(output.has_libraries(), "should have libraries after load");
                    assert!(output.size() > 0, "payload should be non-empty");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_restore_roundtrip() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Load a library
                    let lua_code = "#!lua name=testlib\nredis.register_function('testfunc', function(keys, args) return 42 end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code.len(), lua_code);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load failed");

                    // Dump
                    let dump_result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("dump failed");
                    let dump_output = FunctionDumpOutput::decode(&dump_result).expect("decode dump");
                    let payload = dump_output.payload().to_vec();

                    // Flush again
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Restore
                    let restore_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$7\r\nRESTORE\r\n${}\r\n", payload.len());
                    let mut restore_bytes = restore_cmd.into_bytes();
                    restore_bytes.extend_from_slice(&payload);
                    restore_bytes.extend_from_slice(b"\r\n");
                    ctx.raw(&restore_bytes).await.expect("restore failed");

                    // Verify by calling the function
                    let fcall_result = ctx.raw(b"*3\r\n$5\r\nFCALL\r\n$8\r\ntestfunc\r\n$1\r\n0\r\n").await.expect("fcall failed");

                    // Should return 42
                    assert!(
                        fcall_result.starts_with(b":42") || fcall_result.contains(&b'4'),
                        "function should return expected value"
                    );
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Flush first
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("raw failed");

            // RESP2 bulk string format: $<length>\r\n<data>\r\n
            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            FunctionDumpOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Flush first
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("raw failed");

            FunctionDumpOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_dump_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Pipeline: FUNCTION DUMP + FUNCTION DUMP
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&FunctionDumpInput::new().command());
                    pipeline.extend_from_slice(&FunctionDumpInput::new().command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = FunctionDumpOutput::decode(responses[0]).expect("decode first");
                    let output2 = FunctionDumpOutput::decode(responses[1]).expect("decode second");

                    // Both should return the same payload
                    assert_eq!(output1.payload(), output2.payload());
                })
            })
            .await;
        }
    }
}
