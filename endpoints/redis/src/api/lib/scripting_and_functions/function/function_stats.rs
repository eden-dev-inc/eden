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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, FunctionStatsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FunctionStats,
    "Returns information about a function during execution",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FUNCTION STATS`
/// https://redis.io/docs/latest/commands/function-stats/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionStatsInput {}

impl FunctionStatsInput {
    /// Create a new FUNCTION STATS input
    pub fn new() -> Self {
        Self::default()
    }
}

impl Serialize for FunctionStatsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionStatsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(FunctionStatsInput, API_INFO);

impl RedisCommandInput for FunctionStatsInput {
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
                "FUNCTION STATS expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Information about a currently running script
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RunningScript {
    /// The name of the function being executed
    pub name: String,
    /// The command arguments used to invoke the function
    pub command: Vec<String>,
    /// Duration in milliseconds the function has been running
    pub duration_ms: i64,
}

/// Statistics for a single engine
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct EngineStats {
    /// Number of libraries loaded for this engine
    pub libraries_count: i64,
    /// Number of functions loaded for this engine
    pub functions_count: i64,
}

/// Output for Redis FUNCTION STATS command
///
/// Returns information about the currently executing function and engine statistics.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct FunctionStatsOutput {
    /// Information about the currently running script, if any
    pub running_script: Option<RunningScript>,
    /// Statistics per engine (e.g., "LUA")
    pub engines: HashMap<String, EngineStats>,
}

impl FunctionStatsOutput {
    pub fn new(running_script: Option<RunningScript>, engines: HashMap<String, EngineStats>) -> Self {
        Self { running_script, engines }
    }

    /// Check if a function is currently running
    pub fn is_function_running(&self) -> bool {
        self.running_script.is_some()
    }

    /// Get the currently running script info
    pub fn running_script(&self) -> Option<&RunningScript> {
        self.running_script.as_ref()
    }

    /// Get engine statistics
    pub fn engines(&self) -> &HashMap<String, EngineStats> {
        &self.engines
    }

    /// Get statistics for a specific engine
    pub fn engine_stats(&self, engine: &str) -> Option<&EngineStats> {
        self.engines.get(engine)
    }

    /// Decode the Redis protocol response into a FunctionStatsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        Self::decode_frame(frame)
    }

    fn decode_frame(frame: DecoderRespFrame) -> Result<Self, EpError> {
        let items = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => items.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<_>>(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION STATS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Map { data, .. } => {
                    // RESP3 returns a map, convert to array format for uniform processing
                    let mut items = Vec::new();
                    for (k, v) in data {
                        items.push(DecoderRespFrame::Resp3(k));
                        items.push(DecoderRespFrame::Resp3(v));
                    }
                    items
                }
                Resp3Frame::Array { data, .. } => data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<_>>(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION STATS response: {:?}", other)));
                }
            },
        };

        let mut running_script = None;
        let mut engines = HashMap::new();

        // Parse key-value pairs
        let mut i = 0;
        while i + 1 < items.len() {
            let key = Self::extract_string(&items[i])?;
            let value = &items[i + 1];

            match key.to_lowercase().as_str() {
                "running_script" => {
                    running_script = Self::parse_running_script(value)?;
                }
                "engines" => {
                    engines = Self::parse_engines(value)?;
                }
                _ => {} // Ignore unknown fields
            }
            i += 2;
        }

        Ok(Self { running_script, engines })
    }

    fn extract_string(frame: &DecoderRespFrame) -> Result<String, EpError> {
        match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
                Resp2Frame::SimpleString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
                _ => Err(EpError::parse("expected string")),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse),
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse),
                _ => Err(EpError::parse("expected string")),
            },
        }
    }

    fn extract_integer(frame: &DecoderRespFrame) -> Result<i64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::Integer(i) => Ok(*i),
                _ => Err(EpError::parse("expected integer")),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::Number { data, .. } => Ok(*data),
                _ => Err(EpError::parse("expected integer")),
            },
        }
    }

    fn parse_running_script(frame: &DecoderRespFrame) -> Result<Option<RunningScript>, EpError> {
        // Check for null
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => return Ok(None),
            DecoderRespFrame::Resp3(Resp3Frame::Null) => return Ok(None),
            _ => {}
        }

        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => items.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut items = Vec::new();
                for (k, v) in data {
                    items.push(DecoderRespFrame::Resp3(k.clone()));
                    items.push(DecoderRespFrame::Resp3(v.clone()));
                }
                items
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array or map for running_script")),
        };

        let mut name = String::new();
        let mut command = Vec::new();
        let mut duration_ms = 0i64;

        let mut i = 0;
        while i + 1 < items.len() {
            let key = Self::extract_string(&items[i])?;
            match key.to_lowercase().as_str() {
                "name" => name = Self::extract_string(&items[i + 1])?,
                "command" => command = Self::parse_string_array(&items[i + 1])?,
                "duration_ms" => duration_ms = Self::extract_integer(&items[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok(Some(RunningScript { name, command, duration_ms }))
    }

    fn parse_string_array(frame: &DecoderRespFrame) -> Result<Vec<String>, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => items.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array")),
        };

        items.iter().map(Self::extract_string).collect()
    }

    fn parse_engines(frame: &DecoderRespFrame) -> Result<HashMap<String, EngineStats>, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => items.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut items = Vec::new();
                for (k, v) in data {
                    items.push(DecoderRespFrame::Resp3(k.clone()));
                    items.push(DecoderRespFrame::Resp3(v.clone()));
                }
                items
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array or map for engines")),
        };

        let mut engines = HashMap::new();
        let mut i = 0;
        while i + 1 < items.len() {
            let engine_name = Self::extract_string(&items[i])?;
            let stats = Self::parse_engine_stats(&items[i + 1])?;
            engines.insert(engine_name, stats);
            i += 2;
        }

        Ok(engines)
    }

    fn parse_engine_stats(frame: &DecoderRespFrame) -> Result<EngineStats, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => items.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut items = Vec::new();
                for (k, v) in data {
                    items.push(DecoderRespFrame::Resp3(k.clone()));
                    items.push(DecoderRespFrame::Resp3(v.clone()));
                }
                items
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array or map for engine stats")),
        };

        let mut libraries_count = 0i64;
        let mut functions_count = 0i64;

        let mut i = 0;
        while i + 1 < items.len() {
            let key = Self::extract_string(&items[i])?;
            match key.to_lowercase().as_str() {
                "libraries_count" => libraries_count = Self::extract_integer(&items[i + 1])?,
                "functions_count" => functions_count = Self::extract_integer(&items[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok(EngineStats { libraries_count, functions_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FunctionStatsInput::new();
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nSTATS\r\n");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FunctionStatsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = FunctionStatsInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionStats);
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            let input = FunctionStatsInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionStats);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FunctionStatsInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FunctionStatsInput::new();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
        }

        #[test]
        fn test_serialize_output() {
            let mut engines = HashMap::new();
            engines.insert("LUA".to_string(), EngineStats { libraries_count: 1, functions_count: 2 });
            let output = FunctionStatsOutput::new(None, engines);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"engines\""));
            assert!(json.contains("\"LUA\""));
        }

        #[test]
        fn test_is_function_running() {
            let output = FunctionStatsOutput::new(None, HashMap::new());
            assert!(!output.is_function_running());

            let running = RunningScript {
                name: "test".to_string(),
                command: vec!["FCALL".to_string()],
                duration_ms: 100,
            };
            let output_with_script = FunctionStatsOutput::new(Some(running), HashMap::new());
            assert!(output_with_script.is_function_running());
        }

        #[test]
        fn test_kind() {
            let input = FunctionStatsInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionStats);
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
        async fn test_function_stats_empty() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    let result = ctx.raw(&FunctionStatsInput::new().command()).await.expect("raw failed");

                    let output = FunctionStatsOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_function_running());
                    // Should have LUA engine with 0 libraries/functions
                    if let Some(lua) = output.engine_stats("LUA") {
                        assert_eq!(lua.libraries_count, 0);
                        assert_eq!(lua.functions_count, 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_stats_with_library() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n")
                        .await
                        .expect("flush failed");

                    // Load a library with two functions
                    let lua_code = "#!lua name=statslib\nredis.register_function('func1', function(keys, args) return 1 end)\nredis.register_function('func2', function(keys, args) return 2 end)";
                    let load_cmd = format!(
                        "*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n",
                        lua_code.len(),
                        lua_code
                    );
                    ctx.raw(load_cmd.as_bytes())
                        .await
                        .expect("load failed");

                    let result = ctx
                        .raw(&FunctionStatsInput::new().command())
                        .await
                        .expect("raw failed");

                    let output = FunctionStatsOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_function_running());

                    // Should have LUA engine with 1 library and 2 functions
                    let lua = output.engine_stats("LUA").expect("LUA engine should exist");
                    assert_eq!(lua.libraries_count, 1);
                    assert_eq!(lua.functions_count, 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_stats_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Flush first
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let result = ctx.raw(&FunctionStatsInput::new().command()).await.expect("raw failed");

            // RESP2 returns an array
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = FunctionStatsOutput::decode(&result).expect("decode failed");
            assert!(!output.is_function_running());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_stats_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Flush first
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let result = ctx.raw(&FunctionStatsInput::new().command()).await.expect("raw failed");

            // RESP3 may return a map
            let output = FunctionStatsOutput::decode(&result).expect("decode failed");
            assert!(!output.is_function_running());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_stats_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Pipeline: FUNCTION STATS + FUNCTION STATS
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&FunctionStatsInput::new().command());
                    pipeline.extend_from_slice(&FunctionStatsInput::new().command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = FunctionStatsOutput::decode(responses[0]).expect("decode first");
                    let output2 = FunctionStatsOutput::decode(responses[1]).expect("decode second");

                    // Both should show no running function
                    assert!(!output1.is_function_running());
                    assert!(!output2.is_function_running());
                })
            })
            .await;
        }
    }
}
