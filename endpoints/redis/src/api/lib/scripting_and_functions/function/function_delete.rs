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

const API_INFO: ApiInfo<RedisApi, FunctionDeleteInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FunctionDelete, "Deletes a library and its functions", ReqType::Write, true);

/// See official Redis documentation for `FUNCTION DELETE`
/// https://redis.io/docs/latest/commands/function-delete/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionDeleteInput {
    /// The name of the library to delete
    library_name: RedisJsonValue,
}

impl FunctionDeleteInput {
    /// Create a new FUNCTION DELETE input
    pub fn new(library_name: impl Into<String>) -> Self {
        Self { library_name: RedisJsonValue::String(library_name.into()) }
    }

    /// Get the library name
    pub fn library_name(&self) -> &RedisJsonValue {
        &self.library_name
    }
}

impl Serialize for FunctionDeleteInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionDeleteInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("library_name", &self.library_name)?;
        state.end()
    }
}

impl_redis_operation!(FunctionDeleteInput, API_INFO, { library_name });

impl RedisCommandInput for FunctionDeleteInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.library_name);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FUNCTION DELETE requires 1 argument, given {}", args.len())));
        }

        Ok(Self { library_name: args[0].clone() })
    }
}

/// Output for Redis FUNCTION DELETE command
///
/// Returns OK when the library was successfully deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FunctionDeleteOutput {
    /// Whether the delete operation succeeded
    success: bool,
}

impl FunctionDeleteOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the delete was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FunctionDeleteOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION DELETE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION DELETE response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for FunctionDeleteOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FunctionDeleteOutput", 1)?;
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
            let input = FunctionDeleteInput::new("mylib");
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$5\r\nmylib\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = FunctionDeleteOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_not_found() {
            let err = FunctionDeleteOutput::decode(b"-ERR Library not found\r\n").unwrap_err();
            assert!(err.to_string().contains("Library not found"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testlib".into())];
            let input = FunctionDeleteInput::decode(args).unwrap();
            assert_eq!(input.library_name, RedisJsonValue::String("testlib".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FunctionDeleteInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("lib1".into()), RedisJsonValue::String("lib2".into())];
            let err = FunctionDeleteInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FunctionDeleteInput::new("mylib");
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FunctionDeleteInput::new("mylib");
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
            assert!(json.contains("\"library_name\""));
            assert!(json.contains("mylib"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FunctionDeleteOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_kind() {
            let input = FunctionDeleteInput::new("mylib");
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionDelete);
        }

        #[test]
        fn test_library_name_getter() {
            let input = FunctionDeleteInput::new("testlib");
            assert_eq!(input.library_name(), &RedisJsonValue::String("testlib".into()));
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
        async fn test_function_delete_nonexistent() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Try to delete a library that doesn't exist
                    let result = ctx.raw(&FunctionDeleteInput::new("nonexistent_lib").command()).await.expect("raw failed");

                    let err = FunctionDeleteOutput::decode(&result);
                    assert!(err.is_err(), "should error for nonexistent library");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_delete_existing() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Load a library
                    let lua_code = "#!lua name=deletetest\nredis.register_function('delfunc', function(keys, args) return 'hi' end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code.len(), lua_code);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load failed");

                    // Delete the library
                    let result = ctx.raw(&FunctionDeleteInput::new("deletetest").command()).await.expect("raw failed");

                    let output = FunctionDeleteOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "FUNCTION DELETE should succeed");

                    // Verify it's gone - try to delete again should fail
                    let result2 = ctx.raw(&FunctionDeleteInput::new("deletetest").command()).await.expect("raw failed");

                    let err = FunctionDeleteOutput::decode(&result2);
                    assert!(err.is_err(), "second delete should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_delete_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Flush and load
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let lua_code = "#!lua name=resp2lib\nredis.register_function('f', function(keys, args) return 1 end)";
            let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code.len(), lua_code);
            ctx.raw(load_cmd.as_bytes()).await.expect("load failed");

            let result = ctx.raw(&FunctionDeleteInput::new("resp2lib").command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = FunctionDeleteOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_delete_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Flush and load
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

            let lua_code = "#!lua name=resp3lib\nredis.register_function('f', function(keys, args) return 1 end)";
            let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code.len(), lua_code);
            ctx.raw(load_cmd.as_bytes()).await.expect("load failed");

            let result = ctx.raw(&FunctionDeleteInput::new("resp3lib").command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = FunctionDeleteOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_delete_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush first
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush failed");

                    // Load two libraries
                    let lua_code1 = "#!lua name=pipelib1\nredis.register_function('f1', function(keys, args) return 1 end)";
                    let load_cmd1 = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code1.len(), lua_code1);
                    ctx.raw(load_cmd1.as_bytes()).await.expect("load1 failed");

                    let lua_code2 = "#!lua name=pipelib2\nredis.register_function('f2', function(keys, args) return 2 end)";
                    let load_cmd2 = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua_code2.len(), lua_code2);
                    ctx.raw(load_cmd2.as_bytes()).await.expect("load2 failed");

                    // Pipeline: delete both libraries
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&FunctionDeleteInput::new("pipelib1").command());
                    pipeline.extend_from_slice(&FunctionDeleteInput::new("pipelib2").command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = FunctionDeleteOutput::decode(responses[0]).expect("decode first");
                    assert!(output1.success());

                    let output2 = FunctionDeleteOutput::decode(responses[1]).expect("decode second");
                    assert!(output2.success());
                })
            })
            .await;
        }
    }
}
