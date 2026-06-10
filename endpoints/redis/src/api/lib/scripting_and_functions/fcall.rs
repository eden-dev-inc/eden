use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use bigdecimal::ToPrimitive;
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FcallInput> = ApiInfo::new(EpKind::Redis, RedisApi::Fcall, "Invokes a function", ReqType::Write, true);

/// Input for Redis `FCALL` command.
///
/// Invokes a function previously loaded with FUNCTION LOAD. Functions are
/// stored in libraries and can be invoked by name.
///
/// See official Redis documentation for `FCALL`:
/// https://redis.io/docs/latest/commands/fcall/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FcallInput {
    /// The name of the function to invoke
    function: RedisJsonValue,
    /// The number of keys that follow
    numkeys: RedisJsonValue,
    /// The keys accessed by the function (for cluster routing)
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Vec<RedisKey>>,
    /// Additional arguments passed to the function
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<RedisJsonValue>>,
}

impl Default for FcallInput {
    fn default() -> Self {
        Self {
            function: RedisJsonValue::String(String::new()),
            numkeys: RedisJsonValue::Integer(0),
            keys: None,
            args: None,
        }
    }
}

impl Serialize for FcallInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.keys.is_some() {
            fields += 1;
        }
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FcallInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("function", &self.function)?;
        state.serialize_field("numkeys", &self.numkeys)?;
        if let Some(keys) = &self.keys {
            state.serialize_field("keys", keys)?;
        }
        if let Some(args) = &self.args {
            state.serialize_field("args", args)?;
        }
        state.end()
    }
}

impl_redis_operation!(FcallInput, API_INFO, { function, numkeys, keys, args });

impl RedisCommandInput for FcallInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone().unwrap_or_default()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.function).arg(&self.numkeys).arg(&self.keys).arg(&self.args);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FCALL requires at least 2 arguments, given {}", args.len())));
        }

        let function = args[0].clone();
        let numkeys = args[1].clone();

        let numkeys_val = match &numkeys {
            RedisJsonValue::Integer(n) => n.to_usize().unwrap_or(0),
            RedisJsonValue::Float(f) => f.to_usize().unwrap_or(0),
            RedisJsonValue::String(s) => s.parse::<usize>().unwrap_or(0),
            _ => 0,
        };

        let mut keys = None;
        let mut function_args = None;

        if args.len() > 2 {
            let remaining_args = &args[2..];

            if numkeys_val > 0 && remaining_args.len() >= numkeys_val {
                let mut keys_ = vec![];

                for k in remaining_args[..numkeys_val].iter() {
                    keys_.push(k.try_into()?);
                }
                keys = Some(keys_);

                if remaining_args.len() > numkeys_val {
                    function_args = Some(remaining_args[numkeys_val..].to_vec());
                }
            } else if numkeys_val == 0 {
                function_args = Some(remaining_args.to_vec());
            }
        }

        Ok(Self { function, numkeys, keys, args: function_args })
    }
}

/// Output for Redis `FCALL` command.
///
/// The return value depends on the function. Functions can return any Redis data type.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FcallOutput {
    /// The result returned by the function
    result: RedisJsonValue,
}

impl FcallOutput {
    pub fn new(result: RedisJsonValue) -> Self {
        Self { result }
    }

    /// Get the result from the function execution
    pub fn result(&self) -> &RedisJsonValue {
        &self.result
    }

    /// Try to get the result as a string
    pub fn as_str(&self) -> Option<&str> {
        match &self.result {
            RedisJsonValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get the result as an integer
    pub fn as_int(&self) -> Option<i64> {
        match &self.result {
            RedisJsonValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Check if the result is nil/null
    pub fn is_nil(&self) -> bool {
        matches!(&self.result, RedisJsonValue::Null)
    }

    /// Decode the Redis protocol response into an FcallOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = Self::frame_to_value(frame)?;
        Ok(Self { result })
    }

    fn frame_to_value(frame: DecoderRespFrame) -> Result<RedisJsonValue, EpError> {
        match frame {
            DecoderRespFrame::Resp2(f) => Self::resp2_to_value(f),
            DecoderRespFrame::Resp3(f) => Self::resp3_to_value(f),
        }
    }

    fn resp2_to_value(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) => Ok(RedisJsonValue::String(String::from_utf8(s).map_err(EpError::parse)?)),
            Resp2Frame::BulkString(bytes) => Ok(RedisJsonValue::String(String::from_utf8(bytes).map_err(EpError::parse)?)),
            Resp2Frame::Integer(i) => Ok(RedisJsonValue::Integer(i)),
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            Resp2Frame::Array(arr) => {
                let values: Result<Vec<_>, _> = arr.into_iter().map(Self::resp2_to_value).collect();
                Ok(RedisJsonValue::Array(values?))
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
        }
    }

    fn resp3_to_value(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::BlobString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
            Resp3Frame::Double { data, .. } => Ok(RedisJsonValue::Float(data)),
            Resp3Frame::Boolean { data, .. } => Ok(RedisJsonValue::Bool(data)),
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            Resp3Frame::Array { data, .. } => {
                let values: Result<Vec<_>, _> = data.into_iter().map(Self::resp3_to_value).collect();
                Ok(RedisJsonValue::Array(values?))
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FCALL response type: {:?}", other))),
        }
    }
}

impl Serialize for FcallOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FcallOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_simple() {
            let input = FcallInput {
                function: RedisJsonValue::String("myfunc".into()),
                numkeys: RedisJsonValue::Integer(0),
                keys: None,
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"myfunc"));
        }

        #[test]
        fn test_encode_command_with_keys() {
            let input = FcallInput {
                function: RedisJsonValue::String("myfunc".into()),
                numkeys: RedisJsonValue::Integer(1),
                keys: Some(vec![RedisKey::String("mykey".into())]),
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_encode_command_with_args() {
            let input = FcallInput {
                function: RedisJsonValue::String("myfunc".into()),
                numkeys: RedisJsonValue::Integer(0),
                keys: None,
                args: Some(vec![RedisJsonValue::String("arg1".into())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"arg1"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = FcallOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.as_int(), Some(42));
        }

        #[test]
        fn test_decode_string_response() {
            let output = FcallOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_nil_response() {
            let output = FcallOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FcallOutput::decode(b"-ERR No such function\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let result = FcallInput::decode(vec![RedisJsonValue::String("func".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_basic() {
            let result = FcallInput::decode(vec![RedisJsonValue::String("myfunc".into()), RedisJsonValue::Integer(0)]);
            assert!(result.is_ok());
        }

        #[test]
        fn test_decode_input_with_keys() {
            let result = FcallInput::decode(vec![
                RedisJsonValue::String("myfunc".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("key1".into()),
            ]);
            assert!(result.is_ok());
            let input = result.unwrap();
            assert!(input.keys.is_some());
        }

        #[test]
        fn test_keys_returns_empty_when_none() {
            let input = FcallInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = FcallInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Fcall);
        }

        #[test]
        fn test_serialize_output() {
            let output = FcallOutput::new(RedisJsonValue::Integer(99));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("99"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FCALL requires Redis 7.0+ and a function to be loaded first.
        // These tests verify the command structure and response parsing.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_function_not_found() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FcallInput {
                                function: RedisJsonValue::String("nonexistent_function".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should return an error since function doesn't exist
                    let err = FcallOutput::decode(&result).unwrap_err();
                    assert!(
                        err.to_string().contains("ERR") || err.to_string().contains("function"),
                        "should error for nonexistent function"
                    );
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_with_loaded_function() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Load a simple function library
                    let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$74\r\n#!lua name=mylib\nredis.register_function('myfn', function() return 42 end)\r\n";

                    // Try to load; ignore errors if already loaded
                    let _ = ctx.raw(load_cmd).await;

                    // Call the function
                    let result = ctx
                        .raw(
                            &FcallInput {
                                function: RedisJsonValue::String("myfn".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FcallOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_int(), Some(42));

                    // Cleanup: delete the function
                    let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$5\r\nmylib\r\n").await;
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_pipeline() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Load a function
                    let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$77\r\n#!lua name=pipelib\nredis.register_function('pipefn', function() return 1 end)\r\n";
                    let _ = ctx.raw(load_cmd).await;

                    // Pipeline multiple FCALL calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &FcallInput {
                            function: RedisJsonValue::String("pipefn".into()),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                            .command(),
                    );
                    pipeline.extend_from_slice(
                        &FcallInput {
                            function: RedisJsonValue::String("pipefn".into()),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                            .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result)
                        .expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = FcallOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(output1.as_int(), Some(1));

                    let output2 = FcallOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(output2.as_int(), Some(1));

                    // Cleanup
                    let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$7\r\npipelib\r\n").await;
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Load function
            let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$74\r\n#!lua name=r2lib\nredis.register_function('r2fn', function() return 99 end)\r\n";
            let _ = ctx.raw(load_cmd).await;

            let result = ctx
                .raw(
                    &FcallInput {
                        function: RedisJsonValue::String("r2fn".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FcallOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(99));

            // Cleanup
            let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$5\r\nr2lib\r\n").await;
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Load function
            let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$74\r\n#!lua name=r3lib\nredis.register_function('r3fn', function() return 88 end)\r\n";
            let _ = ctx.raw(load_cmd).await;

            let result = ctx
                .raw(
                    &FcallInput {
                        function: RedisJsonValue::String("r3fn".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FcallOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(88));

            // Cleanup
            let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$5\r\nr3lib\r\n").await;
            ctx.stop().await;
        }
    }
}
