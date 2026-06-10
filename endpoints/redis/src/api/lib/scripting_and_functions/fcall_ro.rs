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

const API_INFO: ApiInfo<RedisApi, FcallRoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FcallRo, "Invokes a read-only function", ReqType::Read, true);

/// Input for Redis `FCALL_RO` command.
///
/// This is a read-only variant of the FCALL command. It ensures that the function
/// only executes read-only commands and cannot modify data.
///
/// See official Redis documentation for `FCALL_RO`:
/// https://redis.io/docs/latest/commands/fcall_ro/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FcallRoInput {
    /// The name of the function to invoke (must be read-only)
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

impl Default for FcallRoInput {
    fn default() -> Self {
        Self {
            function: RedisJsonValue::String(String::new()),
            numkeys: RedisJsonValue::Integer(0),
            keys: None,
            args: None,
        }
    }
}

impl Serialize for FcallRoInput {
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

        let mut state = serializer.serialize_struct("FcallRoInput", fields)?;
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

impl_redis_operation!(FcallRoInput, API_INFO, { function, numkeys, keys, args });

impl RedisCommandInput for FcallRoInput {
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
            return Err(EpError::request(format!("FCALL_RO requires at least 2 arguments, given {}", args.len())));
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

/// Output for Redis `FCALL_RO` command.
///
/// The return value depends on the function. Functions can return any Redis data type.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FcallRoOutput {
    /// The result returned by the function
    result: RedisJsonValue,
}

impl FcallRoOutput {
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

    /// Decode the Redis protocol response into an FcallRoOutput
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
            other => Err(EpError::parse(format!("unexpected FCALL_RO response type: {:?}", other))),
        }
    }
}

impl Serialize for FcallRoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FcallRoOutput", 1)?;
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
            let input = FcallRoInput {
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
            let input = FcallRoInput {
                function: RedisJsonValue::String("myfunc".into()),
                numkeys: RedisJsonValue::Integer(1),
                keys: Some(vec![RedisKey::String("mykey".into())]),
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = FcallRoOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.as_int(), Some(42));
        }

        #[test]
        fn test_decode_string_response() {
            let output = FcallRoOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_nil_response() {
            let output = FcallRoOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FcallRoOutput::decode(b"-ERR No such function\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let result = FcallRoInput::decode(vec![RedisJsonValue::String("func".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_basic() {
            let result = FcallRoInput::decode(vec![RedisJsonValue::String("myfunc".into()), RedisJsonValue::Integer(0)]);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty_when_none() {
            let input = FcallRoInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = FcallRoInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::FcallRo);
        }

        #[test]
        fn test_serialize_output() {
            let output = FcallRoOutput::new(RedisJsonValue::String("test".into()));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("test"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FCALL_RO requires Redis 7.0+ and a function to be loaded first.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_ro_function_not_found() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FcallRoInput {
                                function: RedisJsonValue::String("nonexistent_function".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = FcallRoOutput::decode(&result).unwrap_err();
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
        async fn test_fcall_ro_with_loaded_function() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Load a read-only function
                    let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$118\r\n#!lua name=rolib\nredis.register_function{function_name='rofn', callback=function() return 42 end, flags={'no-writes'}}\r\n";
                    let _ = ctx.raw(load_cmd).await;

                    let result = ctx
                        .raw(
                            &FcallRoInput {
                                function: RedisJsonValue::String("rofn".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FcallRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_int(), Some(42));

                    // Cleanup
                    let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$5\r\nrolib\r\n").await;
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_ro_pipeline() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Load a function
                    let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$125\r\n#!lua name=ropipelib\nredis.register_function{function_name='ropipefn', callback=function() return 1 end, flags={'no-writes'}}\r\n";
                    let _ = ctx.raw(load_cmd).await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &FcallRoInput {
                            function: RedisJsonValue::String("ropipefn".into()),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                            .command(),
                    );
                    pipeline.extend_from_slice(
                        &FcallRoInput {
                            function: RedisJsonValue::String("ropipefn".into()),
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

                    let output1 = FcallRoOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(output1.as_int(), Some(1));

                    let output2 = FcallRoOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(output2.as_int(), Some(1));

                    // Cleanup
                    let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$9\r\nropipelib\r\n").await;
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_ro_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Load function with no-writes flag
            let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$124\r\n#!lua name=ro_r2lib\nredis.register_function{function_name='ro_r2fn', callback=function() return 99 end, flags={'no-writes'}}\r\n";
            let _ = ctx.raw(load_cmd).await;

            let result = ctx
                .raw(
                    &FcallRoInput {
                        function: RedisJsonValue::String("ro_r2fn".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FcallRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(99));

            // Cleanup
            let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$8\r\nro_r2lib\r\n").await;
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_fcall_ro_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Load function with no-writes flag
            let load_cmd = b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n$124\r\n#!lua name=ro_r3lib\nredis.register_function{function_name='ro_r3fn', callback=function() return 88 end, flags={'no-writes'}}\r\n";
            let _ = ctx.raw(load_cmd).await;

            let result = ctx
                .raw(
                    &FcallRoInput {
                        function: RedisJsonValue::String("ro_r3fn".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FcallRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(88));

            // Cleanup
            let _ = ctx.raw(b"*3\r\n$8\r\nFUNCTION\r\n$6\r\nDELETE\r\n$8\r\nro_r3lib\r\n").await;
            ctx.stop().await;
        }
    }
}
