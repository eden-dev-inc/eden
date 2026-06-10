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

const API_INFO: ApiInfo<RedisApi, EvalRoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::EvalRo, "Executes a read-only server-side Lua script", ReqType::Read, true);

/// Input for Redis `EVAL_RO` command.
///
/// This is a read-only variant of the EVAL command. It ensures that the script
/// only executes read-only commands and cannot modify data.
///
/// See official Redis documentation for `EVAL_RO`:
/// https://redis.io/docs/latest/commands/eval_ro/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct EvalRoInput {
    /// The Lua script to execute (must be read-only)
    script: RedisJsonValue,
    /// The number of keys that follow
    numkeys: RedisJsonValue,
    /// The keys accessed by the script (for cluster routing)
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Vec<RedisKey>>,
    /// Additional arguments passed to the script
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<RedisJsonValue>>,
}

impl Default for EvalRoInput {
    fn default() -> Self {
        Self {
            script: RedisJsonValue::String(String::new()),
            numkeys: RedisJsonValue::Integer(0),
            keys: None,
            args: None,
        }
    }
}

impl Serialize for EvalRoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, script, numkeys
        if self.keys.is_some() {
            fields += 1;
        }
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("EvalRoInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("script", &self.script)?;
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

impl_redis_operation!(EvalRoInput, API_INFO, { script, numkeys, keys, args });

impl RedisCommandInput for EvalRoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone().unwrap_or_default()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.script).arg(&self.numkeys);

        if let Some(keys) = &self.keys {
            for key in keys {
                command.arg(key);
            }
        }

        if let Some(args) = &self.args {
            for arg in args {
                command.arg(arg);
            }
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("EVAL_RO requires at least 2 arguments, given {}", args.len())));
        }

        let script = args[0].clone();
        let numkeys = args[1].clone();

        let numkeys_val = match &numkeys {
            RedisJsonValue::Integer(n) => n.to_usize().unwrap_or(0),
            RedisJsonValue::Float(f) => f.to_usize().unwrap_or(0),
            RedisJsonValue::String(s) => s.parse::<usize>().unwrap_or(0),
            _ => 0,
        };

        let mut keys = None;
        let mut script_args = None;

        if args.len() > 2 {
            let remaining_args = &args[2..];

            if numkeys_val > 0 && remaining_args.len() >= numkeys_val {
                let mut keys_ = vec![];

                for k in remaining_args[..numkeys_val].iter() {
                    keys_.push(k.try_into()?);
                }

                keys = Some(keys_);

                if remaining_args.len() > numkeys_val {
                    script_args = Some(remaining_args[numkeys_val..].to_vec());
                }
            } else if numkeys_val == 0 {
                script_args = Some(remaining_args.to_vec());
            }
        }

        Ok(Self { script, numkeys, keys, args: script_args })
    }
}

/// Output for Redis `EVAL_RO` command.
///
/// The return value depends on the script. Scripts can return any Redis data type.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct EvalRoOutput {
    /// The result returned by the script
    result: RedisJsonValue,
}

impl EvalRoOutput {
    pub fn new(result: RedisJsonValue) -> Self {
        Self { result }
    }

    /// Get the result from the script execution
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

    /// Decode the Redis protocol response into an EvalRoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = Self::frame_to_value(frame)?;
        Ok(Self { result })
    }

    fn frame_to_value(frame: DecoderRespFrame) -> Result<RedisJsonValue, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::resp2_to_value(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::resp3_to_value(resp3_frame),
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
            other => Err(EpError::parse(format!("unexpected EVAL_RO response type: {:?}", other))),
        }
    }
}

impl Serialize for EvalRoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EvalRoOutput", 1)?;
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
            let input = EvalRoInput {
                script: RedisJsonValue::String("return 1".into()),
                numkeys: RedisJsonValue::Integer(0),
                keys: None,
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"return 1"));
        }

        #[test]
        fn test_encode_command_with_keys() {
            let input = EvalRoInput {
                script: RedisJsonValue::String("return KEYS[1]".into()),
                numkeys: RedisJsonValue::Integer(1),
                keys: Some(vec![RedisKey::String("mykey".into())]),
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = EvalRoOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.as_int(), Some(42));
        }

        #[test]
        fn test_decode_string_response() {
            let output = EvalRoOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_nil_response() {
            let output = EvalRoOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = EvalRoOutput::decode(b"-ERR Script error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let result = EvalRoInput::decode(vec![RedisJsonValue::String("return 1".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_basic() {
            let result = EvalRoInput::decode(vec![RedisJsonValue::String("return 1".into()), RedisJsonValue::Integer(0)]);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty_when_none() {
            let input = EvalRoInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = EvalRoInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::EvalRo);
        }

        #[test]
        fn test_serialize_output() {
            let output = EvalRoOutput::new(RedisJsonValue::String("test".into()));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("test"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        // EVAL_RO requires Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_return_integer() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &EvalRoInput {
                                script: RedisJsonValue::String("return 42".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_int(), Some(42));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_read_key() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Set a key first using regular EVAL or SET
                    ctx.write(SetInput {
                        key: RedisKey::String("rokey".into()),
                        value: RedisJsonValue::String("rovalue".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &EvalRoInput {
                                script: RedisJsonValue::String("return redis.call('GET', KEYS[1])".into()),
                                numkeys: RedisJsonValue::Integer(1),
                                keys: Some(vec![RedisKey::String("rokey".into())]),
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("rovalue"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_with_argv() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &EvalRoInput {
                                script: RedisJsonValue::String("return ARGV[1]".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: Some(vec![RedisJsonValue::String("argval".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("argval"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_pipeline() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &EvalRoInput {
                            script: RedisJsonValue::String("return 1".into()),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &EvalRoInput {
                            script: RedisJsonValue::String("return 2".into()),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = EvalRoOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(output1.as_int(), Some(1));

                    let output2 = EvalRoOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(output2.as_int(), Some(2));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &EvalRoInput {
                        script: RedisJsonValue::String("return 123".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = EvalRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(123));
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_eval_ro_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &EvalRoInput {
                        script: RedisJsonValue::String("return 456".into()),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = EvalRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(456));
            ctx.stop().await;
        }
    }
}
