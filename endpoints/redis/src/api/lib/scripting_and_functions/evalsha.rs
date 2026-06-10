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

const API_INFO: ApiInfo<RedisApi, EvalshaInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Evalsha,
    "Executes a server-side Lua script by SHA1 digest",
    ReqType::Write,
    true,
);

/// Input for Redis `EVALSHA` command.
///
/// Evaluates a script cached on the server side by its SHA1 digest. Scripts are cached
/// using the SCRIPT LOAD command.
///
/// See official Redis documentation for `EVALSHA`:
/// https://redis.io/docs/latest/commands/evalsha/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct EvalshaInput {
    /// The SHA1 digest of the script to execute
    sha: RedisJsonValue,
    /// The number of keys that follow
    numkeys: RedisJsonValue,
    /// The keys accessed by the script (for cluster routing)
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Vec<RedisKey>>,
    /// Additional arguments passed to the script
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<RedisJsonValue>>,
}

impl Default for EvalshaInput {
    fn default() -> Self {
        Self {
            sha: RedisJsonValue::String(String::new()),
            numkeys: RedisJsonValue::Integer(0),
            keys: None,
            args: None,
        }
    }
}

impl Serialize for EvalshaInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, sha, numkeys
        if self.keys.is_some() {
            fields += 1;
        }
        if self.args.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("EvalshaInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("sha", &self.sha)?;
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

impl_redis_operation!(EvalshaInput, API_INFO, { sha, numkeys, keys, args });

impl RedisCommandInput for EvalshaInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone().unwrap_or_default()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.sha).arg(&self.numkeys).arg(&self.keys).arg(&self.args);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("EVALSHA requires at least 2 arguments, given {}", args.len())));
        }

        let sha = args[0].clone();
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

        Ok(Self { sha, numkeys, keys, args: script_args })
    }
}

/// Output for Redis `EVALSHA` command.
///
/// The return value depends on the script. Scripts can return any Redis data type.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct EvalshaOutput {
    /// The result returned by the script
    result: RedisJsonValue,
}

impl EvalshaOutput {
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

    /// Decode the Redis protocol response into an EvalshaOutput
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
            other => Err(EpError::parse(format!("unexpected EVALSHA response type: {:?}", other))),
        }
    }
}

impl Serialize for EvalshaOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EvalshaOutput", 1)?;
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
            let input = EvalshaInput {
                sha: RedisJsonValue::String("a42059b356c875f0717db19a51f6aaa9161e77a2".into()),
                numkeys: RedisJsonValue::Integer(0),
                keys: None,
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(40).any(|w| w == b"a42059b356c875f0717db19a51f6aaa9161e77a2"));
        }

        #[test]
        fn test_encode_command_with_keys() {
            let input = EvalshaInput {
                sha: RedisJsonValue::String("sha1hash".into()),
                numkeys: RedisJsonValue::Integer(1),
                keys: Some(vec![RedisKey::String("mykey".into())]),
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"mykey"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = EvalshaOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.as_int(), Some(42));
        }

        #[test]
        fn test_decode_string_response() {
            let output = EvalshaOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_nil_response() {
            let output = EvalshaOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_noscript_error() {
            let err = EvalshaOutput::decode(b"-NOSCRIPT No matching script\r\n").unwrap_err();
            assert!(err.to_string().contains("NOSCRIPT"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let result = EvalshaInput::decode(vec![RedisJsonValue::String("sha".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_basic() {
            let result = EvalshaInput::decode(vec![RedisJsonValue::String("sha1hash".into()), RedisJsonValue::Integer(0)]);
            assert!(result.is_ok());
        }

        #[test]
        fn test_decode_input_with_keys() {
            let result = EvalshaInput::decode(vec![
                RedisJsonValue::String("sha1hash".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("key1".into()),
            ]);
            assert!(result.is_ok());
            let input = result.unwrap();
            assert!(input.keys.is_some());
        }

        #[test]
        fn test_keys_returns_empty_when_none() {
            let input = EvalshaInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = EvalshaInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Evalsha);
        }

        #[test]
        fn test_serialize_output() {
            let output = EvalshaOutput::new(RedisJsonValue::Integer(99));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("99"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::scripting_and_functions::script::script_load::{ScriptLoadInput, ScriptLoadOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_after_load() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First load the script
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 42".into()) }.command())
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha");

                    // Now execute using EVALSHA
                    let result = ctx
                        .raw(
                            &EvalshaInput {
                                sha: RedisJsonValue::String(sha.into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalshaOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_int(), Some(42));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_with_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    use crate::api::SetInput;

                    // Set a key
                    ctx.write(SetInput {
                        key: RedisKey::String("shakey".into()),
                        value: RedisJsonValue::String("shavalue".into()),
                        ..Default::default()
                    })
                    .await;

                    // Load script that reads KEYS[1]
                    let load_result = ctx
                        .raw(
                            &ScriptLoadInput {
                                script: RedisJsonValue::String("return redis.call('GET', KEYS[1])".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha = load_output.sha().expect("should have sha");

                    // Execute using EVALSHA with keys
                    let result = ctx
                        .raw(
                            &EvalshaInput {
                                sha: RedisJsonValue::String(sha.into()),
                                numkeys: RedisJsonValue::Integer(1),
                                keys: Some(vec![RedisKey::String("shakey".into())]),
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalshaOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("shavalue"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_nonexistent_script() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &EvalshaInput {
                                sha: RedisJsonValue::String("0000000000000000000000000000000000000000".into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = EvalshaOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("NOSCRIPT"), "should return NOSCRIPT error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Load scripts
                    let load1 = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command())
                        .await
                        .expect("raw failed");
                    let sha1 = ScriptLoadOutput::decode(&load1).expect("decode failed").sha().expect("sha").to_string();

                    let load2 = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 2".into()) }.command())
                        .await
                        .expect("raw failed");
                    let sha2 = ScriptLoadOutput::decode(&load2).expect("decode failed").sha().expect("sha").to_string();

                    // Pipeline EVALSHA calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &EvalshaInput {
                            sha: RedisJsonValue::String(sha1),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &EvalshaInput {
                            sha: RedisJsonValue::String(sha2),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = EvalshaOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(output1.as_int(), Some(1));

                    let output2 = EvalshaOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(output2.as_int(), Some(2));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Load script
            let load_result =
                ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 123".into()) }.command()).await.expect("raw failed");

            let sha = ScriptLoadOutput::decode(&load_result).expect("decode failed").sha().expect("sha").to_string();

            let result = ctx
                .raw(
                    &EvalshaInput {
                        sha: RedisJsonValue::String(sha),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = EvalshaOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(123));
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            // Load script
            let load_result =
                ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 456".into()) }.command()).await.expect("raw failed");

            let sha = ScriptLoadOutput::decode(&load_result).expect("decode failed").sha().expect("sha").to_string();

            let result = ctx
                .raw(
                    &EvalshaInput {
                        sha: RedisJsonValue::String(sha),
                        numkeys: RedisJsonValue::Integer(0),
                        keys: None,
                        args: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = EvalshaOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_int(), Some(456));
            ctx.stop().await;
        }
    }
}
