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

const API_INFO: ApiInfo<RedisApi, EvalshaRoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::EvalshaRo,
    "Executes a read-only server-side Lua script by SHA1 digest",
    ReqType::Read,
    true,
);

/// Input for Redis `EVALSHA_RO` command.
///
/// This is a read-only variant of the EVALSHA command. It ensures that the script
/// only executes read-only commands and cannot modify data.
///
/// See official Redis documentation for `EVALSHA_RO`:
/// https://redis.io/docs/latest/commands/evalsha_ro/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct EvalshaRoInput {
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

impl Default for EvalshaRoInput {
    fn default() -> Self {
        Self {
            sha: RedisJsonValue::String(String::new()),
            numkeys: RedisJsonValue::Integer(0),
            keys: None,
            args: None,
        }
    }
}

impl Serialize for EvalshaRoInput {
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

        let mut state = serializer.serialize_struct("EvalshaRoInput", fields)?;
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

impl_redis_operation!(EvalshaRoInput, API_INFO, { sha, numkeys, keys, args });

impl RedisCommandInput for EvalshaRoInput {
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
            return Err(EpError::request(format!("EVALSHA_RO requires at least 2 arguments, given {}", args.len())));
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
                    keys_.push(k.try_into()?)
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

/// Output for Redis `EVALSHA_RO` command.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct EvalshaRoOutput {
    result: RedisJsonValue,
}

impl EvalshaRoOutput {
    pub fn new(result: RedisJsonValue) -> Self {
        Self { result }
    }

    pub fn result(&self) -> &RedisJsonValue {
        &self.result
    }

    pub fn as_str(&self) -> Option<&str> {
        match &self.result {
            RedisJsonValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match &self.result {
            RedisJsonValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn is_nil(&self) -> bool {
        matches!(&self.result, RedisJsonValue::Null)
    }

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
            Resp2Frame::BulkString(b) => Ok(RedisJsonValue::String(String::from_utf8(b).map_err(EpError::parse)?)),
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
            other => Err(EpError::parse(format!("unexpected EVALSHA_RO response: {:?}", other))),
        }
    }
}

impl Serialize for EvalshaRoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EvalshaRoOutput", 1)?;
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
            let input = EvalshaRoInput {
                sha: RedisJsonValue::String("sha1hash".into()),
                numkeys: RedisJsonValue::Integer(0),
                keys: None,
                args: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"sha1hash"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = EvalshaRoOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.as_int(), Some(42));
        }

        #[test]
        fn test_decode_string_response() {
            let output = EvalshaRoOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("hello"));
        }

        #[test]
        fn test_decode_nil_response() {
            let output = EvalshaRoOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_noscript_error() {
            let err = EvalshaRoOutput::decode(b"-NOSCRIPT No matching script\r\n").unwrap_err();
            assert!(err.to_string().contains("NOSCRIPT"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let result = EvalshaRoInput::decode(vec![RedisJsonValue::String("sha".into())]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_basic() {
            let result = EvalshaRoInput::decode(vec![RedisJsonValue::String("sha1hash".into()), RedisJsonValue::Integer(0)]);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty_when_none() {
            let input = EvalshaRoInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = EvalshaRoInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::EvalshaRo);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::lib::scripting_and_functions::script::script_load::{ScriptLoadInput, ScriptLoadOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_ro_after_load() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let load_result = ctx
                        .raw(&ScriptLoadInput { script: RedisJsonValue::String("return 42".into()) }.command())
                        .await
                        .expect("raw failed");

                    let script_load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha =
                        script_load_output.sha().ok_or_else(|| EpError::parse("SCRIPT LOAD response missing SHA1 digest")).expect("sha");

                    let result = ctx
                        .raw(
                            &EvalshaRoInput {
                                sha: RedisJsonValue::String(sha.into()),
                                numkeys: RedisJsonValue::Integer(0),
                                keys: None,
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalshaRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_int(), Some(42));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_ro_read_key() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("rosha_key".into()),
                        value: RedisJsonValue::String("rosha_value".into()),
                        ..Default::default()
                    })
                    .await;

                    let load_result = ctx
                        .raw(
                            &ScriptLoadInput {
                                script: RedisJsonValue::String("return redis.call('GET', KEYS[1])".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let script_load_output = ScriptLoadOutput::decode(&load_result).expect("decode failed");
                    let sha =
                        script_load_output.sha().ok_or_else(|| EpError::parse("SCRIPT LOAD response missing SHA1 digest")).expect("sha");

                    let result = ctx
                        .raw(
                            &EvalshaRoInput {
                                sha: RedisJsonValue::String(sha.into()),
                                numkeys: RedisJsonValue::Integer(1),
                                keys: Some(vec![RedisKey::String("rosha_key".into())]),
                                args: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = EvalshaRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("rosha_value"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_evalsha_ro_pipeline() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let load1 =
                        ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 1".into()) }.command()).await.expect("raw");
                    let sha1 = ScriptLoadOutput::decode(&load1).expect("decode").sha().expect("sha").to_string();

                    let load2 =
                        ctx.raw(&ScriptLoadInput { script: RedisJsonValue::String("return 2".into()) }.command()).await.expect("raw");
                    let sha2 = ScriptLoadOutput::decode(&load2).expect("decode").sha().expect("sha").to_string();

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &EvalshaRoInput {
                            sha: RedisJsonValue::String(sha1),
                            numkeys: RedisJsonValue::Integer(0),
                            keys: None,
                            args: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &EvalshaRoInput {
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
                    assert_eq!(EvalshaRoOutput::decode(responses[0]).expect("decode").as_int(), Some(1));
                    assert_eq!(EvalshaRoOutput::decode(responses[1]).expect("decode").as_int(), Some(2));
                })
            })
            .await;
        }
    }
}
