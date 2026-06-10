use crate::api::lib::sorted_set::common::Limit;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};
// Use shared Limit type

const API_INFO: ApiInfo<RedisApi, ZrevrangebylexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrevrangebylex,
    "Returns the members in a sorted set within a lexicographical range in reverse order",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZREVRANGEBYLEX`
/// https://redis.io/docs/latest/commands/zrevrangebylex/
///
/// Note: ZREVRANGEBYLEX is deprecated as of Redis 6.2.0, use ZRANGE with BYLEX REV instead.
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrevrangebylexInput {
    key: RedisKey,
    max: RedisJsonValue,
    min: RedisJsonValue,
    limit: Option<Limit>,
}

impl ZrevrangebylexInput {
    pub fn new(key: impl Into<RedisKey>, max: impl Into<RedisJsonValue>, min: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            limit: None,
        }
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit::new(offset, count));
        self
    }
}

impl Serialize for ZrevrangebylexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.limit.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZrevrangebylexInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("max", &self.max)?;
        state.serialize_field("min", &self.min)?;
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", &limit)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZrevrangebylexInput,
    API_INFO,
    {key, max, min, limit}
);

impl RedisCommandInput for ZrevrangebylexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.max).arg(&self.min);

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("ZREVRANGEBYLEX requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let max = args[1].clone();
        let min = args[2].clone();
        let mut limit = None;

        if args.len() >= 6
            && let RedisJsonValue::String(s) = &args[3]
            && s.to_uppercase() == "LIMIT"
        {
            limit = Some(Limit { offset: args[4].clone(), count: args[5].clone() });
        }

        Ok(Self { key, max, min, limit })
    }
}

/// Output for Redis ZREVRANGEBYLEX command
///
/// Returns members in reverse lexicographical order.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrevrangebylexOutput {
    members: Vec<String>,
}

impl ZrevrangebylexOutput {
    pub fn new(members: Vec<String>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let members = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut members = Vec::new();
                for frame in arr {
                    match frame {
                        Resp2Frame::BulkString(b) => {
                            members.push(String::from_utf8(b).map_err(EpError::parse)?);
                        }
                        _ => return Err(EpError::parse("expected string member")),
                    }
                }
                members
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    match frame {
                        Resp3Frame::BlobString { data, .. } => {
                            members.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                        _ => return Err(EpError::parse("expected string member")),
                    }
                }
                members
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected array response")),
        };

        Ok(Self { members })
    }
}

impl Serialize for ZrevrangebylexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrevrangebylexOutput", 1)?;
        state.serialize_field("members", &self.members)?;
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
            let input = ZrevrangebylexInput::new(RedisKey::String("myzset".into()), "[c", "[a");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREVRANGEBYLEX"));
            assert!(cmd_str.contains("myzset"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = ZrevrangebylexInput::new(RedisKey::String("myzset".into()), "+", "-").with_limit(0, 10);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_decode_output() {
            let output = ZrevrangebylexOutput::decode(b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.members(), &["c", "b", "a"]);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrevrangebylexOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZrevrangebylexOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("[c".into()),
                RedisJsonValue::String("[a".into()),
            ];
            let input = ZrevrangebylexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_with_limit() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("+".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("LIMIT".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(5),
            ];
            let input = ZrevrangebylexInput::decode(args).unwrap();
            assert!(input.limit.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("[c".into())];
            let err = ZrevrangebylexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrevrangebylexInput::new(RedisKey::String("myzset".into()), "+", "-");
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebylex_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzrevrangebylex_bsc\r\n")
                        .await
                        .expect("raw failed");

                    // All members with same score for lex ordering
                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$18\r\nzrevrangebylex_bsc\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // Get all in reverse lex order
                    let result = ctx
                        .raw(&ZrevrangebylexInput::new(
                            RedisKey::String("zrevrangebylex_bsc".into()),
                            "+",
                            "-",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 4);
                    assert_eq!(output.members(), &["d", "c", "b", "a"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebylex_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzrevrangebylex_rng\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$18\r\nzrevrangebylex_rng\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n$1\r\n0\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Get [d, c, b] in reverse (from d down to b inclusive)
                    let result = ctx
                        .raw(&ZrevrangebylexInput::new(
                            RedisKey::String("zrevrangebylex_rng".into()),
                            "[d",
                            "[b",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.members(), &["d", "c", "b"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebylex_with_limit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzrevrangebylex_lmt\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$18\r\nzrevrangebylex_lmt\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n$1\r\n0\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Skip 1, take 2
                    let result = ctx
                        .raw(&ZrevrangebylexInput::new(
                            RedisKey::String("zrevrangebylex_lmt".into()),
                            "+",
                            "-",
                        ).with_limit(1, 2).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.members(), &["d", "c"]); // Skip e, take d and c
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebylex_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrevrangebylex_empty\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangebylexInput::new(RedisKey::String("zrevrangebylex_empty".into()), "+", "-").command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebylexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebylex_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$20\r\nzrevrangebylex_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangebylexInput::new(RedisKey::String("zrevrangebylex_wrong".into()), "+", "-").command())
                        .await
                        .expect("raw failed");

                    let err = ZrevrangebylexOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
