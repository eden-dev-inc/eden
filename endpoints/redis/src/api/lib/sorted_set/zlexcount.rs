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

const API_INFO: ApiInfo<RedisApi, ZlexcountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zlexcount,
    "Returns the number of members in a sorted set within a lexicographical range",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZLEXCOUNT`
/// https://redis.io/docs/latest/commands/zlexcount/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZlexcountInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
}

impl ZlexcountInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), min: min.into(), max: max.into() }
    }
}

impl Serialize for ZlexcountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZlexcountInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.end()
    }
}

impl_redis_operation!(
    ZlexcountInput,
    API_INFO,
    {key, min, max}
);

impl RedisCommandInput for ZlexcountInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.min).arg(&self.max);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("ZLEXCOUNT requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            min: args[1].clone(),
            max: args[2].clone(),
        })
    }
}

/// Output for Redis ZLEXCOUNT command
///
/// Returns the number of elements in the specified lexicographical range.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZlexcountOutput {
    count: i64,
}

impl ZlexcountOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the count of elements in the lexicographical range
    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected integer response")),
        };

        Ok(Self { count })
    }
}

impl Serialize for ZlexcountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZlexcountOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = ZlexcountInput::new(RedisKey::String("myzset".into()), "[a", "[z");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZLEXCOUNT"));
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("[a"));
            assert!(cmd_str.contains("[z"));
        }

        #[test]
        fn test_encode_command_unbounded() {
            let input = ZlexcountInput::new(RedisKey::String("myzset".into()), "-", "+");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("-"));
            assert!(cmd_str.contains("+"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZlexcountOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ZlexcountOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_error() {
            let err = ZlexcountOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("[a".into()),
                RedisJsonValue::String("[z".into()),
            ];
            let input = ZlexcountInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("[a".into())];
            let err = ZlexcountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZlexcountInput::new(RedisKey::String("myzset".into()), "[a", "[z");
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
        async fn test_zlexcount_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzlexcount_test\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zlexcount_test 0 a 0 b 0 c 0 d 0 e 0 f
                    ctx.raw(b"*14\r\n$4\r\nZADD\r\n$14\r\nzlexcount_test\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n$1\r\n0\r\n$1\r\ne\r\n$1\r\n0\r\n$1\r\nf\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZlexcountInput::new(
                                RedisKey::String("zlexcount_test".into()),
                                "[b",
                                "[e",
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZlexcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 4); // b, c, d, e
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_unbounded() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzlexcount_unbound\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$17\r\nzlexcount_unbound\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZlexcountInput::new(
                                RedisKey::String("zlexcount_unbound".into()),
                                "-",
                                "+",
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZlexcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3); // all elements
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_exclusive() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzlexcount_excl\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$14\r\nzlexcount_excl\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // Exclusive range (a, d)
                    let result = ctx
                        .raw(
                            &ZlexcountInput::new(
                                RedisKey::String("zlexcount_excl".into()),
                                "(a",
                                "(d",
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZlexcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2); // b, c (excludes a and d)
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_empty_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzlexcount_empty\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$15\r\nzlexcount_empty\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    // Range with no elements
                    let result = ctx
                        .raw(&ZlexcountInput::new(RedisKey::String("zlexcount_empty".into()), "[x", "[z").command())
                        .await
                        .expect("raw failed");

                    let output = ZlexcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzlexcount_nokey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZlexcountInput::new(RedisKey::String("zlexcount_nokey".into()), "-", "+").command())
                        .await
                        .expect("raw failed");

                    let output = ZlexcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\nzlexcount_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZlexcountInput::new(RedisKey::String("zlexcount_wrong".into()), "-", "+").command())
                        .await
                        .expect("raw failed");

                    let err = ZlexcountOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzlexcount_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$12\r\nzlexcount_r2\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result =
                ctx.raw(&ZlexcountInput::new(RedisKey::String("zlexcount_r2".into()), "-", "+").command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ZlexcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 2);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zlexcount_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzlexcount_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$12\r\nzlexcount_r3\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result =
                ctx.raw(&ZlexcountInput::new(RedisKey::String("zlexcount_r3".into()), "-", "+").command()).await.expect("raw failed");

            let output = ZlexcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 2);

            ctx.stop().await;
        }
    }
}
