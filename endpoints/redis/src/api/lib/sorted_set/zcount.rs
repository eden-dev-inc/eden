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

const API_INFO: ApiInfo<RedisApi, ZcountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zcount,
    "Returns the count of members in a sorted set that have scores within a range",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZCOUNT`
/// https://redis.io/docs/latest/commands/zcount/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZcountInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
}

impl ZcountInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), min: min.into(), max: max.into() }
    }
}

impl Serialize for ZcountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZcountInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.end()
    }
}

impl_redis_operation!(ZcountInput, API_INFO, { key, min, max });

impl RedisCommandInput for ZcountInput {
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
            return Err(EpError::request(format!("ZCOUNT requires exactly 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            min: args[1].clone(),
            max: args[2].clone(),
        })
    }
}

/// Output for Redis ZCOUNT command
///
/// Returns the number of members in the sorted set with scores
/// within the specified range.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZcountOutput {
    count: i64,
}

impl ZcountOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of members in the score range
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if no members are in the range
    pub fn is_empty(&self) -> bool {
        self.count == 0
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
            _ => return Err(EpError::parse("ZCOUNT must return integer")),
        };

        Ok(Self { count })
    }
}

impl Serialize for ZcountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZcountOutput", 1)?;
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
            let input = ZcountInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZCOUNT"));
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("-inf"));
            assert!(cmd_str.contains("+inf"));
        }

        #[test]
        fn test_encode_command_numeric_range() {
            let input = ZcountInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(1), RedisJsonValue::Integer(5));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZCOUNT"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ZcountOutput::decode(b":10\r\n").unwrap();
            assert_eq!(output.count(), 10);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZcountOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZcountOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            ];
            let input = ZcountInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let err = ZcountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZcountInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(10));
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myzset".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzcount_basic\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zcount_basic 1 one 2 two 3 three 4 four 5 five
                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$12\r\nzcount_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n$1\r\n4\r\n$4\r\nfour\r\n$1\r\n5\r\n$4\r\nfive\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZcountInput::new(
                            RedisKey::String("zcount_basic".into()),
                            RedisJsonValue::Integer(2),
                            RedisJsonValue::Integer(4),
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3); // two, three, four
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_exclusive_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzcount_exclusive\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$16\r\nzcount_exclusive\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    // Exclusive range (1 < score < 3)
                    let result = ctx
                        .raw(&ZcountInput::new(
                            RedisKey::String("zcount_exclusive".into()),
                            RedisJsonValue::String("(1".into()),
                            RedisJsonValue::String("(3".into()),
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1); // only "b" with score 2
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_infinite_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzcount_inf\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$10\r\nzcount_inf\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZcountInput::new(
                                RedisKey::String("zcount_inf".into()),
                                RedisJsonValue::String("-inf".into()),
                                RedisJsonValue::String("+inf".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzcount_noexist\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZcountInput::new(
                                RedisKey::String("zcount_noexist".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(100),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$12\r\nzcount_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZcountInput::new(
                                RedisKey::String("zcount_wrong".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(10),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = ZcountOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nzcount_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$9\r\nzcount_r2\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n5\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result = ctx
                .raw(
                    &ZcountInput::new(RedisKey::String("zcount_r2".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(3))
                        .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ZcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1); // only "a" with score 1

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zcount_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nzcount_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$9\r\nzcount_r3\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n5\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result = ctx
                .raw(
                    &ZcountInput::new(RedisKey::String("zcount_r3".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(10))
                        .command(),
                )
                .await
                .expect("raw failed");

            let output = ZcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 2);

            ctx.stop().await;
        }
    }
}
