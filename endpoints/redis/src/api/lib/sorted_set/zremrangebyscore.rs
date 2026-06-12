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

const API_INFO: ApiInfo<RedisApi, ZremrangebyscoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zremrangebyscore,
    "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZREMRANGEBYSCORE`
/// https://redis.io/docs/latest/commands/zremrangebyscore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZremrangebyscoreInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
}

impl ZremrangebyscoreInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), min: min.into(), max: max.into() }
    }
}

impl Serialize for ZremrangebyscoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebyscoreInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.end()
    }
}

impl_redis_operation!(
    ZremrangebyscoreInput,
    API_INFO,
    {key, min, max }
);

impl RedisCommandInput for ZremrangebyscoreInput {
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
            return Err(EpError::request(format!("ZREMRANGEBYSCORE requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            min: args[1].clone(),
            max: args[2].clone(),
        })
    }
}

/// Output for Redis ZREMRANGEBYSCORE command
///
/// Returns the number of members removed from the sorted set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZremrangebyscoreOutput {
    removed: i64,
}

impl ZremrangebyscoreOutput {
    pub fn new(removed: i64) -> Self {
        Self { removed }
    }

    /// Get the number of members removed
    pub fn removed(&self) -> i64 {
        self.removed
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let removed = match frame {
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

        Ok(Self { removed })
    }
}

impl Serialize for ZremrangebyscoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebyscoreOutput", 1)?;
        state.serialize_field("removed", &self.removed)?;
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
            let input = ZremrangebyscoreInput::new(RedisKey::String("myzset".into()), "-inf", "(2");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREMRANGEBYSCORE"));
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("-inf"));
        }

        #[test]
        fn test_encode_command_numeric_scores() {
            let input = ZremrangebyscoreInput::new(RedisKey::String("myzset".into()), 1.0, 5.0);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREMRANGEBYSCORE"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZremrangebyscoreOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.removed(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ZremrangebyscoreOutput::decode(b":10\r\n").unwrap();
            assert_eq!(output.removed(), 10);
        }

        #[test]
        fn test_decode_error() {
            let err =
                ZremrangebyscoreOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            ];
            let input = ZremrangebyscoreInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let err = ZremrangebyscoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZremrangebyscoreInput::new(RedisKey::String("myzset".into()), 0, 100);
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
        async fn test_zremrangebyscore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzremscore_basic\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zremscore_basic 1 one 2 two 3 three
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$15\r\nzremscore_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    // Remove members with scores between 1 and 2 (inclusive)
                    let result = ctx
                        .raw(&ZremrangebyscoreInput::new(
                            RedisKey::String("zremscore_basic".into()),
                            "1",
                            "2",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyscore_exclusive() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzremscore_excl\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD with scores 1, 2, 3, 4, 5
                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$14\r\nzremscore_excl\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n5\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Remove with exclusive bounds (2 < score < 4)
                    let result = ctx
                        .raw(&ZremrangebyscoreInput::new(
                            RedisKey::String("zremscore_excl".into()),
                            "(2",
                            "(4",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 1); // Only score 3
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyscore_inf() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzremscore_inf\r\n").await.expect("raw failed");

                    ctx.raw(
                        b"*8\r\n$4\r\nZADD\r\n$13\r\nzremscore_inf\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
                    )
                    .await
                    .expect("raw failed");

                    // Remove all with -inf to +inf
                    let result = ctx
                        .raw(&ZremrangebyscoreInput::new(RedisKey::String("zremscore_inf".into()), "-inf", "+inf").command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyscore_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzremscore_noexist\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZremrangebyscoreInput::new(RedisKey::String("zremscore_noexist".into()), "-inf", "+inf").command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyscore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\nzremscore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZremrangebyscoreInput::new(RedisKey::String("zremscore_wrong".into()), "0", "10").command())
                        .await
                        .expect("raw failed");

                    let err = ZremrangebyscoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyscore_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzremscore_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$12\r\nzremscore_r2\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result = ctx
                .raw(&ZremrangebyscoreInput::new(RedisKey::String("zremscore_r2".into()), "1", "1").command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 integer format");

            ctx.stop().await;
        }
    }
}
