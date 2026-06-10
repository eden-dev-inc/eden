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

const API_INFO: ApiInfo<RedisApi, ZremrangebyrankInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zremrangebyrank,
    "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZREMRANGEBYRANK`
/// https://redis.io/docs/latest/commands/zremrangebyrank/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZremrangebyrankInput {
    key: RedisKey,
    start: RedisJsonValue,
    stop: RedisJsonValue,
}

impl ZremrangebyrankInput {
    pub fn new(key: impl Into<RedisKey>, start: impl Into<RedisJsonValue>, stop: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), start: start.into(), stop: stop.into() }
    }
}

impl Serialize for ZremrangebyrankInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebyrankInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;
        state.end()
    }
}

impl_redis_operation!(
    ZremrangebyrankInput,
    API_INFO,
    {key, start, stop }
);

impl RedisCommandInput for ZremrangebyrankInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.start).arg(&self.stop);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("ZREMRANGEBYRANK requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            start: args[1].clone(),
            stop: args[2].clone(),
        })
    }
}

/// Output for Redis ZREMRANGEBYRANK command
///
/// Returns the number of members removed from the sorted set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZremrangebyrankOutput {
    removed: i64,
}

impl ZremrangebyrankOutput {
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

impl Serialize for ZremrangebyrankOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebyrankOutput", 1)?;
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
            let input = ZremrangebyrankInput::new(RedisKey::String("myzset".into()), 0, 1);
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*4\r\n$15\r\nZREMRANGEBYRANK\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_encode_command_negative_indices() {
            let input = ZremrangebyrankInput::new(RedisKey::String("myzset".into()), -2, -1);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREMRANGEBYRANK"));
            assert!(cmd_str.contains("-2"));
            assert!(cmd_str.contains("-1"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZremrangebyrankOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.removed(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ZremrangebyrankOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.removed(), 5);
        }

        #[test]
        fn test_decode_error() {
            let err = ZremrangebyrankOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = ZremrangebyrankInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let err = ZremrangebyrankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZremrangebyrankInput::new(RedisKey::String("myzset".into()), 0, 1);
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
        async fn test_zremrangebyrank_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzremrank_basic\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zremrank_basic 1 one 2 two 3 three
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$14\r\nzremrank_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    // Remove first two elements (rank 0 and 1)
                    let result = ctx
                        .raw(&ZremrangebyrankInput::new(
                            RedisKey::String("zremrank_basic".into()),
                            0,
                            1,
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);

                    // Verify only "three" remains
                    let zcard_result = ctx
                        .raw(b"*2\r\n$5\r\nZCARD\r\n$14\r\nzremrank_basic\r\n")
                        .await
                        .expect("raw failed");
                    assert!(zcard_result.starts_with(b":1\r\n"));
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyrank_negative_indices() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzremrank_neg\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zremrank_neg 1 one 2 two 3 three 4 four
                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$12\r\nzremrank_neg\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n$1\r\n4\r\n$4\r\nfour\r\n")
                        .await
                        .expect("raw failed");

                    // Remove last two elements using negative indices
                    let result = ctx
                        .raw(&ZremrangebyrankInput::new(
                            RedisKey::String("zremrank_neg".into()),
                            -2,
                            -1,
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyrank_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzremrank_noexist\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZremrangebyrankInput::new(RedisKey::String("zremrank_noexist".into()), 0, 10).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyrank_out_of_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzremrank_range\r\n").await.expect("raw failed");

                    // ZADD with 3 elements
                    ctx.raw(
                        b"*8\r\n$4\r\nZADD\r\n$14\r\nzremrank_range\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
                    )
                    .await
                    .expect("raw failed");

                    // Remove with range beyond set size
                    let result = ctx
                        .raw(&ZremrangebyrankInput::new(RedisKey::String("zremrank_range".into()), 0, 100).command())
                        .await
                        .expect("raw failed");

                    let output = ZremrangebyrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyrank_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$14\r\nzremrank_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZremrangebyrankInput::new(RedisKey::String("zremrank_wrong".into()), 0, 1).command())
                        .await
                        .expect("raw failed");

                    let err = ZremrangebyrankOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebyrank_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzremrank_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$11\r\nzremrank_r2\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result =
                ctx.raw(&ZremrangebyrankInput::new(RedisKey::String("zremrank_r2".into()), 0, 0).command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 integer format");
            let output = ZremrangebyrankOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 1);

            ctx.stop().await;
        }
    }
}
