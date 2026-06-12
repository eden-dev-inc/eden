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

const API_INFO: ApiInfo<RedisApi, ZremrangebylexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zremrangebylex,
    "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZREMRANGEBYLEX`
/// https://redis.io/docs/latest/commands/zremrangebylex/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZremrangebylexInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
}

impl ZremrangebylexInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), min: min.into(), max: max.into() }
    }
}

impl Serialize for ZremrangebylexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebylexInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.end()
    }
}

impl_redis_operation!(
    ZremrangebylexInput,
    API_INFO,
    {key, min, max}
);

impl RedisCommandInput for ZremrangebylexInput {
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
            return Err(EpError::request(format!("ZREMRANGEBYLEX requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            min: args[1].clone(),
            max: args[2].clone(),
        })
    }
}

/// Output for Redis ZREMRANGEBYLEX command
///
/// Returns the number of members removed from the sorted set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZremrangebylexOutput {
    removed: i64,
}

impl ZremrangebylexOutput {
    pub fn new(removed: i64) -> Self {
        Self { removed }
    }

    /// Get the number of members removed
    pub fn removed(&self) -> i64 {
        self.removed
    }

    /// Check if any members were removed
    pub fn any_removed(&self) -> bool {
        self.removed > 0
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
            _ => return Err(EpError::parse("ZREMRANGEBYLEX must return integer")),
        };

        Ok(Self { removed })
    }
}

impl Serialize for ZremrangebylexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZremrangebylexOutput", 1)?;
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
            let input = ZremrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("[a".into()),
                RedisJsonValue::String("[c".into()),
            );
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*4\r\n$14\r\nZREMRANGEBYLEX\r\n$6\r\nmyzset\r\n$2\r\n[a\r\n$2\r\n[c\r\n");
        }

        #[test]
        fn test_encode_command_unbounded() {
            let input = ZremrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREMRANGEBYLEX"));
        }

        #[test]
        fn test_decode_output_removed() {
            let output = ZremrangebylexOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.removed(), 5);
            assert!(output.any_removed());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZremrangebylexOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.removed(), 0);
            assert!(!output.any_removed());
        }

        #[test]
        fn test_decode_error() {
            let err = ZremrangebylexOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("[a".into()),
                RedisJsonValue::String("[z".into()),
            ];
            let input = ZremrangebylexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("[a".into())];
            let err = ZremrangebylexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZremrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            );
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
        async fn test_zremrangebylex_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzremrangebylex_basic\r\n")
                        .await
                        .expect("raw failed");

                    // Add members with same score (for lexicographical ordering)
                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$20\r\nzremrangebylex_basic\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_basic".into()),
                                RedisJsonValue::String("[a".into()),
                                RedisJsonValue::String("[c".into()),
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 3); // a, b, c
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_exclusive() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$24\r\nzremrangebylex_exclusive\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$24\r\nzremrangebylex_exclusive\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_exclusive".into()),
                                RedisJsonValue::String("(a".into()),
                                RedisJsonValue::String("(d".into()),
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2); // b, c (exclusive bounds)
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_all() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzremrangebylex_all\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$18\r\nzremrangebylex_all\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_all".into()),
                                RedisJsonValue::String("-".into()),
                                RedisJsonValue::String("+".into()),
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 3);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_empty_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzremrangebylex_empty\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$20\r\nzremrangebylex_empty\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_empty".into()),
                                RedisJsonValue::String("[x".into()),
                                RedisJsonValue::String("[z".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzremrangebylex_nokey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_nokey".into()),
                                RedisJsonValue::String("-".into()),
                                RedisJsonValue::String("+".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$20\r\nzremrangebylex_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremrangebylexInput::new(
                                RedisKey::String("zremrangebylex_wrong".into()),
                                RedisJsonValue::String("-".into()),
                                RedisJsonValue::String("+".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = ZremrangebylexOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zremrangebylex_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzremrangebylex_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*6\r\n$4\r\nZADD\r\n$17\r\nzremrangebylex_r2\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n")
                .await
                .expect("raw failed");

            let result = ctx
                .raw(
                    &ZremrangebylexInput::new(
                        RedisKey::String("zremrangebylex_r2".into()),
                        RedisJsonValue::String("-".into()),
                        RedisJsonValue::String("+".into()),
                    )
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ZremrangebylexOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 2);

            ctx.stop().await;
        }
    }
}
