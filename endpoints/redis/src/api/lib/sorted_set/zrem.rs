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

const API_INFO: ApiInfo<RedisApi, ZremInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrem,
    "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZREM`
/// https://redis.io/docs/latest/commands/zrem/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZremInput {
    key: RedisKey,
    members: Vec<RedisJsonValue>,
}

impl ZremInput {
    pub fn new(key: impl Into<RedisKey>, members: Vec<RedisJsonValue>) -> Self {
        Self { key: key.into(), members }
    }

    pub fn single(key: impl Into<RedisKey>, member: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), members: vec![member.into()] }
    }
}

impl Serialize for ZremInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZremInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

impl_redis_operation!(
    ZremInput,
    API_INFO,
    {key, members}
);

impl RedisCommandInput for ZremInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.members);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request("ZREM requires at least 2 arguments"));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            members: args[1..].to_vec(),
        })
    }
}

/// Output for Redis ZREM command
///
/// Returns the number of members removed from the sorted set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZremOutput {
    removed: i64,
}

impl ZremOutput {
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
            _ => return Err(EpError::parse("ZREM must return integer")),
        };

        Ok(Self { removed })
    }
}

impl Serialize for ZremOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZremOutput", 1)?;
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
        fn test_encode_command_single() {
            let input = ZremInput::single(RedisKey::String("myzset".into()), "member1");
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$4\r\nZREM\r\n$6\r\nmyzset\r\n$7\r\nmember1\r\n");
        }

        #[test]
        fn test_encode_command_multiple() {
            let input = ZremInput::new(
                RedisKey::String("myzset".into()),
                vec![RedisJsonValue::String("member1".into()), RedisJsonValue::String("member2".into())],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("member1"));
            assert!(cmd_str.contains("member2"));
        }

        #[test]
        fn test_decode_output_removed() {
            let output = ZremOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.removed(), 3);
            assert!(output.any_removed());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZremOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.removed(), 0);
            assert!(!output.any_removed());
        }

        #[test]
        fn test_decode_error() {
            let err = ZremOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("member1".into())];
            let input = ZremInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert_eq!(input.members.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_members() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("m1".into()),
                RedisJsonValue::String("m2".into()),
                RedisJsonValue::String("m3".into()),
            ];
            let input = ZremInput::decode(args).unwrap();
            assert_eq!(input.members.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let err = ZremInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZremInput::single(RedisKey::String("myzset".into()), "m");
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
        async fn test_zrem_single() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzrem_single\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$11\r\nzrem_single\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n")
                        .await
                        .expect("raw failed");

                    let result =
                        ctx.raw(&ZremInput::single(RedisKey::String("zrem_single".into()), "one").command()).await.expect("raw failed");

                    let output = ZremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_multiple() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzrem_multi\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$10\r\nzrem_multi\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZremInput::new(
                                RedisKey::String("zrem_multi".into()),
                                vec![
                                    RedisJsonValue::String("one".into()),
                                    RedisJsonValue::String("two".into()),
                                ],
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzrem_nomember\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$13\r\nzrem_nomember\r\n$1\r\n1\r\n$3\r\none\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZremInput::single(RedisKey::String("zrem_nomember".into()), "nonexistent").command())
                        .await
                        .expect("raw failed");

                    let output = ZremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                    assert!(!output.any_removed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzrem_nokey\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZremInput::single(RedisKey::String("zrem_nokey".into()), "member").command()).await.expect("raw failed");

                    let output = ZremOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$10\r\nzrem_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZremInput::single(RedisKey::String("zrem_wrong".into()), "member").command()).await.expect("raw failed");

                    let err = ZremOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzrem_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$7\r\nzrem_r2\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZremInput::single(RedisKey::String("zrem_r2".into()), "a").command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ZremOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrem_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzrem_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$7\r\nzrem_r3\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZremInput::single(RedisKey::String("zrem_r3".into()), "a").command()).await.expect("raw failed");

            let output = ZremOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 1);

            ctx.stop().await;
        }
    }
}
