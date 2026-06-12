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

const API_INFO: ApiInfo<RedisApi, SismemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sismember,
    "Determines whether a member belongs to a set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SISMEMBER`
/// https://redis.io/docs/latest/commands/sismember/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SismemberInput {
    pub(crate) key: RedisKey,
    pub(crate) member: RedisJsonValue,
}

impl SismemberInput {
    pub fn new(key: impl Into<RedisKey>, member: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), member: member.into() }
    }
}

impl Serialize for SismemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SismemberInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

impl_redis_operation!(
    SismemberInput,
    API_INFO,
    {key, member }
);

impl RedisCommandInput for SismemberInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.member);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SISMEMBER requires 2 arguments, given {}", args.len())));
        }
        Ok(SismemberInput { key: args[0].clone().try_into()?, member: args[1].clone() })
    }
}

/// Output for Redis SISMEMBER command
///
/// Returns whether the member exists in the set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SismemberOutput {
    is_member: bool,
}

impl SismemberOutput {
    pub fn new(is_member: bool) -> Self {
        Self { is_member }
    }

    pub fn is_member(&self) -> bool {
        self.is_member
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let is_member = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(i)) => i == 1,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data == 1,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { is_member })
    }
}

impl Serialize for SismemberOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SismemberOutput", 1)?;
        state.serialize_field("is_member", &self.is_member)?;
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
            let input = SismemberInput::new(RedisKey::String("myset".into()), RedisJsonValue::String("member1".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$7\r\nmember1\r\n");
        }

        #[test]
        fn test_decode_output_member_exists() {
            let output = SismemberOutput::decode(b":1\r\n").unwrap();
            assert!(output.is_member());
        }

        #[test]
        fn test_decode_output_member_not_exists() {
            let output = SismemberOutput::decode(b":0\r\n").unwrap();
            assert!(!output.is_member());
        }

        #[test]
        fn test_decode_output_error() {
            let err = SismemberOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myset".into()), RedisJsonValue::String("member1".into())];
            let input = SismemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert_eq!(input.member, RedisJsonValue::String("member1".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![RedisJsonValue::String("myset".into())];
            let err = SismemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SismemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SismemberInput::new(RedisKey::String("myset".into()), RedisJsonValue::String("member1".into()));
            assert_eq!(input.keys().len(), 1);
        }

        #[test]
        fn test_new_helper() {
            let input = SismemberInput::new("myset", RedisJsonValue::String("member1".into()));
            assert_eq!(input.key, RedisKey::String("myset".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsismember_test1\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$15\r\nsismember_test1\r\n$3\r\none\r\n$3\r\ntwo\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SismemberInput::new(RedisKey::String("sismember_test1".into()), RedisJsonValue::String("one".into()))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(output.is_member());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_not_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsismember_test2\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$15\r\nsismember_test2\r\n$3\r\none\r\n$3\r\ntwo\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SismemberInput::new(RedisKey::String("sismember_test2".into()), RedisJsonValue::String("three".into()))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_member());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsismember_test3\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SismemberInput::new(RedisKey::String("sismember_test3".into()), RedisJsonValue::String("member".into()))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_member());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\nsismember_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SismemberInput::new(RedisKey::String("sismember_wrong".into()), RedisJsonValue::String("member".into()))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = SismemberOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_multiple_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsismember_test4\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$15\r\nsismember_test4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // Check existing member
                    let result = ctx
                        .raw(&SismemberInput::new(RedisKey::String("sismember_test4".into()), RedisJsonValue::String("b".into())).command())
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(output.is_member());

                    // Check non-existing member
                    let result = ctx
                        .raw(&SismemberInput::new(RedisKey::String("sismember_test4".into()), RedisJsonValue::String("z".into())).command())
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_member());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sismember_integer_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsismember_test5\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$15\r\nsismember_test5\r\n$1\r\n1\r\n$1\r\n2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&SismemberInput::new(RedisKey::String("sismember_test5".into()), RedisJsonValue::Integer(1)).command())
                        .await
                        .expect("raw failed");

                    let output = SismemberOutput::decode(&result).expect("decode failed");
                    assert!(output.is_member());
                })
            })
            .await;
        }
    }
}
