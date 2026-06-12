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

const API_INFO: ApiInfo<RedisApi, SmismemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Smismember,
    "Determines whether multiple members belong to a set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SMISMEMBER`
/// https://redis.io/docs/latest/commands/smismember/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SmismemberInput {
    pub(crate) key: RedisKey,
    pub(crate) member: Vec<RedisJsonValue>,
}

impl SmismemberInput {
    pub fn new(key: impl Into<RedisKey>, member: Vec<RedisJsonValue>) -> Self {
        Self { key: key.into(), member }
    }
}

impl Serialize for SmismemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SmismemberInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

impl_redis_operation!(
    SmismemberInput,
    API_INFO,
    {key, member }
);

impl RedisCommandInput for SmismemberInput {
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
            return Err(EpError::request(format!("SMISMEMBER requires at least two arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, member: args[1..].to_vec() })
    }
}

/// Output for Redis SMISMEMBER command
///
/// Returns whether each member exists in the set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SmismemberOutput {
    results: Vec<bool>,
}

impl SmismemberOutput {
    pub fn new(results: Vec<bool>) -> Self {
        Self { results }
    }

    pub fn results(&self) -> &[bool] {
        &self.results
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => {
                let mut results = Vec::new();
                for item in items {
                    match item {
                        Resp2Frame::Integer(i) => results.push(i == 1),
                        Resp2Frame::Error(e) => {
                            return Err(EpError::parse(e));
                        }
                        _ => return Err(EpError::parse("unexpected array item format")),
                    }
                }
                results
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut results = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::Number { data, .. } => results.push(data == 1),
                        Resp3Frame::SimpleError { data, .. } => {
                            return Err(EpError::parse(data));
                        }
                        Resp3Frame::BlobError { data, .. } => {
                            return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
                        }
                        _ => return Err(EpError::parse("unexpected array item format")),
                    }
                }
                results
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { results })
    }
}

impl Serialize for SmismemberOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SmismemberOutput", 1)?;
        state.serialize_field("results", &self.results)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_member() {
            let input = SmismemberInput::new(RedisKey::String("myset".into()), vec![RedisJsonValue::String("member1".into())]);
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$10\r\nSMISMEMBER\r\n$5\r\nmyset\r\n$7\r\nmember1\r\n");
        }

        #[test]
        fn test_encode_command_multiple_members() {
            let input = SmismemberInput::new(
                RedisKey::String("myset".into()),
                vec![RedisJsonValue::String("member1".into()), RedisJsonValue::String("member2".into())],
            );
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*4\r\n$10\r\nSMISMEMBER\r\n$5\r\nmyset\r\n$7\r\nmember1\r\n$7\r\nmember2\r\n");
        }

        #[test]
        fn test_decode_output_all_exist() {
            let output = SmismemberOutput::decode(b"*2\r\n:1\r\n:1\r\n").unwrap();
            assert_eq!(output.results(), &[true, true]);
        }

        #[test]
        fn test_decode_output_none_exist() {
            let output = SmismemberOutput::decode(b"*2\r\n:0\r\n:0\r\n").unwrap();
            assert_eq!(output.results(), &[false, false]);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = SmismemberOutput::decode(b"*3\r\n:1\r\n:0\r\n:1\r\n").unwrap();
            assert_eq!(output.results(), &[true, false, true]);
        }

        #[test]
        fn test_decode_output_single_member() {
            let output = SmismemberOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.results(), &[true]);
        }

        #[test]
        fn test_decode_output_error() {
            let err = SmismemberOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myset".into()),
                RedisJsonValue::String("member1".into()),
                RedisJsonValue::String("member2".into()),
            ];
            let input = SmismemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert_eq!(input.member.len(), 2);
            assert_eq!(input.member[0], RedisJsonValue::String("member1".into()));
            assert_eq!(input.member[1], RedisJsonValue::String("member2".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![RedisJsonValue::String("myset".into())];
            let err = SmismemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least two arguments"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SmismemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least two arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SmismemberInput::new(RedisKey::String("myset".into()), vec![RedisJsonValue::String("member1".into())]);
            assert_eq!(input.keys().len(), 1);
        }

        #[test]
        fn test_new_helper() {
            let input =
                SmismemberInput::new("myset", vec![RedisJsonValue::String("member1".into()), RedisJsonValue::String("member2".into())]);
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert_eq!(input.member.len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_all_exist() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test1\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsmismember_test1\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_test1".into()),
                                vec![RedisJsonValue::String("one".into()), RedisJsonValue::String("two".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[true, true]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_none_exist() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test2\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsmismember_test2\r\n$3\r\none\r\n$3\r\ntwo\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_test2".into()),
                                vec![RedisJsonValue::String("three".into()), RedisJsonValue::String("four".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[false, false]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_mixed() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test3\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsmismember_test3\r\n$3\r\none\r\n$5\r\nthree\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_test3".into()),
                                vec![
                                    RedisJsonValue::String("one".into()),
                                    RedisJsonValue::String("two".into()),
                                    RedisJsonValue::String("three".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[true, false, true]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_empty_set() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test4\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_test4".into()),
                                vec![RedisJsonValue::String("member1".into()), RedisJsonValue::String("member2".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[false, false]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_wrongtype() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$16\r\nsmismember_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_wrong".into()),
                                vec![RedisJsonValue::String("member".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = SmismemberOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_single_member() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test5\r\n").await.expect("raw failed");

                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsmismember_test5\r\n$3\r\none\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(RedisKey::String("smismember_test5".into()), vec![RedisJsonValue::String("one".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[true]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smismember_integer_members() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsmismember_test6\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsmismember_test6\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SmismemberInput::new(
                                RedisKey::String("smismember_test6".into()),
                                vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(4), RedisJsonValue::Integer(2)],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmismemberOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results(), &[true, false, true]);
                })
            })
            .await;
        }
    }
}
