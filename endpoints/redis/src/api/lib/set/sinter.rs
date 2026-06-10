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

const API_INFO: ApiInfo<RedisApi, SinterInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Sinter, "Returns the intersect of multiple sets", ReqType::Read, true);

/// See official Redis documentation for `SINTER`
/// https://redis.io/docs/latest/commands/sinter/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SinterInput {
    keys: Vec<RedisKey>,
}

impl SinterInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        Self { keys: keys.into_iter().map(|k| k.into()).collect() }
    }
}

impl Serialize for SinterInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SinterInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(SinterInput, API_INFO, { keys });

impl RedisCommandInput for SinterInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.keys);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SINTER requires at least 1 key"));
        }

        let mut keys = vec![];
        for key in args.into_iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { keys })
    }
}

/// Output for Redis SINTER command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SinterOutput {
    members: Vec<String>,
}

impl SinterOutput {
    pub fn new(members: Vec<String>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut members = Vec::new();
                for frame in arr {
                    let member = match frame {
                        Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected bulk string")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    let member = match frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected blob string")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Set { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    let member = match frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected blob string for member")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("SINTER must return an array or set")),
        }
    }
}

impl Serialize for SinterOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SinterOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = SinterInput::new(vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SINTER"));
            assert!(cmd_str.contains("set1"));
            assert!(cmd_str.contains("set2"));
        }

        #[test]
        fn test_encode_command_single_key() {
            let input = SinterInput::new(vec![RedisKey::String("set1".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SINTER"));
            assert!(cmd_str.contains("set1"));
        }

        #[test]
        fn test_decode_output_basic() {
            let output = SinterOutput::decode(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.members(), vec!["a", "b", "c"]);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = SinterOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_single_member() {
            let output = SinterOutput::decode(b"*1\r\n$4\r\ntest\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.members(), vec!["test"]);
        }

        #[test]
        fn test_decode_error() {
            let err = SinterOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
                RedisJsonValue::String("set3".into()),
            ];
            let input = SinterInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 3);
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("set1".into())];
            let input = SinterInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args = vec![];
            let err = SinterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 key"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = SinterInput::new(vec![RedisKey::String("a".into()), RedisKey::String("b".into())]);
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinter_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsinter_set1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsinter_set2\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsinter_set3\r\n").await.expect("del");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$11\r\nsinter_set1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("sadd");
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$11\r\nsinter_set2\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("sadd");
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$11\r\nsinter_set3\r\n$1\r\nc\r\n").await.expect("sadd");

                    let result = ctx
                        .raw(
                            &SinterInput::new(vec![
                                RedisKey::String("sinter_set1".into()),
                                RedisKey::String("sinter_set2".into()),
                                RedisKey::String("sinter_set3".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterOutput::decode(&result).expect("decode");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.members(), vec!["c"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinter_two_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nsinter_two_s1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nsinter_two_s2\r\n").await.expect("del");

                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$13\r\nsinter_two_s1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
                        .await
                        .expect("sadd");
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$13\r\nsinter_two_s2\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\ne\r\n").await.expect("sadd");

                    let result = ctx
                        .raw(
                            &SinterInput::new(vec![RedisKey::String("sinter_two_s1".into()), RedisKey::String("sinter_two_s2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterOutput::decode(&result).expect("decode");
                    assert_eq!(output.len(), 2);
                    let members: Vec<&str> = output.members().iter().map(|s| s.as_str()).collect();
                    assert!(members.contains(&"b"));
                    assert!(members.contains(&"c"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinter_empty_intersection() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsinter_em_set1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsinter_em_set2\r\n").await.expect("del");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$14\r\nsinter_em_set1\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("sadd");
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$14\r\nsinter_em_set2\r\n$1\r\nc\r\n$1\r\nd\r\n").await.expect("sadd");

                    let result = ctx
                        .raw(
                            &SinterInput::new(vec![RedisKey::String("sinter_em_set1".into()), RedisKey::String("sinter_em_set2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterOutput::decode(&result).expect("decode");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinter_single_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsinter_single_s1\r\n").await.expect("del");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsinter_single_s1\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n").await.expect("sadd");

                    let result =
                        ctx.raw(&SinterInput::new(vec![RedisKey::String("sinter_single_s1".into())]).command()).await.expect("raw failed");

                    let output = SinterOutput::decode(&result).expect("decode");
                    assert_eq!(output.len(), 3);
                    let members: Vec<&str> = output.members().iter().map(|s| s.as_str()).collect();
                    assert!(members.contains(&"x"));
                    assert!(members.contains(&"y"));
                    assert!(members.contains(&"z"));
                })
            })
            .await;
        }
    }
}
