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

const API_INFO: ApiInfo<RedisApi, ZmscoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zmscore,
    "Returns the score of one or more members in a sorted set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZMSCORE`
/// https://redis.io/docs/latest/commands/zmscore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZmscoreInput {
    key: RedisKey,
    members: Vec<RedisJsonValue>,
}

impl ZmscoreInput {
    pub fn new(key: impl Into<RedisKey>, members: Vec<impl Into<RedisJsonValue>>) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(|m| m.into()).collect(),
        }
    }

    pub fn single(key: impl Into<RedisKey>, member: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), members: vec![member.into()] }
    }
}

impl Serialize for ZmscoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZmscoreInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

impl_redis_operation!(
    ZmscoreInput,
    API_INFO,
    {key, members}
);

impl RedisCommandInput for ZmscoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);
        for member in &self.members {
            command.arg(member);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("ZMSCORE requires at least 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            members: args[1..].to_vec(),
        })
    }
}

/// Output for Redis ZMSCORE command
///
/// Returns an array of scores corresponding to the requested members.
/// Each score is either a float (if member exists) or None (if member doesn't exist).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZmscoreOutput {
    scores: Vec<Option<f64>>,
}

impl ZmscoreOutput {
    pub fn new(scores: Vec<Option<f64>>) -> Self {
        Self { scores }
    }

    /// Get the scores array
    pub fn scores(&self) -> &[Option<f64>] {
        &self.scores
    }

    /// Get the number of scores returned
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Get the score at a specific index
    pub fn get(&self, index: usize) -> Option<Option<f64>> {
        self.scores.get(index).copied()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let scores = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut scores = Vec::new();
                for frame in arr {
                    let score = match frame {
                        Resp2Frame::Null => None,
                        Resp2Frame::BulkString(b) => {
                            let s = String::from_utf8(b).map_err(EpError::parse)?;
                            Some(s.parse::<f64>().map_err(|_| EpError::parse("score must be numeric"))?)
                        }
                        Resp2Frame::Integer(n) => Some(n as f64),
                        _ => return Err(EpError::parse("unexpected score format")),
                    };
                    scores.push(score);
                }
                scores
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut scores = Vec::new();
                for frame in data {
                    let score = match frame {
                        Resp3Frame::Null => None,
                        Resp3Frame::Double { data, .. } => Some(data),
                        Resp3Frame::BlobString { data, .. } => {
                            let s = String::from_utf8(data).map_err(EpError::parse)?;
                            Some(s.parse::<f64>().map_err(|_| EpError::parse("score must be numeric"))?)
                        }
                        Resp3Frame::Number { data, .. } => Some(data as f64),
                        _ => return Err(EpError::parse("unexpected score format")),
                    };
                    scores.push(score);
                }
                scores
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected array response")),
        };

        Ok(Self { scores })
    }
}

impl Serialize for ZmscoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZmscoreOutput", 1)?;
        state.serialize_field("scores", &self.scores)?;
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
            let input = ZmscoreInput::single(RedisKey::String("myzset".into()), "member1");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZMSCORE"));
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("member1"));
        }

        #[test]
        fn test_encode_command_multiple() {
            let input = ZmscoreInput::new(RedisKey::String("myzset".into()), vec!["member1", "member2", "member3"]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("member1"));
            assert!(cmd_str.contains("member2"));
            assert!(cmd_str.contains("member3"));
        }

        #[test]
        fn test_decode_output_all_exist() {
            // Array of bulk strings representing scores
            let output = ZmscoreOutput::decode(b"*3\r\n$3\r\n1.5\r\n$3\r\n2.5\r\n$3\r\n3.5\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.get(0), Some(Some(1.5)));
            assert_eq!(output.get(1), Some(Some(2.5)));
            assert_eq!(output.get(2), Some(Some(3.5)));
        }

        #[test]
        fn test_decode_output_mixed() {
            // Array with some nulls (non-existent members)
            let output = ZmscoreOutput::decode(b"*3\r\n$3\r\n1.5\r\n$-1\r\n$3\r\n3.5\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.get(0), Some(Some(1.5)));
            assert_eq!(output.get(1), Some(None));
            assert_eq!(output.get(2), Some(Some(3.5)));
        }

        #[test]
        fn test_decode_output_all_null() {
            let output = ZmscoreOutput::decode(b"*2\r\n$-1\r\n$-1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.get(0), Some(None));
            assert_eq!(output.get(1), Some(None));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZmscoreOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZmscoreOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("member1".into()),
                RedisJsonValue::String("member2".into()),
            ];
            let input = ZmscoreInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert_eq!(input.members.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let err = ZmscoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZmscoreInput::new(RedisKey::String("myzset".into()), vec!["m1", "m2"]);
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
        async fn test_zmscore_all_exist() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzmscore_exists\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zmscore_exists 1 one 2 two 3 three
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$14\r\nzmscore_exists\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZmscoreInput::new(
                                RedisKey::String("zmscore_exists".into()),
                                vec!["one", "two", "three"],
                            )
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZmscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.get(0), Some(Some(1.0)));
                    assert_eq!(output.get(1), Some(Some(2.0)));
                    assert_eq!(output.get(2), Some(Some(3.0)));
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_mixed() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzmscore_mixed\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$13\r\nzmscore_mixed\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZmscoreInput::new(RedisKey::String("zmscore_mixed".into()), vec!["one", "two", "three"]).command())
                        .await
                        .expect("raw failed");

                    let output = ZmscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.get(0), Some(Some(1.0)));
                    assert_eq!(output.get(1), Some(None)); // "two" doesn't exist
                    assert_eq!(output.get(2), Some(Some(3.0)));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_nonexistent_key() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzmscore_nokey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZmscoreInput::new(RedisKey::String("zmscore_nokey".into()), vec!["member1", "member2"]).command())
                        .await
                        .expect("raw failed");

                    let output = ZmscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.get(0), Some(None));
                    assert_eq!(output.get(1), Some(None));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_float_scores() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzmscore_float\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$13\r\nzmscore_float\r\n$4\r\n2.51\r\n$2\r\npi\r\n$4\r\n2.71\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZmscoreInput::new(RedisKey::String("zmscore_float".into()), vec!["pi", "e"]).command())
                        .await
                        .expect("raw failed");

                    let output = ZmscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert!((output.get(0).unwrap().unwrap() - 2.51).abs() < 0.001);
                    assert!((output.get(1).unwrap().unwrap() - 2.71).abs() < 0.001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_single_member() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzmscore_single\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$14\r\nzmscore_single\r\n$2\r\n42\r\n$6\r\nanswer\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZmscoreInput::single(RedisKey::String("zmscore_single".into()), "answer").command())
                        .await
                        .expect("raw failed");

                    let output = ZmscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.get(0), Some(Some(42.0)));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_wrongtype() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$13\r\nzmscore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZmscoreInput::single(RedisKey::String("zmscore_wrong".into()), "member").command())
                        .await
                        .expect("raw failed");

                    let err = ZmscoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("6.2")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzmscore_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzmscore_r2\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZmscoreInput::single(RedisKey::String("zmscore_r2".into()), "a").command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ZmscoreOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            assert_eq!(output.get(0), Some(Some(5.0)));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmscore_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("6.2")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzmscore_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzmscore_r3\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZmscoreInput::single(RedisKey::String("zmscore_r3".into()), "a").command()).await.expect("raw failed");

            let output = ZmscoreOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            assert_eq!(output.get(0), Some(Some(5.0)));

            ctx.stop().await;
        }
    }
}
