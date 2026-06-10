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

const API_INFO: ApiInfo<RedisApi, ZrankInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrank,
    "Returns the index of a member in a sorted set ordered by ascending scores",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZRANK`
/// https://redis.io/docs/latest/commands/zrank/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrankInput {
    key: RedisKey,
    member: RedisJsonValue,
    withscore: Option<bool>,
}

impl ZrankInput {
    pub fn new(key: impl Into<RedisKey>, member: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), member: member.into(), withscore: None }
    }

    pub fn with_score(mut self) -> Self {
        self.withscore = Some(true);
        self
    }
}

impl Serialize for ZrankInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.withscore.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZrankInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;

        if let Some(withscore) = &self.withscore {
            state.serialize_field("withscore", withscore)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZrankInput,
    API_INFO,
    {key, member, withscore}
);

impl RedisCommandInput for ZrankInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.member);

        if self.withscore == Some(true) {
            command.arg("WITHSCORE");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request("ZRANK requires at least 2 arguments"));
        }

        let key = args[0].clone().try_into()?;
        let member = args[1].clone();
        let withscore = if args.len() > 2 {
            if let RedisJsonValue::String(cmd) = &args[2] {
                if cmd.to_uppercase() == "WITHSCORE" { Some(true) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { key, member, withscore })
    }
}

/// Output for Redis ZRANK command
///
/// Returns the rank of a member in a sorted set (0-based, lowest score = rank 0),
/// or None if member doesn't exist. Optionally includes the score.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrankOutput {
    rank: Option<i64>,
    score: Option<f64>,
}

impl ZrankOutput {
    pub fn new(rank: Option<i64>, score: Option<f64>) -> Self {
        Self { rank, score }
    }

    pub fn rank(&self) -> Option<i64> {
        self.rank
    }

    pub fn score(&self) -> Option<f64> {
        self.score
    }

    pub fn exists(&self) -> bool {
        self.rank.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => Ok(Self { rank: Some(n), score: None }),
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(Self { rank: None, score: None }),
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() != 2 {
                    return Err(EpError::parse("ZRANK WITHSCORE must return [rank, score]"));
                }
                let rank = match &arr[0] {
                    Resp2Frame::Integer(n) => *n,
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<i64>()
                        .map_err(|_| EpError::parse("rank must be numeric"))?,
                    _ => return Err(EpError::parse("expected integer rank")),
                };
                let score = match &arr[1] {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("score must be numeric"))?,
                    Resp2Frame::Integer(n) => *n as f64,
                    _ => return Err(EpError::parse("expected numeric score")),
                };
                Ok(Self { rank: Some(rank), score: Some(score) })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Ok(Self { rank: Some(data), score: None }),
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(Self { rank: None, score: None }),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() != 2 {
                    return Err(EpError::parse("ZRANK WITHSCORE must return [rank, score]"));
                }
                let rank = match &data[0] {
                    Resp3Frame::Number { data, .. } => *data,
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<i64>()
                        .map_err(|_| EpError::parse("rank must be numeric"))?,
                    _ => return Err(EpError::parse("expected integer rank")),
                };
                let score = match &data[1] {
                    Resp3Frame::Double { data, .. } => *data,
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("score must be numeric"))?,
                    Resp3Frame::Number { data, .. } => *data as f64,
                    _ => return Err(EpError::parse("expected numeric score")),
                };
                Ok(Self { rank: Some(rank), score: Some(score) })
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("unexpected response format")),
        }
    }
}

impl Serialize for ZrankOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrankOutput", 2)?;
        state.serialize_field("rank", &self.rank)?;
        state.serialize_field("score", &self.score)?;
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
            let input = ZrankInput::new(RedisKey::String("myzset".into()), "member1");
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$5\r\nZRANK\r\n$6\r\nmyzset\r\n$7\r\nmember1\r\n");
        }

        #[test]
        fn test_encode_command_with_score() {
            let input = ZrankInput::new(RedisKey::String("myzset".into()), "member1").with_score();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("WITHSCORE"));
        }

        #[test]
        fn test_decode_output_rank() {
            let output = ZrankOutput::decode(b":5\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.rank(), Some(5));
            assert_eq!(output.score(), None);
        }

        #[test]
        fn test_decode_output_null() {
            let output = ZrankOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.rank(), None);
        }

        #[test]
        fn test_decode_output_with_score() {
            // Array: [rank=0, score="2.51"]
            let output = ZrankOutput::decode(b"*2\r\n:0\r\n$4\r\n2.51\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.rank(), Some(0));
            assert!((output.score().unwrap() - 2.51).abs() < 0.001);
        }

        #[test]
        fn test_decode_error() {
            let err = ZrankOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("member".into())];
            let input = ZrankInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_with_withscore() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("member".into()),
                RedisJsonValue::String("WITHSCORE".into()),
            ];
            let input = ZrankInput::decode(args).unwrap();
            assert_eq!(input.withscore, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let err = ZrankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrankInput::new(RedisKey::String("myzset".into()), "m");
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
        async fn test_zrank_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzrank_basic\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD: one=1, two=2, three=3
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$11\r\nzrank_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    // one has lowest score, so rank = 0
                    let result = ctx
                        .raw(&ZrankInput::new(RedisKey::String("zrank_basic".into()), "one").command())
                        .await
                        .expect("raw failed");

                    let output = ZrankOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.rank(), Some(0));

                    // three has highest score, so rank = 2
                    let result = ctx
                        .raw(&ZrankInput::new(RedisKey::String("zrank_basic".into()), "three").command())
                        .await
                        .expect("raw failed");

                    let output = ZrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.rank(), Some(2));
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrank_with_score() {
            // WITHSCORE requires Redis 7.2+
            test_all_protocols_min_version("7.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrank_withscore\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$15\r\nzrank_withscore\r\n$4\r\n2.51\r\n$2\r\npi\r\n$4\r\n2.71\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZrankInput::new(RedisKey::String("zrank_withscore".into()), "pi").with_score().command())
                        .await
                        .expect("raw failed");

                    let output = ZrankOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.rank(), Some(0)); // e has lower score
                    assert!(output.score().is_some());
                    assert!((output.score().unwrap() - 2.51).abs() < 0.001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrank_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzrank_nomemb\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzrank_nomemb\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrankInput::new(RedisKey::String("zrank_nomemb".into()), "nonexistent").command())
                        .await
                        .expect("raw failed");

                    let output = ZrankOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrank_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzrank_nokey\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZrankInput::new(RedisKey::String("zrank_nokey".into()), "member").command()).await.expect("raw failed");

                    let output = ZrankOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrank_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$11\r\nzrank_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZrankInput::new(RedisKey::String("zrank_wrong".into()), "member").command()).await.expect("raw failed");

                    let err = ZrankOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
