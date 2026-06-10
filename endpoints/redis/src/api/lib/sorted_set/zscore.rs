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

const API_INFO: ApiInfo<RedisApi, ZscoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zscore,
    "Returns the score of a member in a sorted set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZSCORE`
/// https://redis.io/docs/latest/commands/zscore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZscoreInput {
    key: RedisKey,
    member: RedisJsonValue,
}

impl ZscoreInput {
    pub fn new(key: impl Into<RedisKey>, member: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), member: member.into() }
    }
}

impl Serialize for ZscoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZscoreInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

impl_redis_operation!(
    ZscoreInput,
    API_INFO,
    {key, member}
);

impl RedisCommandInput for ZscoreInput {
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
        if args.len() != 2 {
            return Err(EpError::request(format!("ZSCORE requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, member: args[1].clone() })
    }
}

/// Output for Redis ZSCORE command
///
/// Returns the score of a member in a sorted set, or None if member doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZscoreOutput {
    score: Option<f64>,
}

impl ZscoreOutput {
    pub fn new(score: Option<f64>) -> Self {
        Self { score }
    }

    /// Get the score of the member
    pub fn score(&self) -> Option<f64> {
        self.score
    }

    /// Check if the member exists in the sorted set
    pub fn exists(&self) -> bool {
        self.score.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let score = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => None,
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(b)) => {
                let s = String::from_utf8(b).map_err(EpError::parse)?;
                Some(s.parse::<f64>().map_err(|_| EpError::parse("score must be numeric"))?)
            }
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => Some(n as f64),
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => None,
            DecoderRespFrame::Resp3(Resp3Frame::Double { data, .. }) => Some(data),
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Some(s.parse::<f64>().map_err(|_| EpError::parse("score must be numeric"))?)
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Some(data as f64),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { score })
    }
}

impl Serialize for ZscoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZscoreOutput", 1)?;
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
        fn test_encode_command() {
            let input = ZscoreInput::new(RedisKey::String("myzset".into()), "member1");
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$6\r\nZSCORE\r\n$6\r\nmyzset\r\n$7\r\nmember1\r\n");
        }

        #[test]
        fn test_decode_output_score() {
            let output = ZscoreOutput::decode(b"$3\r\n1.5\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.score(), Some(1.5));
        }

        #[test]
        fn test_decode_output_null() {
            let output = ZscoreOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.score(), None);
        }

        #[test]
        fn test_decode_output_integer() {
            let output = ZscoreOutput::decode(b"$1\r\n5\r\n").unwrap();
            assert_eq!(output.score(), Some(5.0));
        }

        #[test]
        fn test_decode_output_negative() {
            let output = ZscoreOutput::decode(b"$5\r\n-2.51\r\n").unwrap();
            assert_eq!(output.score(), Some(-2.51));
        }

        #[test]
        fn test_decode_error() {
            let err = ZscoreOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("member1".into())];
            let input = ZscoreInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let err = ZscoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZscoreInput::new(RedisKey::String("myzset".into()), "member");
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
        async fn test_zscore_existing_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzscore_exist\r\n").await.expect("raw failed");

                    // ZADD zscore_exist 2.51 pi
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzscore_exist\r\n$4\r\n2.51\r\n$2\r\npi\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_exist".into()), "pi").command()).await.expect("raw failed");

                    let output = ZscoreOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!((output.score().unwrap() - 2.51).abs() < 0.001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzscore_nomemb\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$13\r\nzscore_nomemb\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZscoreInput::new(RedisKey::String("zscore_nomemb".into()), "nonexistent").command())
                        .await
                        .expect("raw failed");

                    let output = ZscoreOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                    assert_eq!(output.score(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzscore_nokey\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_nokey".into()), "member").command()).await.expect("raw failed");

                    let output = ZscoreOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_integer_score() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzscore_int\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzscore_int\r\n$2\r\n42\r\n$6\r\nanswer\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_int".into()), "answer").command()).await.expect("raw failed");

                    let output = ZscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.score(), Some(42.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_negative_score() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzscore_neg\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzscore_neg\r\n$4\r\n-100\r\n$3\r\nneg\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_neg".into()), "neg").command()).await.expect("raw failed");

                    let output = ZscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.score(), Some(-100.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$12\r\nzscore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_wrong".into()), "member").command()).await.expect("raw failed");

                    let err = ZscoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nzscore_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$9\r\nzscore_r2\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_r2".into()), "a").command()).await.expect("raw failed");

            // RESP2 returns bulk string for scores
            assert!(result.starts_with(b"$"), "RESP2 bulk string format");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscore_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nzscore_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$9\r\nzscore_r3\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZscoreInput::new(RedisKey::String("zscore_r3".into()), "a").command()).await.expect("raw failed");

            let output = ZscoreOutput::decode(&result).expect("decode failed");
            assert_eq!(output.score(), Some(5.0));

            ctx.stop().await;
        }
    }
}
