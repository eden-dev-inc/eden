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

const API_INFO: ApiInfo<RedisApi, BzpopminInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bzpopmin,
    "Removes and returns the member with the lowest score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `BZPOPMIN`
/// https://redis.io/docs/latest/commands/bzpopmin/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BzpopminInput {
    keys: Vec<RedisKey>,
    timeout: RedisJsonValue,
}

impl BzpopminInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>, timeout: impl Into<RedisJsonValue>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            timeout: timeout.into(),
        }
    }

    pub fn single(key: impl Into<RedisKey>, timeout: impl Into<RedisJsonValue>) -> Self {
        Self { keys: vec![key.into()], timeout: timeout.into() }
    }
}

impl Serialize for BzpopminInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BzpopminInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(BzpopminInput, API_INFO, { keys, timeout });

impl RedisCommandInput for BzpopminInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        for key in &self.keys {
            command.arg(key);
        }
        command.arg(&self.timeout);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("BZPOPMIN requires at least 2 arguments, given {}", args.len())));
        }

        let timeout = args[args.len() - 1].clone();
        let mut keys = Vec::new();
        for key in args[0..args.len() - 1].iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { keys, timeout })
    }
}

/// Output for Redis BZPOPMIN command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BzpopminOutput {
    key: Option<String>,
    member: Option<String>,
    score: Option<f64>,
}

impl BzpopminOutput {
    pub fn new(key: Option<String>, member: Option<String>, score: Option<f64>) -> Self {
        Self { key, member, score }
    }

    pub fn null() -> Self {
        Self { key: None, member: None, score: None }
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn member(&self) -> Option<&str> {
        self.member.as_deref()
    }

    pub fn score(&self) -> Option<f64> {
        self.score
    }

    pub fn is_null(&self) -> bool {
        self.key.is_none()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(Self::null()),
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) if arr.len() == 3 => {
                let key = match &arr[0] {
                    Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected bulk string for key")),
                };
                let member = match &arr[1] {
                    Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected bulk string for member")),
                };
                let score = match &arr[2] {
                    Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("Invalid score format"))?,
                    _ => return Err(EpError::parse("Expected bulk string for score")),
                };
                Ok(Self { key: Some(key), member: Some(member), score: Some(score) })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(Self::null()),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) if data.len() == 3 => {
                let key = match &data[0] {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected blob string for key")),
                };
                let member = match &data[1] {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected blob string for member")),
                };
                let score = match &data[2] {
                    Resp3Frame::Double { data, .. } => *data,
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("Invalid score format"))?,
                    _ => return Err(EpError::parse("Expected double for score")),
                };
                Ok(Self { key: Some(key), member: Some(member), score: Some(score) })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("BZPOPMIN unexpected response format")),
        }
    }
}

impl Serialize for BzpopminOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BzpopminOutput", 3)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;
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
        fn test_encode_command_single_key() {
            let input = BzpopminInput::single(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BZPOPMIN"));
            assert!(cmd_str.contains("myzset"));
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input =
                BzpopminInput::new(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())], RedisJsonValue::Integer(5));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("zset1"));
            assert!(cmd_str.contains("zset2"));
        }

        #[test]
        fn test_decode_output_success() {
            let output = BzpopminOutput::decode(b"*3\r\n$6\r\nmyzset\r\n$6\r\nmember\r\n$1\r\n1\r\n").unwrap();
            assert!(!output.is_null());
            assert_eq!(output.key(), Some("myzset"));
            assert_eq!(output.member(), Some("member"));
            assert_eq!(output.score(), Some(1.0));
        }

        #[test]
        fn test_decode_output_null() {
            let output = BzpopminOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_null());
        }

        #[test]
        fn test_decode_error() {
            let err = BzpopminOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("zset1".into()), RedisJsonValue::Integer(10)];
            let input = BzpopminInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(5)];
            let err = BzpopminInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = BzpopminInput::new(vec![RedisKey::String("a".into()), RedisKey::String("b".into())], RedisJsonValue::Integer(0));
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
        async fn test_bzpopmin_immediate() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nbzpopmin_immed\r\n").await.expect("del");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$14\r\nbzpopmin_immed\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n5\r\n$5\r\nthree\r\n")
                        .await
                        .expect("zadd");

                    let result = ctx
                        .raw(&BzpopminInput::single(RedisKey::String("bzpopmin_immed".into()), RedisJsonValue::Integer(0)).command())
                        .await
                        .expect("raw failed");

                    let output = BzpopminOutput::decode(&result).expect("decode");
                    assert!(!output.is_null());
                    assert_eq!(output.key(), Some("bzpopmin_immed"));
                    assert_eq!(output.member(), Some("one"));
                    assert_eq!(output.score(), Some(1.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bzpopmin_timeout() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nbzpopmin_timeout\r\n").await.expect("del");

                    let start = std::time::Instant::now();
                    let result = ctx
                        .raw(&BzpopminInput::single(RedisKey::String("bzpopmin_timeout".into()), RedisJsonValue::Integer(1)).command())
                        .await
                        .expect("raw failed");

                    let elapsed = start.elapsed();
                    assert!(elapsed.as_secs() >= 1, "Should block for at least 1 second");

                    let output = BzpopminOutput::decode(&result).expect("decode");
                    assert!(output.is_null());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bzpopmin_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$14\r\nbzpopmin_wrong\r\n$5\r\nvalue\r\n").await.expect("set");

                    let result = ctx
                        .raw(&BzpopminInput::single(RedisKey::String("bzpopmin_wrong".into()), RedisJsonValue::Integer(0)).command())
                        .await
                        .expect("raw failed");

                    let err = BzpopminOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
