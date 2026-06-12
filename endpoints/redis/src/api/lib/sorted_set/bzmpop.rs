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

const API_INFO: ApiInfo<RedisApi, BzmpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bzmpop,
    "Removes and returns a member by score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped",
    ReqType::Write,
    false,
);

/// MIN or MAX direction for BZMPOP
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, ToSchema, JsonSchema,
)]
pub enum PopDirection {
    #[default]
    MIN,
    MAX,
}

impl PopDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            PopDirection::MIN => "MIN",
            PopDirection::MAX => "MAX",
        }
    }
}

/// See official Redis documentation for `BZMPOP`
/// https://redis.io/docs/latest/commands/bzmpop/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BzmpopInput {
    timeout: RedisJsonValue,
    keys: Vec<RedisKey>,
    direction: PopDirection,
    count: Option<RedisJsonValue>,
}

impl BzmpopInput {
    pub fn new(timeout: impl Into<RedisJsonValue>, keys: Vec<impl Into<RedisKey>>, direction: PopDirection) -> Self {
        Self {
            timeout: timeout.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
            direction,
            count: None,
        }
    }

    pub fn min(timeout: impl Into<RedisJsonValue>, keys: Vec<impl Into<RedisKey>>) -> Self {
        Self::new(timeout, keys, PopDirection::MIN)
    }

    pub fn max(timeout: impl Into<RedisJsonValue>, keys: Vec<impl Into<RedisKey>>) -> Self {
        Self::new(timeout, keys, PopDirection::MAX)
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for BzmpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BzmpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("timeout", &self.timeout)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("direction", &self.direction)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(BzmpopInput, API_INFO, { timeout, keys, direction, count });

impl RedisCommandInput for BzmpopInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.timeout).arg(self.keys.len());

        for key in &self.keys {
            command.arg(key);
        }

        command.arg(self.direction.as_str());

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("BZMPOP requires at least 4 arguments, given {}", args.len())));
        }

        let timeout = args[0].clone();
        let numkeys = match &args[1] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be an integer")),
        };

        if args.len() < 2 + numkeys + 1 {
            return Err(EpError::request("Insufficient arguments for keys and MIN/MAX"));
        }

        let mut keys = Vec::new();
        for key in args[2..2 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        let direction = match &args[2 + numkeys] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "MIN" => PopDirection::MIN,
                "MAX" => PopDirection::MAX,
                _ => return Err(EpError::parse("Expected MIN or MAX")),
            },
            _ => return Err(EpError::parse("MIN/MAX must be string")),
        };

        let count = if args.len() > 3 + numkeys {
            if let RedisJsonValue::String(cmd) = &args[3 + numkeys] {
                if cmd.to_uppercase() == "COUNT" && args.len() > 4 + numkeys {
                    Some(args[4 + numkeys].clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { timeout, keys, direction, count })
    }
}

/// Entry with member and score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct BzmpopEntry {
    pub member: String,
    pub score: f64,
}

/// Output for Redis BZMPOP command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BzmpopOutput {
    key: Option<String>,
    entries: Vec<BzmpopEntry>,
}

impl BzmpopOutput {
    pub fn new(key: String, entries: Vec<BzmpopEntry>) -> Self {
        Self { key: Some(key), entries }
    }

    pub fn null() -> Self {
        Self { key: None, entries: Vec::new() }
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn entries(&self) -> &[BzmpopEntry] {
        &self.entries
    }

    pub fn is_null(&self) -> bool {
        self.key.is_none()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(Self::null()),
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) if arr.len() == 2 => {
                let key = match &arr[0] {
                    Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected bulk string for key")),
                };

                let entries = match &arr[1] {
                    Resp2Frame::Array(members) => {
                        let mut entries = Vec::new();
                        for member_score in members.chunks(2) {
                            if member_score.len() != 2 {
                                return Err(EpError::parse("Invalid member/score pair"));
                            }
                            let member = match &member_score[0] {
                                Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                                _ => return Err(EpError::parse("Expected bulk string for member")),
                            };
                            let score = match &member_score[1] {
                                Resp2Frame::BulkString(data) => String::from_utf8(data.to_vec())
                                    .map_err(EpError::parse)?
                                    .parse::<f64>()
                                    .map_err(|_| EpError::parse("Invalid score"))?,
                                _ => return Err(EpError::parse("Expected bulk string for score")),
                            };
                            entries.push(BzmpopEntry { member, score });
                        }
                        entries
                    }
                    _ => return Err(EpError::parse("Expected array for members")),
                };

                Ok(Self { key: Some(key), entries })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(Self::null()),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) if data.len() == 2 => {
                let key = match &data[0] {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("Expected blob string for key")),
                };

                let entries = match &data[1] {
                    Resp3Frame::Array { data, .. } => {
                        let mut entries = Vec::new();
                        for entry in data {
                            match entry {
                                Resp3Frame::Array { data, .. } if data.len() == 2 => {
                                    let member = match &data[0] {
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                                        _ => return Err(EpError::parse("Expected blob string")),
                                    };
                                    let score = match &data[1] {
                                        Resp3Frame::Double { data, .. } => *data,
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                                            .map_err(EpError::parse)?
                                            .parse::<f64>()
                                            .map_err(|_| EpError::parse("Invalid score"))?,
                                        _ => return Err(EpError::parse("Expected double")),
                                    };
                                    entries.push(BzmpopEntry { member, score });
                                }
                                _ => return Err(EpError::parse("Expected [member, score] array")),
                            }
                        }
                        entries
                    }
                    _ => return Err(EpError::parse("Expected array for members")),
                };

                Ok(Self { key: Some(key), entries })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("BZMPOP unexpected response format")),
        }
    }
}

impl Serialize for BzmpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BzmpopOutput", 2)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("entries", &self.entries)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_min() {
            let input = BzmpopInput::min(RedisJsonValue::Integer(0), vec![RedisKey::String("myzset".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BZMPOP"));
            assert!(cmd_str.contains("0"));
            assert!(cmd_str.contains("1")); // numkeys
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("MIN"));
        }

        #[test]
        fn test_encode_command_max_with_count() {
            let input =
                BzmpopInput::max(RedisJsonValue::Integer(5), vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())])
                    .with_count(RedisJsonValue::Integer(3));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("BZMPOP"));
            assert!(cmd_str.contains("MAX"));
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("3"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = BzmpopOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_null());
        }

        #[test]
        fn test_decode_error() {
            let err = BzmpopOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("MIN".into()),
            ];
            let input = BzmpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.direction, PopDirection::MIN);
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("MAX".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = BzmpopInput::decode(args).unwrap();
            assert_eq!(input.direction, PopDirection::MAX);
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("zset1".into()),
            ];
            let err = BzmpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("BZMPOP requires at least 4 arguments"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = BzmpopInput::min(RedisJsonValue::Integer(0), vec![RedisKey::String("a".into()), RedisKey::String("b".into())]);
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
        async fn test_bzmpop_min_immediate() {
            // BZMPOP requires Redis 7.0+
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nbzmpop_immed\r\n").await.expect("del");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$12\r\nbzmpop_immed\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n5\r\n$5\r\nthree\r\n")
                        .await
                        .expect("zadd");

                    let result = ctx
                        .raw(&BzmpopInput::min(RedisJsonValue::Integer(0), vec![RedisKey::String("bzmpop_immed".into())]).command())
                        .await
                        .expect("raw failed");

                    let output = BzmpopOutput::decode(&result).expect("decode");
                    assert!(!output.is_null());
                    assert_eq!(output.key(), Some("bzmpop_immed"));
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].member, "one");
                    assert_eq!(output.entries()[0].score, 1.0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bzmpop_max_with_count() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nbzmpop_count\r\n").await.expect("del");

                    ctx.raw(
                        b"*8\r\n$4\r\nZADD\r\n$12\r\nbzmpop_count\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
                    )
                    .await
                    .expect("zadd");

                    let result = ctx
                        .raw(
                            &BzmpopInput::max(RedisJsonValue::Integer(0), vec![RedisKey::String("bzmpop_count".into())])
                                .with_count(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BzmpopOutput::decode(&result).expect("decode");
                    assert!(!output.is_null());
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bzmpop_timeout() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nbzmpop_timeout\r\n").await.expect("del");

                    let start = std::time::Instant::now();
                    let result = ctx
                        .raw(&BzmpopInput::min(RedisJsonValue::Integer(1), vec![RedisKey::String("bzmpop_timeout".into())]).command())
                        .await
                        .expect("raw failed");

                    let elapsed = start.elapsed();
                    assert!(elapsed.as_secs() >= 1, "Should block for at least 1 second");

                    let output = BzmpopOutput::decode(&result).expect("decode");
                    assert!(output.is_null());
                })
            })
            .await;
        }
    }
}
