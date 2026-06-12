use crate::api::lib::sorted_set::common::Limit;
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

const API_INFO: ApiInfo<RedisApi, ZrevrangebyscoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrevrangebyscore,
    "Returns members in a sorted set within a range of scores in reverse order",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZREVRANGEBYSCORE`
/// https://redis.io/docs/latest/commands/zrevrangebyscore/
///
/// Note: ZREVRANGEBYSCORE is deprecated as of Redis 6.2.0, use ZRANGE with BYSCORE REV instead.
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrevrangebyscoreInput {
    key: RedisKey,
    max: RedisJsonValue,
    min: RedisJsonValue,
    withscores: Option<bool>,
    limit: Option<Limit>,
}

impl ZrevrangebyscoreInput {
    pub fn new(key: impl Into<RedisKey>, max: impl Into<RedisJsonValue>, min: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            withscores: None,
            limit: None,
        }
    }

    pub fn with_scores(mut self) -> Self {
        self.withscores = Some(true);
        self
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit::new(offset, count));
        self
    }
}

impl Serialize for ZrevrangebyscoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.withscores.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZrevrangebyscoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("max", &self.max)?;
        state.serialize_field("min", &self.min)?;
        if let Some(withscores) = &self.withscores {
            state.serialize_field("withscores", &withscores)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", &limit)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZrevrangebyscoreInput,
    API_INFO,
    {key, max, min, withscores, limit }
);

impl RedisCommandInput for ZrevrangebyscoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.max).arg(&self.min);

        if self.withscores == Some(true) {
            command.arg("WITHSCORES");
        }

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("ZREVRANGEBYSCORE requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let max = args[1].clone();
        let min = args[2].clone();
        let mut withscores = None;
        let mut limit = None;
        let mut i = 3;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "WITHSCORES" {
                    withscores = Some(true);
                    i += 1;
                } else if upper == "LIMIT" && i + 2 < args.len() {
                    limit = Some(Limit { offset: args[i + 1].clone(), count: args[i + 2].clone() });
                    i += 3;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, max, min, withscores, limit })
    }
}

/// Entry in ZREVRANGEBYSCORE result with member and optional score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZrevrangebyscoreEntry {
    pub member: String,
    pub score: Option<f64>,
}

/// Output for Redis ZREVRANGEBYSCORE command
///
/// Returns members in reverse score order (highest to lowest).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrevrangebyscoreOutput {
    entries: Vec<ZrevrangebyscoreEntry>,
}

impl ZrevrangebyscoreOutput {
    pub fn new(entries: Vec<ZrevrangebyscoreEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ZrevrangebyscoreEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn members(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.member.as_str()).collect()
    }

    pub fn decode(bytes: &[u8], with_scores: bool) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let entries = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_resp2_array(&arr, with_scores)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_resp3_array(&data, with_scores)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected array response")),
        };

        Ok(Self { entries })
    }

    fn parse_resp2_array(arr: &[Resp2Frame], with_scores: bool) -> Result<Vec<ZrevrangebyscoreEntry>, EpError> {
        let mut entries = Vec::new();

        if with_scores {
            let mut i = 0;
            while i + 1 < arr.len() {
                let member = match &arr[i] {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                let score = match &arr[i + 1] {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("score must be numeric"))?,
                    Resp2Frame::Integer(n) => *n as f64,
                    _ => return Err(EpError::parse("expected numeric score")),
                };
                entries.push(ZrevrangebyscoreEntry { member, score: Some(score) });
                i += 2;
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZrevrangebyscoreEntry { member, score: None });
            }
        }

        Ok(entries)
    }

    fn parse_resp3_array(arr: &[Resp3Frame], with_scores: bool) -> Result<Vec<ZrevrangebyscoreEntry>, EpError> {
        let mut entries = Vec::new();
        if with_scores {
            // Check if it's nested arrays (true RESP3) or flat array (RESP2 fallback)
            let is_nested = arr.first().is_some_and(|f| matches!(f, Resp3Frame::Array { .. }));

            if is_nested {
                // RESP3 nested: [[member1, score1], [member2, score2], ...]
                for entry_frame in arr {
                    match entry_frame {
                        Resp3Frame::Array { data, .. } => {
                            if data.len() != 2 {
                                return Err(EpError::parse("ZREVRANGE entry must have 2 elements"));
                            }

                            let member = match &data[0] {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                                _ => return Err(EpError::parse("expected string member")),
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

                            entries.push(ZrevrangebyscoreEntry { member, score: Some(score) });
                        }
                        _ => {
                            return Err(EpError::parse("expected nested array for ZREVRANGE entry"));
                        }
                    }
                }
            } else {
                // Flat array (RESP2 compatibility): [member1, score1, member2, score2, ...]
                let mut i = 0;
                while i + 1 < arr.len() {
                    let member = match &arr[i] {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected string member")),
                    };
                    let score = match &arr[i + 1] {
                        Resp3Frame::Double { data, .. } => *data,
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                            .map_err(EpError::parse)?
                            .parse::<f64>()
                            .map_err(|_| EpError::parse("score must be numeric"))?,
                        Resp3Frame::Number { data, .. } => *data as f64,
                        _ => return Err(EpError::parse("expected numeric score")),
                    };
                    entries.push(ZrevrangebyscoreEntry { member, score: Some(score) });
                    i += 2;
                }
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZrevrangebyscoreEntry { member, score: None });
            }
        }
        Ok(entries)
    }
}

impl Serialize for ZrevrangebyscoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrevrangebyscoreOutput", 1)?;
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
        fn test_encode_command() {
            let input = ZrevrangebyscoreInput::new(RedisKey::String("myzset".into()), "+inf", "-inf");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREVRANGEBYSCORE"));
            assert!(cmd_str.contains("myzset"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZrevrangebyscoreInput::new(RedisKey::String("myzset".into()), 100, 0).with_scores();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHSCORES"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = ZrevrangebyscoreInput::new(RedisKey::String("myzset".into()), "+inf", "-inf").with_limit(0, 10);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_decode_output_without_scores() {
            let output = ZrevrangebyscoreOutput::decode(b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n", false).unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.members(), vec!["c", "b", "a"]);
        }

        #[test]
        fn test_decode_output_with_scores() {
            let output = ZrevrangebyscoreOutput::decode(b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n", true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].score, Some(2.0));
            assert_eq!(output.entries()[1].score, Some(1.0));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrevrangebyscoreOutput::decode(b"*0\r\n", false).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZrevrangebyscoreOutput::decode(b"-WRONGTYPE Operation\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("+inf".into()),
                RedisJsonValue::String("-inf".into()),
            ];
            let input = ZrevrangebyscoreInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("+inf".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("WITHSCORES".into()),
                RedisJsonValue::String("LIMIT".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = ZrevrangebyscoreInput::decode(args).unwrap();
            assert_eq!(input.withscores, Some(true));
            assert!(input.limit.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("+inf".into())];
            let err = ZrevrangebyscoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrevrangebyscoreInput::new(RedisKey::String("myzset".into()), "+inf", "-inf");
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
        async fn test_zrevrangebyscore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrevrangebyscore_bsc\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD: a=1, b=2, c=3
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$20\r\nzrevrangebyscore_bsc\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(
                            RedisKey::String("zrevrangebyscore_bsc".into()),
                            "+inf",
                            "-inf",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.members(), vec!["c", "b", "a"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrevrangebyscore_rng\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$20\r\nzrevrangebyscore_rng\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n5\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Get scores between 2 and 4 (inclusive) in reverse
                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(
                            RedisKey::String("zrevrangebyscore_rng".into()),
                            "4",
                            "2",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.members(), vec!["d", "c", "b"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_with_scores() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrevrangebyscore_scr\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$20\r\nzrevrangebyscore_scr\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrevrangebyscoreInput::new(RedisKey::String("zrevrangebyscore_scr".into()), "+inf", "-inf")
                                .with_scores()
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, true).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.entries()[0].member, "b");
                    assert_eq!(output.entries()[0].score, Some(2.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_with_limit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrevrangebyscore_lmt\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$20\r\nzrevrangebyscore_lmt\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n5\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Skip 1, take 2
                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(
                            RedisKey::String("zrevrangebyscore_lmt".into()),
                            "+inf",
                            "-inf",
                        ).with_limit(1, 2).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.members(), vec!["d", "c"]); // Skip e, take d and c
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_exclusive() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$21\r\nzrevrangebyscore_excl\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$21\r\nzrevrangebyscore_excl\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n4\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // Exclusive range: 1 < score < 4
                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(
                            RedisKey::String("zrevrangebyscore_excl".into()),
                            "(4",
                            "(1",
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.members(), vec!["c", "b"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$22\r\nzrevrangebyscore_empty\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(RedisKey::String("zrevrangebyscore_empty".into()), "+inf", "-inf").command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangebyscoreOutput::decode(&result, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrangebyscore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$22\r\nzrevrangebyscore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangebyscoreInput::new(RedisKey::String("zrevrangebyscore_wrong".into()), "+inf", "-inf").command())
                        .await
                        .expect("raw failed");

                    let err = ZrevrangebyscoreOutput::decode(&result, false).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
