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

const API_INFO: ApiInfo<RedisApi, ZdiffInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zdiff,
    "Returns the difference between multiple sorted sets",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZDIFF`
/// https://redis.io/docs/latest/commands/zdiff/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZdiffInput {
    keys: Vec<RedisKey>,
    withscores: Option<bool>,
}

impl ZdiffInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            withscores: None,
        }
    }

    pub fn with_scores(mut self) -> Self {
        self.withscores = Some(true);
        self
    }
}

impl Serialize for ZdiffInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.withscores.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZdiffInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        if let Some(withscores) = &self.withscores {
            state.serialize_field("withscores", withscores)?;
        }
        state.end()
    }
}

impl_redis_operation!(ZdiffInput, API_INFO, { keys, withscores });

impl RedisCommandInput for ZdiffInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(self.keys.len());
        for key in &self.keys {
            command.arg(key);
        }

        if self.withscores == Some(true) {
            command.arg("WITHSCORES");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request(format!("ZDIFF requires at least 1 argument, given {}", args.len())));
        }

        let numkeys = match &args[0] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be a valid integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 1 + numkeys {
            return Err(EpError::request("Insufficient keys for ZDIFF"));
        }

        let mut keys = Vec::new();
        for key in args[1..1 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        let mut withscores = None;
        let mut i = 1 + numkeys;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i]
                && s.to_uppercase() == "WITHSCORES"
            {
                withscores = Some(true);
            }
            i += 1;
        }

        Ok(Self { keys, withscores })
    }
}

/// Entry in ZDIFF result with member and optional score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZdiffEntry {
    pub member: String,
    pub score: Option<f64>,
}

/// Output for Redis ZDIFF command
///
/// Returns the difference between the first sorted set and all successive sorted sets.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZdiffOutput {
    entries: Vec<ZdiffEntry>,
}

impl ZdiffOutput {
    pub fn new(entries: Vec<ZdiffEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ZdiffEntry] {
        &self.entries
    }

    pub fn members(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.member.as_str()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn decode(bytes: &[u8], withscores: bool) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut entries = Vec::new();
                if withscores {
                    // Flat array: [member1, score1, member2, score2, ...]
                    let mut iter = arr.into_iter();
                    while let Some(member_frame) = iter.next() {
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("ZDIFF missing score for member"))?;

                        let member = match member_frame {
                            Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected bulk string for member")),
                        };

                        let score = match score_frame {
                            Resp2Frame::BulkString(data) => Some(
                                String::from_utf8(data)
                                    .map_err(EpError::parse)?
                                    .parse::<f64>()
                                    .map_err(|_| EpError::parse("Invalid score format"))?,
                            ),
                            _ => return Err(EpError::parse("Expected bulk string for score")),
                        };

                        entries.push(ZdiffEntry { member, score });
                    }
                } else {
                    // Just members
                    for frame in arr {
                        let member = match frame {
                            Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected bulk string for member")),
                        };
                        entries.push(ZdiffEntry { member, score: None });
                    }
                }
                Ok(Self { entries })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut entries = Vec::new();
                if withscores {
                    // Check if it's nested arrays (true RESP3) or flat array (RESP2 fallback)
                    let is_nested = data.first().is_some_and(|f| matches!(f, Resp3Frame::Array { .. }));

                    if is_nested {
                        // RESP3 nested: [[member1, score1], [member2, score2], ...]
                        for entry_frame in data {
                            match entry_frame {
                                Resp3Frame::Array { data: entry_data, .. } => {
                                    if entry_data.len() != 2 {
                                        return Err(EpError::parse("ZDIFF entry must have 2 elements"));
                                    }

                                    let member = match &entry_data[0] {
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                                        _ => {
                                            return Err(EpError::parse("Expected blob string for member"));
                                        }
                                    };

                                    let score = match &entry_data[1] {
                                        Resp3Frame::Double { data, .. } => Some(*data),
                                        _ => {
                                            return Err(EpError::parse("Expected double for score"));
                                        }
                                    };

                                    entries.push(ZdiffEntry { member, score });
                                }
                                _ => {
                                    return Err(EpError::parse("Expected nested array for ZDIFF entry"));
                                }
                            }
                        }
                    } else {
                        // Flat array (RESP2 compatibility): [member1, score1, member2, score2, ...]
                        let mut iter = data.into_iter();
                        while let Some(member_frame) = iter.next() {
                            let score_frame = iter.next().ok_or_else(|| EpError::parse("ZDIFF missing score for member"))?;

                            let member = match member_frame {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                                _ => return Err(EpError::parse("Expected blob string for member")),
                            };

                            let score = match score_frame {
                                Resp3Frame::Double { data, .. } => Some(data),
                                Resp3Frame::BlobString { data, .. } => Some(
                                    String::from_utf8(data)
                                        .map_err(EpError::parse)?
                                        .parse::<f64>()
                                        .map_err(|_| EpError::parse("Invalid score format"))?,
                                ),
                                _ => return Err(EpError::parse("Expected double for score")),
                            };

                            entries.push(ZdiffEntry { member, score });
                        }
                    }
                } else {
                    // Just members
                    for frame in data {
                        let member = match frame {
                            Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected blob string for member")),
                        };
                        entries.push(ZdiffEntry { member, score: None });
                    }
                }
                Ok(Self { entries })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("ZDIFF must return an array")),
        }
    }
}

impl Serialize for ZdiffOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZdiffOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = ZdiffInput::new(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZDIFF"));
            assert!(cmd_str.contains("2")); // numkeys
            assert!(cmd_str.contains("zset1"));
            assert!(cmd_str.contains("zset2"));
            // Should NOT contain WITHSCORES
            assert!(!cmd_str.contains("WITHSCORES"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZdiffInput::new(vec![RedisKey::String("zset1".into())]).with_scores();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZDIFF"));
            assert!(cmd_str.contains("WITHSCORES"));
            // Should NOT contain "true" - this was a bug in the original
            assert!(!cmd_str.contains("true"));
        }

        #[test]
        fn test_decode_output_without_scores() {
            // *2\r\n$1\r\na\r\n$1\r\nb\r\n
            let output = ZdiffOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n", false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.members(), vec!["a", "b"]);
            assert!(output.entries()[0].score.is_none());
        }

        #[test]
        fn test_decode_output_with_scores() {
            // *4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n
            let output = ZdiffOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n", true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].score, Some(1.0));
            assert_eq!(output.entries()[1].score, Some(2.0));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZdiffOutput::decode(b"*0\r\n", false).unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = ZdiffOutput::decode(b"-WRONGTYPE Operation\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
            ];
            let input = ZdiffInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_with_withscores() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("WITHSCORES".into()),
            ];
            let input = ZdiffInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.withscores, Some(true));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ZdiffInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = ZdiffInput::new(vec![
                RedisKey::String("a".into()),
                RedisKey::String("b".into()),
                RedisKey::String("c".into()),
            ]);
            assert_eq!(input.keys().len(), 3);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zdiff_basic() {
            // ZDIFF requires Redis 6.2+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzdiff_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzdiff_set2\r\n").await.expect("raw failed");

                    // ZADD zdiff_set1 1 a 2 b 3 c
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$10\r\nzdiff_set1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zdiff_set2 2 b 4 d
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$10\r\nzdiff_set2\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZdiffInput::new(vec![RedisKey::String("zdiff_set1".into()), RedisKey::String("zdiff_set2".into())]).command())
                        .await
                        .expect("raw failed");

                    let output = ZdiffOutput::decode(&result, false).expect("decode failed");
                    // set1 - set2 = {a, c} (b is in both)
                    assert_eq!(output.len(), 2);
                    let members = output.members();
                    assert!(members.contains(&"a"));
                    assert!(members.contains(&"c"));
                    assert!(!members.contains(&"b"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zdiff_with_scores() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzdiff_scores_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzdiff_scores_set2\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$17\r\nzdiff_scores_set1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$17\r\nzdiff_scores_set2\r\n$1\r\n2\r\n$1\r\nb\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZdiffInput::new(vec![
                                RedisKey::String("zdiff_scores_set1".into()),
                                RedisKey::String("zdiff_scores_set2".into()),
                            ])
                            .with_scores()
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZdiffOutput::decode(&result, true).expect("decode failed");

                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].member, "a");
                    assert_eq!(output.entries()[0].score, Some(1.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zdiff_empty_result() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzdiff_empty_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzdiff_empty_set2\r\n").await.expect("raw failed");

                    // set1 is subset of set2
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$16\r\nzdiff_empty_set1\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$16\r\nzdiff_empty_set2\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZdiffInput::new(vec![
                                RedisKey::String("zdiff_empty_set1".into()),
                                RedisKey::String("zdiff_empty_set2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZdiffOutput::decode(&result, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zdiff_nonexistent_keys() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzdiff_noexist_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzdiff_noexist_set2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZdiffInput::new(vec![
                                RedisKey::String("zdiff_noexist_set1".into()),
                                RedisKey::String("zdiff_noexist_set2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZdiffOutput::decode(&result, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
