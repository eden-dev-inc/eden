use crate::api::lib::sorted_set::common::Aggregate;
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

const API_INFO: ApiInfo<RedisApi, ZunionInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Zunion, "Returns the union of multiple sorted sets", ReqType::Read, true);

/// See official Redis documentation for `ZUNION`
/// https://redis.io/docs/latest/commands/zunion/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZunionInput {
    numkeys: RedisJsonValue,
    keys: Vec<RedisKey>,
    weights: Option<Vec<RedisJsonValue>>,
    aggregate: Option<Aggregate>,
    withscores: Option<bool>,
}

impl ZunionInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        let numkeys = RedisJsonValue::Integer(keys.len() as i64);
        Self {
            numkeys,
            keys,
            weights: None,
            aggregate: None,
            withscores: None,
        }
    }

    pub fn with_weights(mut self, weights: Vec<impl Into<RedisJsonValue>>) -> Self {
        self.weights = Some(weights.into_iter().map(|w| w.into()).collect());
        self
    }

    pub fn with_aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }

    pub fn with_scores(mut self) -> Self {
        self.withscores = Some(true);
        self
    }
}

impl Serialize for ZunionInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.weights.is_some() {
            fields += 1;
        }
        if self.aggregate.is_some() {
            fields += 1;
        }
        if self.withscores.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZunionInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("keys", &self.keys)?;

        if let Some(weights) = &self.weights {
            state.serialize_field("weights", weights)?;
        }
        if let Some(aggregate) = &self.aggregate {
            state.serialize_field("aggregate", aggregate)?;
        }
        if let Some(withscores) = &self.withscores {
            state.serialize_field("withscores", withscores)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZunionInput,
    API_INFO,
    { numkeys, keys, weights, aggregate, withscores }
);

impl RedisCommandInput for ZunionInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.numkeys);
        for key in &self.keys {
            command.arg(key);
        }

        if let Some(weights) = &self.weights {
            command.arg("WEIGHTS");
            for w in weights {
                command.arg(w);
            }
        }

        if let Some(aggregate) = &self.aggregate {
            command.arg("AGGREGATE").arg(aggregate.as_str());
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
            return Err(EpError::request("ZUNION requires at least numkeys argument"));
        }

        let numkeys = args[0].clone();
        let num_keys = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be a valid integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 1 + num_keys {
            return Err(EpError::request("Insufficient keys"));
        }

        let mut keys = vec![];
        for key in args[1..1 + num_keys].iter() {
            keys.push(key.try_into()?)
        }

        let mut weights = None;
        let mut aggregate = None;
        let mut withscores = None;

        let mut i = 1 + num_keys;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "WEIGHTS" if i + num_keys < args.len() => {
                        weights = Some(args[i + 1..i + 1 + num_keys].to_vec());
                        i += 1 + num_keys;
                    }
                    "AGGREGATE" if i + 1 < args.len() => {
                        if let RedisJsonValue::String(ref s) = args[i + 1] {
                            aggregate = Aggregate::from_str(s);
                            if aggregate.is_none() {
                                return Err(EpError::parse("Invalid aggregate function"));
                            }
                        } else {
                            return Err(EpError::parse("Aggregate must be string"));
                        }
                        i += 2;
                    }
                    "WITHSCORES" => {
                        withscores = Some(true);
                        i += 1;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { numkeys, keys, weights, aggregate, withscores })
    }
}

/// Entry in ZUNION result with member and optional score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZunionEntry {
    pub member: String,
    pub score: Option<f64>,
}

/// Output for Redis ZUNION command
///
/// Returns the union of multiple sorted sets as an array of members,
/// optionally with scores.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZunionOutput {
    entries: Vec<ZunionEntry>,
}

impl ZunionOutput {
    pub fn new(entries: Vec<ZunionEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ZunionEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get just the members (without scores)
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

    fn parse_resp2_array(arr: &[Resp2Frame], with_scores: bool) -> Result<Vec<ZunionEntry>, EpError> {
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
                entries.push(ZunionEntry { member, score: Some(score) });
                i += 2;
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZunionEntry { member, score: None });
            }
        }

        Ok(entries)
    }

    fn parse_resp3_array(arr: &[Resp3Frame], with_scores: bool) -> Result<Vec<ZunionEntry>, EpError> {
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
                                return Err(EpError::parse("ZUNION entry must have 2 elements"));
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

                            entries.push(ZunionEntry { member, score: Some(score) });
                        }
                        _ => return Err(EpError::parse("expected nested array for ZUNION entry")),
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
                    entries.push(ZunionEntry { member, score: Some(score) });
                    i += 2;
                }
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZunionEntry { member, score: None });
            }
        }
        Ok(entries)
    }
}

impl Serialize for ZunionOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZunionOutput", 1)?;
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
            let input = ZunionInput::new(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZUNION"));
            assert!(cmd_str.contains("2")); // numkeys
        }

        #[test]
        fn test_encode_command_with_weights() {
            let input = ZunionInput::new(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]).with_weights(vec![1, 2]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WEIGHTS"));
        }

        #[test]
        fn test_encode_command_with_aggregate() {
            let input = ZunionInput::new(vec![RedisKey::String("zset1".into())]).with_aggregate(Aggregate::MAX);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("AGGREGATE"));
            assert!(cmd_str.contains("MAX"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZunionInput::new(vec![RedisKey::String("zset1".into())]).with_scores();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHSCORES"));
        }

        #[test]
        fn test_decode_output_without_scores() {
            // *2\r\n$1\r\na\r\n$1\r\nb\r\n
            let output = ZunionOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n", false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.members(), vec!["a", "b"]);
            assert!(output.entries()[0].score.is_none());
        }

        #[test]
        fn test_decode_output_with_scores() {
            // *4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n
            let output = ZunionOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n", true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].score, Some(1.0));
            assert_eq!(output.entries()[1].score, Some(2.0));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZunionOutput::decode(b"*0\r\n", false).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZunionOutput::decode(b"-WRONGTYPE Operation\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
            ];
            let input = ZunionInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(1)];
            let err = ZunionInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Insufficient keys"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = ZunionInput::new(vec![RedisKey::String("a".into()), RedisKey::String("b".into())]);
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
        async fn test_zunion_basic() {
            // ZUNION requires Redis 6.2+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzunion_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzunion_set2\r\n").await.expect("raw failed");

                    // ZADD zunion_set1 1 a 2 b
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$11\r\nzunion_set1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zunion_set2 3 b 4 c
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$11\r\nzunion_set2\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionInput::new(vec![RedisKey::String("zunion_set1".into()), RedisKey::String("zunion_set2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 3); // a, b, c
                    let members = output.members();
                    assert!(members.contains(&"a"));
                    assert!(members.contains(&"b"));
                    assert!(members.contains(&"c"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunion_with_scores() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzunion_scores_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzunion_scores_s2\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$16\r\nzunion_scores_s1\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$16\r\nzunion_scores_s2\r\n$1\r\n2\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionInput::new(vec![
                                RedisKey::String("zunion_scores_s1".into()),
                                RedisKey::String("zunion_scores_s2".into()),
                            ])
                            .with_scores()
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionOutput::decode(&result, true).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    // Default aggregate is SUM: 1 + 2 = 3
                    assert_eq!(output.entries()[0].score, Some(3.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunion_aggregate_max() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzunion_max_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzunion_max_s2\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$13\r\nzunion_max_s1\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$13\r\nzunion_max_s2\r\n$1\r\n3\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionInput::new(vec![RedisKey::String("zunion_max_s1".into()), RedisKey::String("zunion_max_s2".into())])
                                .with_aggregate(Aggregate::MAX)
                                .with_scores()
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionOutput::decode(&result, true).expect("decode failed");
                    assert_eq!(output.entries()[0].score, Some(5.0)); // MAX(5, 3) = 5
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zunion_empty_sets() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzunion_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzunion_empty_s2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZunionInput::new(vec![
                                RedisKey::String("zunion_empty_s1".into()),
                                RedisKey::String("zunion_empty_s2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZunionOutput::decode(&result, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
