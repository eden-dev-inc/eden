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

// Use shared types if available, otherwise define locally
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, ToSchema, JsonSchema,
)]
pub enum Aggregate {
    #[default]
    SUM,
    MIN,
    MAX,
}

impl Aggregate {
    pub fn as_str(&self) -> &'static str {
        match self {
            Aggregate::SUM => "SUM",
            Aggregate::MIN => "MIN",
            Aggregate::MAX => "MAX",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SUM" => Some(Aggregate::SUM),
            "MIN" => Some(Aggregate::MIN),
            "MAX" => Some(Aggregate::MAX),
            _ => None,
        }
    }
}

const API_INFO: ApiInfo<RedisApi, ZinterInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zinter,
    "Returns the intersection of multiple sorted sets",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZINTER`
/// https://redis.io/docs/latest/commands/zinter/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZinterInput {
    keys: Vec<RedisKey>,
    weights: Option<Vec<RedisJsonValue>>,
    aggregate: Option<Aggregate>,
    withscores: Option<bool>,
}

impl ZinterInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
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

impl Serialize for ZinterInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.weights.is_some() {
            fields += 1;
        }
        if self.aggregate.is_some() {
            fields += 1;
        }
        if self.withscores.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZinterInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
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

impl_redis_operation!(ZinterInput, API_INFO, { keys, weights, aggregate, withscores });

impl RedisCommandInput for ZinterInput {
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
        if args.len() < 2 {
            return Err(EpError::request(format!("ZINTER requires at least 2 arguments, given {}", args.len())));
        }

        let numkeys = match &args[0] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 1 + numkeys {
            return Err(EpError::request("Insufficient keys for ZINTER"));
        }

        let mut keys = Vec::new();
        for key in args[1..1 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        let mut weights = None;
        let mut aggregate = None;
        let mut withscores = None;

        let mut i = 1 + numkeys;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "WEIGHTS" if i + numkeys < args.len() => {
                        weights = Some(args[i + 1..i + 1 + numkeys].to_vec());
                        i += 1 + numkeys;
                    }
                    "AGGREGATE" if i + 1 < args.len() => {
                        if let RedisJsonValue::String(ref s) = args[i + 1] {
                            aggregate = Aggregate::parse(s);
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

        Ok(Self { keys, weights, aggregate, withscores })
    }
}

/// Entry in ZINTER result
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZinterEntry {
    pub member: String,
    pub score: Option<f64>,
}

/// Output for Redis ZINTER command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZinterOutput {
    entries: Vec<ZinterEntry>,
}

impl ZinterOutput {
    pub fn new(entries: Vec<ZinterEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ZinterEntry] {
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
                    let mut iter = arr.into_iter();
                    while let Some(member_frame) = iter.next() {
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("ZINTER missing score"))?;
                        let member = match member_frame {
                            Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected bulk string")),
                        };
                        let score = match score_frame {
                            Resp2Frame::BulkString(data) => Some(
                                String::from_utf8(data)
                                    .map_err(EpError::parse)?
                                    .parse::<f64>()
                                    .map_err(|_| EpError::parse("Invalid score"))?,
                            ),
                            _ => return Err(EpError::parse("Expected bulk string for score")),
                        };
                        entries.push(ZinterEntry { member, score });
                    }
                } else {
                    for frame in arr {
                        let member = match frame {
                            Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected bulk string")),
                        };
                        entries.push(ZinterEntry { member, score: None });
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
                                        return Err(EpError::parse("ZINTER entry must have 2 elements"));
                                    }

                                    let member = match &entry_data[0] {
                                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                                        _ => {
                                            return Err(EpError::parse("Expected blob string for member"));
                                        }
                                    };

                                    let score = match &entry_data[1] {
                                        Resp3Frame::Double { data, .. } => Some(*data),
                                        Resp3Frame::BlobString { data, .. } => Some(
                                            String::from_utf8(data.clone())
                                                .map_err(EpError::parse)?
                                                .parse::<f64>()
                                                .map_err(|_| EpError::parse("Invalid score"))?,
                                        ),
                                        Resp3Frame::Number { data, .. } => Some(*data as f64),
                                        _ => {
                                            return Err(EpError::parse("Expected double for score"));
                                        }
                                    };

                                    entries.push(ZinterEntry { member, score });
                                }
                                _ => {
                                    return Err(EpError::parse("Expected nested array for ZINTER entry"));
                                }
                            }
                        }
                    } else {
                        // Flat array (RESP2 compatibility): [member1, score1, member2, score2, ...]
                        let mut iter = data.into_iter();
                        while let Some(member_frame) = iter.next() {
                            let score_frame = iter.next().ok_or_else(|| EpError::parse("ZINTER missing score"))?;
                            let member = match member_frame {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                                _ => return Err(EpError::parse("Expected blob string")),
                            };
                            let score = match score_frame {
                                Resp3Frame::Double { data, .. } => Some(data),
                                Resp3Frame::BlobString { data, .. } => Some(
                                    String::from_utf8(data)
                                        .map_err(EpError::parse)?
                                        .parse::<f64>()
                                        .map_err(|_| EpError::parse("Invalid score"))?,
                                ),
                                Resp3Frame::Number { data, .. } => Some(data as f64),
                                _ => return Err(EpError::parse("Expected double for score")),
                            };
                            entries.push(ZinterEntry { member, score });
                        }
                    }
                } else {
                    for frame in data {
                        let member = match frame {
                            Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                            _ => return Err(EpError::parse("Expected blob string")),
                        };
                        entries.push(ZinterEntry { member, score: None });
                    }
                }
                Ok(Self { entries })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("ZINTER must return an array")),
        }
    }
}

impl Serialize for ZinterOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZinterOutput", 1)?;
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
            let input = ZinterInput::new(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZINTER"));
            assert!(cmd_str.contains("2"));
        }

        #[test]
        fn test_encode_command_with_weights() {
            let input = ZinterInput::new(vec![RedisKey::String("zset1".into())]).with_weights(vec![RedisJsonValue::Float(2.0)]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WEIGHTS"));
        }

        #[test]
        fn test_encode_command_with_aggregate() {
            let input = ZinterInput::new(vec![RedisKey::String("zset1".into())]).with_aggregate(Aggregate::MAX);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("AGGREGATE"));
            assert!(cmd_str.contains("MAX"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZinterInput::new(vec![RedisKey::String("zset1".into())]).with_scores();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHSCORES"));
        }

        #[test]
        fn test_decode_output_without_scores() {
            let output = ZinterOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n", false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.members(), vec!["a", "b"]);
        }

        #[test]
        fn test_decode_output_with_scores() {
            let output = ZinterOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n", true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].score, Some(1.0));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZinterOutput::decode(b"*0\r\n", false).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZinterOutput::decode(b"-WRONGTYPE Operation\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
            ];
            let input = ZinterInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = ZinterInput::new(vec![RedisKey::String("a".into()), RedisKey::String("b".into())]);
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
        async fn test_zinter_basic() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzinter_set1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzinter_set2\r\n").await.expect("del");

                    ctx.raw(
                        b"*8\r\n$4\r\nZADD\r\n$11\r\nzinter_set1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
                    )
                    .await
                    .expect("zadd");
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$11\r\nzinter_set2\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nd\r\n").await.expect("zadd");

                    let result = ctx
                        .raw(
                            &ZinterInput::new(vec![RedisKey::String("zinter_set1".into()), RedisKey::String("zinter_set2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZinterOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.members(), vec!["b"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zinter_with_scores() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzinter_sc_s1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzinter_sc_s2\r\n").await.expect("del");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzinter_sc_s1\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("zadd");
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzinter_sc_s2\r\n$1\r\n2\r\n$1\r\na\r\n").await.expect("zadd");

                    let result = ctx
                        .raw(
                            &ZinterInput::new(vec![RedisKey::String("zinter_sc_s1".into()), RedisKey::String("zinter_sc_s2".into())])
                                .with_scores()
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZinterOutput::decode(&result, true).expect("decode");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].score, Some(3.0)); // SUM: 1 + 2
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zinter_empty_intersection() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzinter_em_s1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzinter_em_s2\r\n").await.expect("del");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzinter_em_s1\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("zadd");
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzinter_em_s2\r\n$1\r\n1\r\n$1\r\nb\r\n").await.expect("zadd");

                    let result = ctx
                        .raw(
                            &ZinterInput::new(vec![RedisKey::String("zinter_em_s1".into()), RedisKey::String("zinter_em_s2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZinterOutput::decode(&result, false).expect("decode");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
