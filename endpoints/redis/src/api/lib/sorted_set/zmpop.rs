#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Scores, ScoresBuilder, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ZmpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zmpop,
    "Returns the highest or lowest scoring members from one or more sorted sets after removing them. Deletes the sorted set if the last member was popped",
    ReqType::Write,
    true,
);

/// Direction for ZMPOP command
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema,
)]
pub enum Direction {
    #[default]
    MIN,
    MAX,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::MIN => "MIN",
            Direction::MAX => "MAX",
        }
    }
}

/// See official Redis documentation for `ZMPOP`
/// https://redis.io/docs/latest/commands/zmpop/
///
/// Note: ZMPOP was introduced in Redis 7.0.0.
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZmpopInput {
    numkeys: RedisJsonValue,
    keys: Vec<RedisKey>,
    direction: Direction,
    count: Option<RedisJsonValue>,
}

impl ZmpopInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>, direction: Direction) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        let numkeys = RedisJsonValue::Integer(keys.len() as i64);
        Self { numkeys, keys, direction, count: None }
    }

    pub fn min(keys: Vec<impl Into<RedisKey>>) -> Self {
        Self::new(keys, Direction::MIN)
    }

    pub fn max(keys: Vec<impl Into<RedisKey>>) -> Self {
        Self::new(keys, Direction::MAX)
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for ZmpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.count.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZmpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("direction", &self.direction)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZmpopInput,
    API_INFO,
    {numkeys, keys, direction, count}
);

impl RedisCommandInput for ZmpopInput {
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
        if args.is_empty() {
            return Err(EpError::request("ZMPOP requires at least numkeys argument"));
        }

        let numkeys = args[0].clone();
        let num_keys = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 1 + num_keys + 1 {
            return Err(EpError::request("Insufficient arguments"));
        }

        let mut keys = Vec::new();
        for i in 0..num_keys {
            keys.push(args[1 + i].clone().try_into()?);
        }

        let direction_idx = 1 + num_keys;
        let direction = if let RedisJsonValue::String(s) = &args[direction_idx] {
            match s.to_uppercase().as_str() {
                "MIN" => Direction::MIN,
                "MAX" => Direction::MAX,
                _ => return Err(EpError::parse("direction must be MIN or MAX")),
            }
        } else {
            return Err(EpError::parse("direction must be string"));
        };

        let mut count = None;
        let mut i = direction_idx + 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i]
                && s.to_uppercase() == "COUNT"
                && i + 1 < args.len()
            {
                count = Some(args[i + 1].clone());
                i += 2;
                continue;
            }
            i += 1;
        }

        Ok(Self { numkeys, keys, direction, count })
    }
}

/// Output for Redis ZMPOP command
///
/// Returns the key from which elements were popped and the popped elements,
/// or None if no elements were available.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZmpopOutput {
    key: Option<String>,
    elements: Vec<Scores>,
}

impl ZmpopOutput {
    pub fn new(key: Option<String>, elements: Vec<Scores>) -> Self {
        Self { key, elements }
    }

    pub fn empty() -> Self {
        Self { key: None, elements: Vec::new() }
    }

    /// Get the key from which elements were popped
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Get the popped elements
    pub fn elements(&self) -> &Vec<Scores> {
        &self.elements
    }

    /// Check if any elements were popped
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of popped elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            // Null response (no elements)
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(Self::empty()),
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(Self::empty()),

            // RESP2: Array [key, [[member, score], ...]]
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() != 2 {
                    return Err(EpError::parse("ZMPOP must return [key, elements] array"));
                }

                let mut iter = arr.into_iter();

                // Parse key
                let key = match iter.next().unwrap() {
                    Resp2Frame::BulkString(b) => String::from_utf8(b).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("ZMPOP key must be string")),
                };

                // Parse elements array
                let elements_frame = iter.next().unwrap();
                let elements = match elements_frame {
                    Resp2Frame::Array(elems) => {
                        let mut elements = Vec::new();
                        // RESP2 returns flat array: [member1, score1, member2, score2, ...]
                        let mut elem_iter = elems.into_iter();
                        while let Some(member_frame) = elem_iter.next() {
                            let score_frame = elem_iter.next().ok_or_else(|| EpError::parse("ZMPOP missing score for member"))?;

                            let member: RedisJsonValue = member_frame.try_into()?;
                            let score: RedisJsonValue = score_frame.try_into()?;

                            elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                        }
                        elements
                    }
                    _ => return Err(EpError::parse("ZMPOP elements must be array")),
                };

                Ok(Self { key: Some(key), elements })
            }

            // RESP3: Array [key, [[member, score], ...]]
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() != 2 {
                    return Err(EpError::parse("ZMPOP must return [key, elements] array"));
                }

                let mut iter = data.into_iter();

                // Parse key
                let key = match iter.next().unwrap() {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                    Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("ZMPOP key must be string")),
                };

                // Parse elements array
                let elements_frame = iter.next().unwrap();
                let elements = match elements_frame {
                    Resp3Frame::Array { data, .. } => {
                        let mut elements = Vec::new();
                        for frame in data {
                            match frame {
                                Resp3Frame::Array { data, .. } if data.len() == 2 => {
                                    let mut it = data.into_iter();
                                    let member: RedisJsonValue = it.next().unwrap().try_into()?;
                                    let score: RedisJsonValue = it.next().unwrap().try_into()?;

                                    elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                                }
                                _ => {
                                    return Err(EpError::parse("ZMPOP element must be [member, score] array"));
                                }
                            }
                        }
                        elements
                    }
                    _ => return Err(EpError::parse("ZMPOP elements must be array")),
                };

                Ok(Self { key: Some(key), elements })
            }

            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }

            _ => Err(EpError::parse("unexpected ZMPOP response format")),
        }
    }
}

impl Serialize for ZmpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZmpopOutput", 2)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("elements", &self.elements)?;
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
            let input = ZmpopInput::min(vec![RedisKey::String("zset1".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZMPOP"));
            assert!(cmd_str.contains("1")); // numkeys
            assert!(cmd_str.contains("MIN"));
        }

        #[test]
        fn test_encode_command_max() {
            let input = ZmpopInput::max(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("MAX"));
            assert!(cmd_str.contains("2")); // numkeys
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = ZmpopInput::min(vec![RedisKey::String("zset1".into())]).with_count(3);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("3"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = ZmpopOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
            assert!(output.key().is_none());
        }

        #[test]
        fn test_decode_error() {
            let err = ZmpopOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
                RedisJsonValue::String("MIN".into()),
            ];
            let input = ZmpopInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
            assert_eq!(input.direction, Direction::MIN);
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("MAX".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = ZmpopInput::decode(args).unwrap();
            assert_eq!(input.direction, Direction::MAX);
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::String("zset1".into())];
            let err = ZmpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Insufficient"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = ZmpopInput::min(vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())]);
            assert_eq!(input.keys().len(), 2);
        }

        #[test]
        fn test_direction_as_str() {
            assert_eq!(Direction::MIN.as_str(), "MIN");
            assert_eq!(Direction::MAX.as_str(), "MAX");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmpop_min() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzmpop_minkey\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zmpop_minkey 1 one 2 two 3 three
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$12\r\nzmpop_minkey\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZmpopInput::min(vec![RedisKey::String("zmpop_minkey".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZmpopOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    assert_eq!(output.key(), Some("zmpop_minkey"));
                    assert_eq!(output.len(), 1);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmpop_max_with_count() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzmpop_maxkey\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$12\r\nzmpop_maxkey\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZmpopInput::max(vec![RedisKey::String("zmpop_maxkey".into())])
                                .with_count(2)
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZmpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmpop_multiple_keys() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzmpop_multi1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzmpop_multi2\r\n").await.expect("raw failed");

                    // Only add to second key
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nzmpop_multi2\r\n$1\r\n1\r\n$1\r\na\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZmpopInput::min(vec![RedisKey::String("zmpop_multi1".into()), RedisKey::String("zmpop_multi2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZmpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.key(), Some("zmpop_multi2"));
                    assert_eq!(output.len(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmpop_empty_keys() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzmpop_empty1\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZmpopInput::min(vec![RedisKey::String("zmpop_empty1".into())]).command()).await.expect("raw failed");

                    let output = ZmpopOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                    assert!(output.key().is_none());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zmpop_wrongtype() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$11\r\nzmpop_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZmpopInput::min(vec![RedisKey::String("zmpop_wrong".into())]).command()).await.expect("raw failed");

                    let err = ZmpopOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
