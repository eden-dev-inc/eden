use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ZscanInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zscan,
    "Iterates over members and scores of a sorted set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZSCAN`
/// https://redis.io/docs/latest/commands/zscan/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZscanInput {
    key: RedisKey,
    cursor: RedisJsonValue,
    r#match: Option<RedisJsonValue>,
    count: Option<RedisJsonValue>,
}

impl ZscanInput {
    pub fn new(key: impl Into<RedisKey>, cursor: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            cursor: cursor.into(),
            r#match: None,
            count: None,
        }
    }

    pub fn with_match(mut self, pattern: impl Into<RedisJsonValue>) -> Self {
        self.r#match = Some(pattern.into());
        self
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for ZscanInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, cursor
        if self.r#match.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZscanInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("cursor", &self.cursor)?;

        if let Some(r#match) = &self.r#match {
            state.serialize_field("match", r#match)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZscanInput,
    API_INFO,
    {key, cursor, r#match, count}
);

impl RedisCommandInput for ZscanInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.cursor);

        // BUG FIX: Must use MATCH/COUNT keywords before values
        if let Some(pattern) = &self.r#match {
            command.arg("MATCH").arg(pattern);
        }

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
        if args.len() < 2 {
            return Err(EpError::request(format!("ZSCAN requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let cursor = args[1].clone();
        let mut r#match = None;
        let mut count = None;

        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "MATCH" if i + 1 < args.len() => {
                        r#match = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" if i + 1 < args.len() => {
                        count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        let _ctx = ctx_with_trace!().with_feature("redis");
                        log_warn!(_ctx, "Unknown ZSCAN option: {}", audience = LogAudience::Internal, details = format!("{}", cmd));
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, cursor, r#match, count })
    }
}

/// Entry in ZSCAN result containing member and score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZscanEntry {
    pub member: String,
    pub score: f64,
}

impl ZscanEntry {
    pub fn new(member: String, score: f64) -> Self {
        Self { member, score }
    }
}

/// Output for Redis ZSCAN command
///
/// Returns a cursor for the next iteration and a list of member-score pairs.
/// When cursor returns 0, the iteration is complete.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZscanOutput {
    cursor: u64,
    entries: Vec<ZscanEntry>,
}

impl ZscanOutput {
    pub fn new(cursor: u64, entries: Vec<ZscanEntry>) -> Self {
        Self { cursor, entries }
    }

    /// Get the cursor for the next iteration
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Get the member-score entries found in this iteration
    pub fn entries(&self) -> &[ZscanEntry] {
        &self.entries
    }

    /// Check if the scan iteration is complete
    pub fn is_complete(&self) -> bool {
        self.cursor == 0
    }

    /// Check if any entries were returned
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get count of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let (cursor, entries) = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() != 2 {
                    return Err(EpError::parse("ZSCAN must return [cursor, elements]"));
                }
                let cursor = match &arr[0] {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8(b.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<u64>()
                        .map_err(|_| EpError::parse("cursor must be numeric"))?,
                    Resp2Frame::Integer(i) => *i as u64,
                    _ => return Err(EpError::parse("expected cursor")),
                };
                let entries = match &arr[1] {
                    Resp2Frame::Array(elems) => Self::parse_resp2_entries(elems)?,
                    _ => return Err(EpError::parse("expected array of elements")),
                };
                (cursor, entries)
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() != 2 {
                    return Err(EpError::parse("ZSCAN must return [cursor, elements]"));
                }
                let cursor = match &data[0] {
                    Resp3Frame::Number { data, .. } => *data as u64,
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<u64>()
                        .map_err(|_| EpError::parse("cursor must be numeric"))?,
                    _ => return Err(EpError::parse("expected cursor")),
                };
                let entries = match &data[1] {
                    Resp3Frame::Array { data: elems, .. } => Self::parse_resp3_entries(elems)?,
                    _ => return Err(EpError::parse("expected array of elements")),
                };
                (cursor, entries)
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { cursor, entries })
    }

    fn parse_resp2_entries(elems: &[Resp2Frame]) -> Result<Vec<ZscanEntry>, EpError> {
        let mut entries = Vec::new();
        let mut i = 0;

        while i + 1 < elems.len() {
            let member = match &elems[i] {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                _ => return Err(EpError::parse("expected string member")),
            };

            let score = match &elems[i + 1] {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8(b.to_vec())
                    .map_err(EpError::parse)?
                    .parse::<f64>()
                    .map_err(|_| EpError::parse("score must be numeric"))?,
                Resp2Frame::Integer(n) => *n as f64,
                _ => return Err(EpError::parse("expected numeric score")),
            };

            entries.push(ZscanEntry { member, score });
            i += 2;
        }

        Ok(entries)
    }

    fn parse_resp3_entries(elems: &[Resp3Frame]) -> Result<Vec<ZscanEntry>, EpError> {
        let mut entries = Vec::new();
        let mut i = 0;

        while i + 1 < elems.len() {
            let member = match &elems[i] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    String::from_utf8(data.to_vec()).map_err(EpError::parse)?
                }
                _ => return Err(EpError::parse("expected string member")),
            };

            let score = match &elems[i + 1] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.to_vec())
                    .map_err(EpError::parse)?
                    .parse::<f64>()
                    .map_err(|_| EpError::parse("score must be numeric"))?,
                Resp3Frame::Number { data, .. } => *data as f64,
                Resp3Frame::Double { data, .. } => *data,
                _ => return Err(EpError::parse("expected numeric score")),
            };

            entries.push(ZscanEntry { member, score });
            i += 2;
        }

        Ok(entries)
    }
}

impl Serialize for ZscanOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZscanOutput", 2)?;
        state.serialize_field("cursor", &self.cursor)?;
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
            let input = ZscanInput::new(RedisKey::String("myzset".into()), 0);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZSCAN"));
            assert!(cmd_str.contains("myzset"));
        }

        #[test]
        fn test_encode_command_with_match() {
            let input = ZscanInput::new(RedisKey::String("myzset".into()), 0).with_match("member*");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("MATCH"));
            assert!(cmd_str.contains("member*"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = ZscanInput::new(RedisKey::String("myzset".into()), 0).with_count(100);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_encode_command_with_all_options() {
            let input = ZscanInput::new(RedisKey::String("myzset".into()), 0).with_match("a*").with_count(10);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("MATCH") && cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_decode_empty() {
            // *2\r\n$1\r\n0\r\n*0\r\n
            let output = ZscanOutput::decode(b"*2\r\n$1\r\n0\r\n*0\r\n").unwrap();
            assert!(output.is_complete());
            assert!(output.is_empty());
            assert_eq!(output.cursor(), 0);
        }

        #[test]
        fn test_decode_with_entries() {
            // cursor=5, entries=[("member1", "1.5"), ("member2", "2.5")]
            let output =
                ZscanOutput::decode(b"*2\r\n$1\r\n5\r\n*4\r\n$7\r\nmember1\r\n$3\r\n1.5\r\n$7\r\nmember2\r\n$3\r\n2.5\r\n").unwrap();
            assert!(!output.is_complete());
            assert_eq!(output.cursor(), 5);
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].member, "member1");
            assert_eq!(output.entries()[0].score, 1.5);
            assert_eq!(output.entries()[1].member, "member2");
            assert_eq!(output.entries()[1].score, 2.5);
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let input = ZscanInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("MATCH".into()),
                RedisJsonValue::String("a*".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = ZscanInput::decode(args).unwrap();
            assert!(input.r#match.is_some());
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let err = ZscanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZscanInput::new(RedisKey::String("myzset".into()), 0);
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_error_response() {
            let err = ZscanOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_zscan_entry_new() {
            let entry = ZscanEntry::new("test".to_string(), 2.51);
            assert_eq!(entry.member, "test");
            assert_eq!(entry.score, 2.51);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzscan_empty\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZscanInput::new(RedisKey::String("zscan_empty".into()), 0).command()).await.expect("raw failed");

                    let output = ZscanOutput::decode(&result).expect("decode failed");
                    assert!(output.is_complete());
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_with_data() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzscan_data\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zscan_data 1 member1 2 member2 3 member3
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$10\r\nzscan_data\r\n$1\r\n1\r\n$7\r\nmember1\r\n$1\r\n2\r\n$7\r\nmember2\r\n$1\r\n3\r\n$7\r\nmember3\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZscanInput::new(RedisKey::String("zscan_data".into()), 0).command())
                        .await
                        .expect("raw failed");

                    let output = ZscanOutput::decode(&result).expect("decode failed");
                    assert!(output.is_complete());
                    assert_eq!(output.len(), 3);

                    // Verify members and scores
                    let members: Vec<&str> = output.entries().iter().map(|e| e.member.as_str()).collect();
                    assert!(members.contains(&"member1"));
                    assert!(members.contains(&"member2"));
                    assert!(members.contains(&"member3"));
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_with_match() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzscan_match\r\n")
                        .await
                        .expect("raw failed");

                    // Add members with different prefixes
                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$11\r\nzscan_match\r\n$1\r\n1\r\n$6\r\nuser:1\r\n$1\r\n2\r\n$6\r\nuser:2\r\n$1\r\n3\r\n$7\r\nadmin:1\r\n$1\r\n4\r\n$7\r\nadmin:2\r\n")
                        .await
                        .expect("raw failed");

                    let mut user_entries = Vec::new();
                    let mut cursor = 0u64;

                    loop {
                        let result = ctx
                            .raw(&ZscanInput::new(RedisKey::String("zscan_match".into()), cursor)
                                .with_match("user:*")
                                .command())
                            .await
                            .expect("raw failed");

                        let output = ZscanOutput::decode(&result).expect("decode failed");

                        for entry in output.entries() {
                            user_entries.push(entry.clone());
                        }

                        cursor = output.cursor();
                        if output.is_complete() {
                            break;
                        }
                    }

                    assert_eq!(user_entries.len(), 2);
                    for entry in &user_entries {
                        assert!(entry.member.starts_with("user:"));
                    }
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_full_iteration() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzscan_iter\r\n").await.expect("raw failed");

                    // Add many members
                    for i in 0..50 {
                        let cmd = format!(
                            "*4\r\n$4\r\nZADD\r\n$10\r\nzscan_iter\r\n${}\r\n{}\r\n${}\r\nmember{}\r\n",
                            i.to_string().len(),
                            i,
                            format!("member{}", i).len(),
                            i
                        );
                        ctx.raw(cmd.as_bytes()).await.expect("raw failed");
                    }

                    let mut all_entries = std::collections::HashSet::new();
                    let mut cursor = 0u64;
                    let mut iterations = 0;

                    loop {
                        let result =
                            ctx.raw(&ZscanInput::new(RedisKey::String("zscan_iter".into()), cursor).command()).await.expect("raw failed");

                        let output = ZscanOutput::decode(&result).expect("decode failed");

                        for entry in output.entries() {
                            all_entries.insert(entry.member.clone());
                        }

                        cursor = output.cursor();
                        iterations += 1;

                        if output.is_complete() {
                            break;
                        }

                        if iterations > 100 {
                            panic!("too many iterations");
                        }
                    }

                    assert_eq!(all_entries.len(), 50);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nzscan_r2\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZscanInput::new(RedisKey::String("zscan_r2".into()), 0).command()).await.expect("raw failed");

            assert!(result.starts_with(b"*2\r\n"), "RESP2 array format");
            let output = ZscanOutput::decode(&result).expect("decode failed");
            assert!(output.is_complete());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nzscan_r3\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZscanInput::new(RedisKey::String("zscan_r3".into()), 0).command()).await.expect("raw failed");

            let output = ZscanOutput::decode(&result).expect("decode failed");
            assert!(output.is_complete());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zscan_wrongtype_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key (not a sorted set)
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$14\r\nzscan_wrongkey\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZscanInput::new(RedisKey::String("zscan_wrongkey".into()), 0).command()).await.expect("raw failed");

                    let err = ZscanOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
