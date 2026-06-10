use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use redis_protocol::resp3::types::FrameMap;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, LcsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lcs,
    "The LCS command implements the longest common subsequence algorithm. Note that this is different than the longest common string algorithm, since matching characters in the string does not need to be contiguous",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LCS`
/// https://redis.io/docs/latest/commands/lcs/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LcsInput {
    pub(crate) key1: RedisKey,
    pub(crate) key2: RedisKey,
    pub(crate) len: Option<bool>,
    pub(crate) idx: Option<bool>,
    pub(crate) min_match_len: Option<RedisJsonValue>,
    pub(crate) with_match_len: Option<bool>,
}

impl Serialize for LcsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 3; // type, key1, key2

        if self.len.is_some() {
            field_count += 1;
        }
        if self.idx.is_some() {
            field_count += 1;
        }
        if self.min_match_len.is_some() {
            field_count += 1;
        }
        if self.with_match_len.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("LcsInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key1", &self.key1)?;
        state.serialize_field("key2", &self.key2)?;

        if let Some(len) = &self.len {
            state.serialize_field("len", len)?;
        }

        if let Some(idx) = &self.idx {
            state.serialize_field("idx", idx)?;
        }

        if let Some(min_match_len) = &self.min_match_len {
            state.serialize_field("min_match_len", min_match_len)?;
        }

        if let Some(with_match_len) = &self.with_match_len {
            state.serialize_field("with_match_len", with_match_len)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    LcsInput,
    API_INFO,
    {key1, key2, len, idx, min_match_len, with_match_len}
);

impl RedisCommandInput for LcsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key1.clone(), self.key2.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key1).arg(&self.key2);

        if let Some(len) = self.len
            && len
        {
            command.arg("LEN");
        }

        if let Some(idx) = self.idx
            && idx
        {
            command.arg("IDX");
        }

        if let Some(min_match_len) = &self.min_match_len {
            command.arg("MINMATCHLEN").arg(min_match_len);
        }

        if let Some(with_match_len) = self.with_match_len
            && with_match_len
        {
            command.arg("WITHMATCHLEN");
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request("LCS requires at least two keys"));
        }

        let key1 = args[0].clone().try_into()?;
        let key2 = args[1].clone().try_into()?;
        let mut len = None;
        let mut idx = None;
        let mut min_match_len = None;
        let mut with_match_len = None;

        let mut i = 2;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "LEN" => {
                        len = Some(true);
                        i += 1;
                    }
                    "IDX" => {
                        idx = Some(true);
                        i += 1;
                    }
                    "MINMATCHLEN" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("MINMATCHLEN requires a value"));
                        }
                        min_match_len = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "WITHMATCHLEN" => {
                        with_match_len = Some(true);
                        i += 1;
                    }
                    _ => {
                        return Err(EpError::request(format!("Unknown LCS option: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::request("LCS options must be strings"));
                }
            }
        }

        Ok(LcsInput { key1, key2, len, idx, min_match_len, with_match_len })
    }
}

/// Output for Redis LCS command
///
/// Can return either:
/// - The longest common subsequence string (default)
/// - The length of the LCS (with LEN option)
/// - Match information with positions (with IDX option)
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum LcsOutput {
    /// The longest common subsequence string
    String(String),
    /// The length of the LCS (when LEN option is used)
    Length(i64),
    /// Match information (when IDX option is used)
    Matches { matches: Vec<LcsMatch>, len: i64 },
}

/// A match in the LCS result
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, Serialize)]
pub struct LcsMatch {
    /// Start and end positions in key1
    pub key1_range: (i64, i64),
    /// Start and end positions in key2
    pub key2_range: (i64, i64),
    /// Match length (if WITHMATCHLEN was specified)
    pub match_len: Option<i64>,
}

impl LcsOutput {
    /// Get the LCS string if this is a string result
    pub fn as_string(&self) -> Option<&str> {
        match self {
            LcsOutput::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get the length if this is a length result
    pub fn as_length(&self) -> Option<i64> {
        match self {
            LcsOutput::Length(n) => Some(*n),
            _ => None,
        }
    }

    /// Get matches if this is a matches result
    pub fn as_matches(&self) -> Option<(&Vec<LcsMatch>, i64)> {
        match self {
            LcsOutput::Matches { matches, len } => Some((matches, *len)),
            _ => None,
        }
    }

    /// Decode the Redis protocol response into an LcsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::BulkString(bytes) => {
                let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                Ok(LcsOutput::String(s))
            }
            Resp2Frame::SimpleString(s) => {
                let s = String::from_utf8(s).map_err(EpError::parse)?;
                Ok(LcsOutput::String(s))
            }
            Resp2Frame::Integer(n) => Ok(LcsOutput::Length(n)),
            Resp2Frame::Array(items) => Self::decode_matches_resp2(items),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected LCS response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Ok(LcsOutput::String(s))
            }
            Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Ok(LcsOutput::String(s))
            }
            Resp3Frame::Number { data, .. } => Ok(LcsOutput::Length(data)),
            Resp3Frame::Array { data, .. } => Self::decode_matches_resp3(data),
            Resp3Frame::Map { data, .. } => Self::decode_map_resp3(data),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected LCS response: {:?}", other))),
        }
    }

    fn decode_matches_resp2(items: Vec<Resp2Frame>) -> Result<Self, EpError> {
        // IDX output format is: ["matches", [[...match data...]], "len", length]
        let mut matches = Vec::new();
        let mut len = 0i64;

        let mut i = 0;
        while i < items.len() {
            match &items[i] {
                Resp2Frame::BulkString(key) | Resp2Frame::SimpleString(key) => {
                    let key_str = String::from_utf8_lossy(key).to_lowercase();
                    if key_str == "matches" && i + 1 < items.len() {
                        if let Resp2Frame::Array(match_items) = &items[i + 1] {
                            matches = Self::parse_matches_resp2(match_items)?;
                        }
                        i += 2;
                    } else if key_str == "len" && i + 1 < items.len() {
                        if let Resp2Frame::Integer(n) = items[i + 1] {
                            len = n;
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }

        Ok(LcsOutput::Matches { matches, len })
    }

    fn decode_matches_resp3(items: Vec<Resp3Frame>) -> Result<Self, EpError> {
        let mut matches = Vec::new();
        let mut len = 0i64;

        let mut i = 0;
        while i < items.len() {
            match &items[i] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    let key_str = String::from_utf8_lossy(data).to_lowercase();
                    if key_str == "matches" && i + 1 < items.len() {
                        if let Resp3Frame::Array { data, .. } = &items[i + 1] {
                            matches = Self::parse_matches_resp3(data)?;
                        }
                        i += 2;
                    } else if key_str == "len" && i + 1 < items.len() {
                        if let Resp3Frame::Number { data, .. } = items[i + 1] {
                            len = data;
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }

        Ok(LcsOutput::Matches { matches, len })
    }

    fn decode_map_resp3(pairs: FrameMap<Resp3Frame, Resp3Frame>) -> Result<Self, EpError> {
        let mut matches = Vec::new();
        let mut len = 0i64;

        for (key, value) in pairs {
            let key_str = match key {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    String::from_utf8_lossy(&data).to_lowercase()
                }
                _ => continue,
            };

            match key_str.as_str() {
                "matches" => {
                    if let Resp3Frame::Array { data, .. } = value {
                        matches = Self::parse_matches_resp3(&data)?;
                    }
                }
                "len" => {
                    if let Resp3Frame::Number { data, .. } = value {
                        len = data;
                    }
                }
                _ => {}
            }
        }

        Ok(LcsOutput::Matches { matches, len })
    }

    fn parse_matches_resp2(items: &[Resp2Frame]) -> Result<Vec<LcsMatch>, EpError> {
        let mut matches = Vec::new();

        for item in items {
            if let Resp2Frame::Array(match_data) = item
                && match_data.len() >= 2
            {
                let key1_range = Self::extract_range_resp2(&match_data[0])?;
                let key2_range = Self::extract_range_resp2(&match_data[1])?;
                let match_len = if match_data.len() > 2 {
                    if let Resp2Frame::Integer(n) = match_data[2] {
                        Some(n)
                    } else {
                        None
                    }
                } else {
                    None
                };

                matches.push(LcsMatch { key1_range, key2_range, match_len });
            }
        }

        Ok(matches)
    }

    fn parse_matches_resp3(items: &[Resp3Frame]) -> Result<Vec<LcsMatch>, EpError> {
        let mut matches = Vec::new();

        for item in items {
            if let Resp3Frame::Array { data, .. } = item
                && data.len() >= 2
            {
                let key1_range = Self::extract_range_resp3(&data[0])?;
                let key2_range = Self::extract_range_resp3(&data[1])?;
                let match_len = if data.len() > 2 {
                    if let Resp3Frame::Number { data, .. } = data[2] {
                        Some(data)
                    } else {
                        None
                    }
                } else {
                    None
                };

                matches.push(LcsMatch { key1_range, key2_range, match_len });
            }
        }

        Ok(matches)
    }

    fn extract_range_resp2(frame: &Resp2Frame) -> Result<(i64, i64), EpError> {
        if let Resp2Frame::Array(items) = frame
            && items.len() >= 2
        {
            let start = match &items[0] {
                Resp2Frame::Integer(n) => *n,
                _ => return Err(EpError::parse("expected integer in range")),
            };
            let end = match &items[1] {
                Resp2Frame::Integer(n) => *n,
                _ => return Err(EpError::parse("expected integer in range")),
            };
            return Ok((start, end));
        }
        Err(EpError::parse("expected array for range"))
    }

    fn extract_range_resp3(frame: &Resp3Frame) -> Result<(i64, i64), EpError> {
        if let Resp3Frame::Array { data, .. } = frame
            && data.len() >= 2
        {
            let start = match &data[0] {
                Resp3Frame::Number { data, .. } => *data,
                _ => return Err(EpError::parse("expected number in range")),
            };
            let end = match &data[1] {
                Resp3Frame::Number { data, .. } => *data,
                _ => return Err(EpError::parse("expected number in range")),
            };
            return Ok((start, end));
        }
        Err(EpError::parse("expected array for range"))
    }
}

impl Serialize for LcsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            LcsOutput::String(s) => {
                let mut state = serializer.serialize_struct("LcsOutput", 1)?;
                state.serialize_field("value", s)?;
                state.end()
            }
            LcsOutput::Length(n) => {
                let mut state = serializer.serialize_struct("LcsOutput", 1)?;
                state.serialize_field("len", n)?;
                state.end()
            }
            LcsOutput::Matches { matches, len } => {
                let mut state = serializer.serialize_struct("LcsOutput", 2)?;
                state.serialize_field("matches", matches)?;
                state.serialize_field("len", len)?;
                state.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = LcsInput {
                key1: RedisKey::String("key1".into()),
                key2: RedisKey::String("key2".into()),
                len: None,
                idx: None,
                min_match_len: None,
                with_match_len: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nLCS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n");
        }

        #[test]
        fn test_encode_command_with_len() {
            let input = LcsInput {
                key1: RedisKey::String("key1".into()),
                key2: RedisKey::String("key2".into()),
                len: Some(true),
                idx: None,
                min_match_len: None,
                with_match_len: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"LEN"));
        }

        #[test]
        fn test_encode_command_with_idx() {
            let input = LcsInput {
                key1: RedisKey::String("key1".into()),
                key2: RedisKey::String("key2".into()),
                len: None,
                idx: Some(true),
                min_match_len: None,
                with_match_len: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"IDX"));
        }

        #[test]
        fn test_encode_command_with_minmatchlen() {
            let input = LcsInput {
                key1: RedisKey::String("key1".into()),
                key2: RedisKey::String("key2".into()),
                len: None,
                idx: Some(true),
                min_match_len: Some(RedisJsonValue::Integer(3)),
                with_match_len: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(11).any(|w| w == b"MINMATCHLEN"));
        }

        #[test]
        fn test_decode_string_response() {
            let output = LcsOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert_eq!(output.as_string(), Some("hello"));
        }

        #[test]
        fn test_decode_length_response() {
            let output = LcsOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.as_length(), Some(5));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LcsOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let input = LcsInput::decode(args).unwrap();
            assert_eq!(input.key1, RedisKey::String("key1".into()));
            assert_eq!(input.key2, RedisKey::String("key2".into()));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("LEN".into()),
            ];
            let input = LcsInput::decode(args).unwrap();
            assert_eq!(input.len, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key1".into())];
            let err = LcsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least two keys"));
        }

        #[test]
        fn test_keys_returns_both_keys() {
            let input = LcsInput {
                key1: RedisKey::String("key1".into()),
                key2: RedisKey::String("key2".into()),
                len: None,
                idx: None,
                min_match_len: None,
                with_match_len: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("key1".into()));
            assert_eq!(keys[1], RedisKey::String("key2".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lcs_basic() {
            // LCS requires Redis 7.0+
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_key1".into()),
                            value: RedisJsonValue::String("ohmytext".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_key2".into()),
                            value: RedisJsonValue::String("mynewtext".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &LcsInput {
                                key1: RedisKey::String("lcs_key1".into()),
                                key2: RedisKey::String("lcs_key2".into()),
                                len: None,
                                idx: None,
                                min_match_len: None,
                                with_match_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LcsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_string(), Some("mytext"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lcs_with_len() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_len1".into()),
                            value: RedisJsonValue::String("ohmytext".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_len2".into()),
                            value: RedisJsonValue::String("mynewtext".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &LcsInput {
                                key1: RedisKey::String("lcs_len1".into()),
                                key2: RedisKey::String("lcs_len2".into()),
                                len: Some(true),
                                idx: None,
                                min_match_len: None,
                                with_match_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LcsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_length(), Some(6)); // "mytext" = 6 chars
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lcs_nonexistent_keys() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LcsInput {
                                key1: RedisKey::String("lcs_missing1".into()),
                                key2: RedisKey::String("lcs_missing2".into()),
                                len: None,
                                idx: None,
                                min_match_len: None,
                                with_match_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LcsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_string(), Some(""));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lcs_identical_strings() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_same1".into()),
                            value: RedisJsonValue::String("identical".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("lcs_same2".into()),
                            value: RedisJsonValue::String("identical".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &LcsInput {
                                key1: RedisKey::String("lcs_same1".into()),
                                key2: RedisKey::String("lcs_same2".into()),
                                len: Some(true),
                                idx: None,
                                min_match_len: None,
                                with_match_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LcsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_length(), Some(9)); // "identical" = 9 chars
                })
            })
            .await;
        }
    }
}
