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

const API_INFO: ApiInfo<RedisApi, ScanInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Scan, "Iterates over the key names in the database", ReqType::Read, true);

/// See official Redis documentation for `SCAN`
/// https://redis.io/docs/latest/commands/scan/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScanInput {
    pub(crate) cursor: RedisJsonValue,
    pub(crate) r#match: Option<RedisJsonValue>,
    pub(crate) count: Option<RedisJsonValue>,
    pub(crate) r#type: Option<RedisJsonValue>,
}

impl ScanInput {
    pub fn new(cursor: impl Into<RedisJsonValue>) -> Self {
        Self {
            cursor: cursor.into(),
            r#match: None,
            count: None,
            r#type: None,
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

    pub fn with_type(mut self, key_type: impl Into<RedisJsonValue>) -> Self {
        self.r#type = Some(key_type.into());
        self
    }
}

impl Serialize for ScanInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, cursor
        if self.r#match.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.r#type.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ScanInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("cursor", &self.cursor)?;

        if let Some(m) = &self.r#match {
            state.serialize_field("match", m)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(t) = &self.r#type {
            state.serialize_field("keyType", t)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ScanInput,
    API_INFO,
    { cursor, r#match, count, r#type }
);

impl RedisCommandInput for ScanInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::default()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.cursor);

        if let Some(pattern) = &self.r#match {
            command.arg("MATCH").arg(pattern);
        }

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        if let Some(r#type) = &self.r#type {
            command.arg("TYPE").arg(r#type);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SCAN requires at least 1 argument (cursor)"));
        }

        let mut r#match = None;
        let mut count = None;
        let mut r#type = None;
        let mut i = 1;

        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("MATCH requires a pattern"));
                        }
                        r#match = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("COUNT requires a value"));
                        }
                        count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TYPE" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("TYPE requires a value"));
                        }
                        r#type = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        let _ctx = ctx_with_trace!().with_feature("redis");

                        log_warn!(_ctx, "Unknown SCAN option: {}", audience = LogAudience::Internal, details = format!("{}", s));

                        i += 1;
                    }
                },
                _ => i += 1,
            }
        }

        Ok(Self { cursor: args[0].clone(), r#match, count, r#type })
    }
}

/// Output for Redis SCAN command
///
/// Returns a cursor for the next iteration and a list of keys found.
/// When cursor returns 0, the iteration is complete.
///
/// See official Redis documentation for `SCAN`
/// https://redis.io/docs/latest/commands/scan/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScanOutput {
    /// Cursor for the next SCAN call. 0 means iteration complete.
    cursor: u64,
    /// Keys found in this iteration
    keys: Vec<RedisKey>,
}

impl ScanOutput {
    pub fn new(cursor: u64, keys: Vec<RedisKey>) -> Self {
        Self { cursor, keys }
    }

    /// Get the cursor for the next iteration
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Get the keys found in this iteration
    pub fn keys(&self) -> &[RedisKey] {
        &self.keys
    }

    /// Check if the scan iteration is complete
    pub fn is_complete(&self) -> bool {
        self.cursor == 0
    }

    /// Check if any keys were returned
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let (cursor, keys) = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let [cursor_frame, keys_frame] = <[_; 2]>::try_from(arr).map_err(|_| EpError::parse("SCAN must return [cursor, keys]"))?;

                let cursor = Self::decode_cursor_resp2(cursor_frame)?;

                let Resp2Frame::Array(key_frames) = keys_frame else {
                    return Err(EpError::parse("expected keys array at index 1"));
                };
                let keys = key_frames.into_iter().map(Self::decode_key_resp2).collect::<Result<Vec<_>, _>>()?;

                (cursor, keys)
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let [cursor_frame, keys_frame] = <[_; 2]>::try_from(data).map_err(|_| EpError::parse("SCAN must return [cursor, keys]"))?;

                let cursor = Self::decode_cursor_resp3(cursor_frame)?;

                let Resp3Frame::Array { data: key_frames, .. } = keys_frame else {
                    return Err(EpError::parse("expected keys array at index 1"));
                };
                let keys = key_frames.into_iter().map(Self::decode_key_resp3).collect::<Result<Vec<_>, _>>()?;

                (cursor, keys)
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for SCAN response")),
        };

        Ok(Self { cursor, keys })
    }

    fn decode_cursor_resp2(frame: Resp2Frame) -> Result<u64, EpError> {
        match frame {
            Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                String::from_utf8_lossy(&b).parse::<u64>().map_err(|_| EpError::parse("cursor must be numeric"))
            }
            Resp2Frame::Integer(i) => Ok(i as u64),
            _ => Err(EpError::parse("expected cursor at index 0")),
        }
    }

    fn decode_cursor_resp3(frame: Resp3Frame) -> Result<u64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(data as u64),
            Resp3Frame::BigNumber { data, .. } | Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8_lossy(&data).parse::<u64>().map_err(|_| EpError::parse("cursor must be numeric"))
            }
            _ => Err(EpError::parse("expected cursor at index 0")),
        }
    }

    fn decode_key_resp2(frame: Resp2Frame) -> Result<RedisKey, EpError> {
        let (Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b)) = frame else {
            return Err(EpError::parse("expected string in keys"));
        };
        Ok(match String::from_utf8(b) {
            Ok(s) => RedisKey::String(s),
            Err(e) => RedisKey::Bytes(e.into_bytes()),
        })
    }

    fn decode_key_resp3(frame: Resp3Frame) -> Result<RedisKey, EpError> {
        let (Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. }) = frame else {
            return Err(EpError::parse("expected string in keys"));
        };
        Ok(match String::from_utf8(data) {
            Ok(s) => RedisKey::String(s),
            Err(e) => RedisKey::Bytes(e.into_bytes()),
        })
    }
}

impl Serialize for ScanOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScanOutput", 2)?;
        state.serialize_field("cursor", &self.cursor)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_cursor_only() {
            let input = ScanInput::new(0);
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n");
        }

        #[test]
        fn test_encode_command_with_match() {
            let input = ScanInput::new(0).with_match("user:*");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SCAN"));
            assert!(cmd_str.contains("MATCH"));
            assert!(cmd_str.contains("user:*"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = ScanInput::new(0).with_count(100);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("100"));
        }

        #[test]
        fn test_encode_command_with_type() {
            let input = ScanInput::new(0).with_type("string");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TYPE"));
            assert!(cmd_str.contains("string"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = ScanInput::new(42).with_match("key:*").with_count(50).with_type("hash");
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SCAN"));
            assert!(cmd_str.contains("42"));
            assert!(cmd_str.contains("MATCH"));
            assert!(cmd_str.contains("key:*"));
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("50"));
            assert!(cmd_str.contains("TYPE"));
            assert!(cmd_str.contains("hash"));
        }

        #[test]
        fn test_decode_input_cursor_only() {
            let args = vec![RedisJsonValue::Integer(0)];
            let input = ScanInput::decode(args).unwrap();
            assert!(input.r#match.is_none());
            assert!(input.count.is_none());
            assert!(input.r#type.is_none());
        }

        #[test]
        fn test_decode_input_with_options_uppercase() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("MATCH".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = ScanInput::decode(args).unwrap();
            assert!(input.r#match.is_some());
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_with_options_lowercase() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("match".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("count".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = ScanInput::decode(args).unwrap();
            assert!(input.r#match.is_some());
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ScanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_input_match_without_value_fails() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::String("MATCH".into())];
            let err = ScanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("MATCH requires a pattern"));
        }

        #[test]
        fn test_decode_input_count_without_value_fails() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::String("COUNT".into())];
            let err = ScanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("COUNT requires a value"));
        }

        #[test]
        fn test_decode_input_type_without_value_fails() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::String("TYPE".into())];
            let err = ScanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("TYPE requires a value"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ScanInput::new(0);
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_scan_output_decode_resp2() {
            let resp2_bytes = b"*2\r\n$1\r\n0\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";

            let output = ScanOutput::decode(resp2_bytes).unwrap();
            assert_eq!(output.cursor(), 0);
            assert!(output.is_complete());
            assert_eq!(output.keys().len(), 3);
        }

        #[test]
        fn test_scan_output_decode_resp2_nonzero_cursor() {
            let resp2_bytes = b"*2\r\n$2\r\n42\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

            let output = ScanOutput::decode(resp2_bytes).unwrap();
            assert_eq!(output.cursor(), 42);
            assert!(!output.is_complete());
            assert_eq!(output.keys().len(), 2);
        }

        #[test]
        fn test_scan_output_decode_resp3() {
            let resp3_bytes = b"*2\r\n:0\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";

            let output = ScanOutput::decode(resp3_bytes).unwrap();
            assert_eq!(output.cursor(), 0);
            assert!(output.is_complete());
            assert_eq!(output.keys().len(), 3);
        }

        #[test]
        fn test_scan_output_empty_keys() {
            let resp2_bytes = b"*2\r\n$1\r\n0\r\n*0\r\n";

            let output = ScanOutput::decode(resp2_bytes).unwrap();
            assert_eq!(output.cursor(), 0);
            assert!(output.is_empty());
            assert!(output.is_complete());
        }

        #[test]
        fn test_scan_output_invalid_format() {
            let invalid_bytes = b"+OK\r\n";
            assert!(ScanOutput::decode(invalid_bytes).is_err());
        }

        #[test]
        fn test_scan_output_wrong_array_length() {
            let invalid_bytes = b"*1\r\n$1\r\n0\r\n";
            assert!(ScanOutput::decode(invalid_bytes).is_err());
        }

        #[test]
        fn test_scan_output_invalid_cursor() {
            let invalid_bytes = b"*2\r\n$3\r\nabc\r\n*0\r\n";
            assert!(ScanOutput::decode(invalid_bytes).is_err());
        }

        #[test]
        fn test_decode_error_resp2() {
            let err = ScanOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_scan_output_new() {
            let output = ScanOutput::new(10, vec![RedisKey::String("test".into())]);
            assert_eq!(output.cursor(), 10);
            assert_eq!(output.keys().len(), 1);
        }

        #[test]
        fn test_scan_output_preserves_binary_keys() {
            // Key containing non-UTF-8 bytes (0x00, 0xFF, 0x10)
            let binary_key: Vec<u8> = b"key:\x00\xff\x10:data".to_vec();

            let mut resp2_bytes: Vec<u8> = Vec::new();
            resp2_bytes.extend_from_slice(b"*2\r\n$1\r\n0\r\n*1\r\n");
            resp2_bytes.extend_from_slice(format!("${}\r\n", binary_key.len()).as_bytes());
            resp2_bytes.extend_from_slice(&binary_key);
            resp2_bytes.extend_from_slice(b"\r\n");

            let output = ScanOutput::decode(&resp2_bytes).expect("decode should succeed");
            assert_eq!(output.keys().len(), 1);
            assert_eq!(output.keys()[0].as_bytes(), &binary_key[..]);
        }

        #[test]
        fn test_scan_output_utf8_keys_remain_strings() {
            let resp2_bytes = b"*2\r\n$1\r\n0\r\n*1\r\n$10\r\nuser:12345\r\n";

            let output = ScanOutput::decode(resp2_bytes).expect("decode should succeed");
            assert_eq!(output.keys().len(), 1);
            assert!(output.keys()[0].is_string());
            assert_eq!(output.keys()[0].as_str(), Some("user:12345"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_empty_database() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB to ensure empty
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ScanInput::new(0).command()).await.expect("raw failed");

                    let output = ScanOutput::decode(&result).expect("decode failed");
                    assert!(output.is_complete(), "empty db should complete in one scan");
                    assert!(output.is_empty(), "empty db should return no keys");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_finds_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    // Create some keys
                    ctx.write(SetInput {
                        key: RedisKey::String("scan_key1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("scan_key2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("scan_key3".into()),
                        value: RedisJsonValue::String("v3".into()),
                        ..Default::default()
                    })
                    .await;

                    // Scan until complete
                    let mut all_keys = Vec::new();
                    let mut cursor = 0u64;

                    loop {
                        let result = ctx.raw(&ScanInput::new(cursor).command()).await.expect("raw failed");
                        let output = ScanOutput::decode(&result).expect("decode failed");

                        for key in output.keys() {
                            all_keys.push(key.clone());
                        }

                        cursor = output.cursor();
                        if output.is_complete() {
                            break;
                        }
                    }

                    assert_eq!(all_keys.len(), 3, "should find all 3 keys");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_with_match_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    // Create keys with different prefixes
                    ctx.write(SetInput {
                        key: RedisKey::String("user:1".into()),
                        value: RedisJsonValue::String("u1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("user:2".into()),
                        value: RedisJsonValue::String("u2".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("order:1".into()),
                        value: RedisJsonValue::String("o1".into()),
                        ..Default::default()
                    })
                    .await;

                    // Scan only user:* keys
                    let mut user_keys = Vec::new();
                    let mut cursor = 0u64;

                    loop {
                        let result = ctx.raw(&ScanInput::new(cursor).with_match("user:*").command()).await.expect("raw failed");
                        let output = ScanOutput::decode(&result).expect("decode failed");

                        for key in output.keys() {
                            user_keys.push(key.clone());
                        }

                        cursor = output.cursor();
                        if output.is_complete() {
                            break;
                        }
                    }

                    assert_eq!(user_keys.len(), 2, "should find only user:* keys");
                    for key in &user_keys {
                        match key {
                            RedisKey::String(s) => assert!(s.starts_with("user:")),
                            _ => panic!("expected string key"),
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    // Create many keys
                    for i in 0..20 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("count_key:{}", i)),
                            value: RedisJsonValue::String(format!("v{}", i)),
                            ..Default::default()
                        })
                        .await;
                    }

                    // Scan with count hint
                    let result = ctx.raw(&ScanInput::new(0).with_count(5).command()).await.expect("raw failed");

                    let output = ScanOutput::decode(&result).expect("decode failed");
                    // COUNT is a hint, not a guarantee, so we just verify it works
                    assert!(output.keys().len() <= 20);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_with_type_filter() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    // Create string keys
                    ctx.write(SetInput {
                        key: RedisKey::String("string_key".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // Create a hash key
                    ctx.raw(b"*4\r\n$4\r\nHSET\r\n$8\r\nhash_key\r\n$5\r\nfield\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    // Scan only string type
                    let mut string_keys = Vec::new();
                    let mut cursor = 0u64;

                    loop {
                        let result = ctx.raw(&ScanInput::new(cursor).with_type("string").command()).await.expect("raw failed");
                        let output = ScanOutput::decode(&result).expect("decode failed");

                        for key in output.keys() {
                            string_keys.push(key.clone());
                        }

                        cursor = output.cursor();
                        if output.is_complete() {
                            break;
                        }
                    }

                    assert_eq!(string_keys.len(), 1);
                    assert_eq!(string_keys[0], RedisKey::String("string_key".into()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_full_iteration() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    let key_count = 100;
                    for i in 0..key_count {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("iter_key:{}", i)),
                            value: RedisJsonValue::String(format!("v{}", i)),
                            ..Default::default()
                        })
                        .await;
                    }

                    let mut all_keys = std::collections::HashSet::new();
                    let mut cursor = 0u64;
                    let mut iterations = 0;

                    loop {
                        let result = ctx.raw(&ScanInput::new(cursor).command()).await.expect("raw failed");
                        let output = ScanOutput::decode(&result).expect("decode failed");

                        for key in output.keys() {
                            all_keys.insert(key.clone());
                        }

                        cursor = output.cursor();
                        iterations += 1;

                        if output.is_complete() {
                            break;
                        }

                        // Safety limit
                        if iterations > 1000 {
                            panic!("too many iterations");
                        }
                    }

                    assert_eq!(all_keys.len(), key_count, "should find all {} keys", key_count);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_key".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ScanInput::new(0).command());
                    pipeline.extend_from_slice(&ScanInput::new(0).with_match("pipe_*").command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = ScanOutput::decode(responses[0]).expect("decode scan1");
                    let out2 = ScanOutput::decode(responses[1]).expect("decode scan2");

                    // Both should complete and find the key
                    assert!(out1.is_complete());
                    assert!(out2.is_complete());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&ScanInput::new(0).command()).await.expect("raw failed");

            // RESP2 format: *2\r\n$<cursor_len>\r\n<cursor>\r\n*<keys_count>\r\n...
            assert!(result.starts_with(b"*2\r\n"), "RESP2 array format");

            let output = ScanOutput::decode(&result).expect("decode failed");
            assert!(output.is_complete());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scan_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&ScanInput::new(0).command()).await.expect("raw failed");

            // RESP3 may use *2 or other array format
            let output = ScanOutput::decode(&result).expect("decode failed");
            assert!(output.is_complete());
            ctx.stop().await;
        }
    }
}
