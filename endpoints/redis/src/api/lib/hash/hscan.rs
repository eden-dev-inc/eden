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

const API_INFO: ApiInfo<RedisApi, HscanInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hscan, "Iterates over fields and values of a hash", ReqType::Read, true);

/// See official Redis documentation for `HSCAN`
/// https://redis.io/docs/latest/commands/hscan/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HscanInput {
    pub(crate) key: RedisKey,
    pub(crate) cursor: RedisJsonValue,
    pub(crate) r#match: Option<RedisJsonValue>,
    pub(crate) count: Option<RedisJsonValue>,
    pub(crate) no_values: Option<bool>,
}

impl HscanInput {
    pub fn new(key: impl Into<RedisKey>, cursor: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            cursor: cursor.into(),
            r#match: None,
            count: None,
            no_values: None,
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

    pub fn with_no_values(mut self) -> Self {
        self.no_values = Some(true);
        self
    }
}

impl Serialize for HscanInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.r#match.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.no_values.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("HscanInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("cursor", &self.cursor)?;
        if let Some(m) = &self.r#match {
            state.serialize_field("match", m)?;
        }
        if let Some(c) = &self.count {
            state.serialize_field("count", c)?;
        }
        if let Some(n) = &self.no_values {
            state.serialize_field("no_values", n)?;
        }
        state.end()
    }
}

impl_redis_operation!(HscanInput, API_INFO, {key, cursor, r#match, count, no_values});

impl RedisCommandInput for HscanInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut cmd = crate::command::cmd(&API_INFO.api.to_string());
        cmd.arg(&self.key).arg(&self.cursor);
        if let Some(p) = &self.r#match {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = &self.count {
            cmd.arg("COUNT").arg(c);
        }
        if self.no_values == Some(true) {
            cmd.arg("NOVALUES");
        }
        cmd.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("HSCAN requires at least 2 arguments, found {}", args.len())));
        }
        let key = args[0].clone().try_into()?;
        let cursor = args[1].clone();
        let (mut r#match, mut count, mut no_values) = (None, None, None);
        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "MATCH" => {
                        r#match = Some(args.get(i + 1).cloned().ok_or_else(|| EpError::request("MATCH requires a value"))?);
                        i += 2;
                    }
                    "COUNT" => {
                        count = Some(args.get(i + 1).cloned().ok_or_else(|| EpError::request("COUNT requires a value"))?);
                        i += 2;
                    }
                    "NOVALUES" => {
                        no_values = Some(true);
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }
        Ok(Self { key, cursor, r#match, count, no_values })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct HscanEntry {
    pub field: String,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HscanOutput {
    cursor: u64,
    entries: Vec<HscanEntry>,
}

impl HscanOutput {
    pub fn new(cursor: u64, entries: Vec<HscanEntry>) -> Self {
        Self { cursor, entries }
    }
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
    pub fn entries(&self) -> &[HscanEntry] {
        &self.entries
    }
    pub fn is_complete(&self) -> bool {
        self.cursor == 0
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let (cursor, entries) = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() != 2 {
                    return Err(EpError::parse("HSCAN must return [cursor, elements]"));
                }
                let cursor = match &arr[0] {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                        String::from_utf8_lossy(b).parse::<u64>().map_err(|_| EpError::parse("cursor must be numeric"))?
                    }
                    Resp2Frame::Integer(i) => *i as u64,
                    _ => return Err(EpError::parse("expected cursor")),
                };
                let entries = match &arr[1] {
                    Resp2Frame::Array(elems) => Self::parse_resp2(elems)?,
                    _ => return Err(EpError::parse("expected array")),
                };
                (cursor, entries)
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() != 2 {
                    return Err(EpError::parse("HSCAN must return [cursor, elements]"));
                }
                let cursor = match &data[0] {
                    Resp3Frame::Number { data, .. } => *data as u64,
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                        String::from_utf8_lossy(data).parse::<u64>().map_err(|_| EpError::parse("cursor must be numeric"))?
                    }
                    _ => return Err(EpError::parse("expected cursor")),
                };
                let entries = match &data[1] {
                    Resp3Frame::Array { data: elems, .. } => Self::parse_resp3(elems)?,
                    _ => return Err(EpError::parse("expected array")),
                };
                (cursor, entries)
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array")),
        };
        Ok(Self { cursor, entries })
    }

    fn parse_resp2(elems: &[Resp2Frame]) -> Result<Vec<HscanEntry>, EpError> {
        let mut entries = Vec::new();
        let mut i = 0;
        while i < elems.len() {
            let field = match &elems[i] {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                _ => return Err(EpError::parse("expected string")),
            };
            let value = if i + 1 < elems.len() {
                match &elems[i + 1] {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                        i += 2;
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => {
                        i += 1;
                        None
                    }
                }
            } else {
                i += 1;
                None
            };
            entries.push(HscanEntry { field, value });
        }
        Ok(entries)
    }

    fn parse_resp3(elems: &[Resp3Frame]) -> Result<Vec<HscanEntry>, EpError> {
        let mut entries = Vec::new();
        let mut i = 0;
        while i < elems.len() {
            let field = match &elems[i] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_string(),
                _ => return Err(EpError::parse("expected string")),
            };
            let value = if i + 1 < elems.len() {
                match &elems[i + 1] {
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                        i += 2;
                        Some(String::from_utf8_lossy(data).to_string())
                    }
                    _ => {
                        i += 1;
                        None
                    }
                }
            } else {
                i += 1;
                None
            };
            entries.push(HscanEntry { field, value });
        }
        Ok(entries)
    }
}

impl Serialize for HscanOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HscanOutput", 2)?;
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
            let input = HscanInput::new(RedisKey::String("myhash".into()), 0);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HSCAN"));
            assert!(cmd_str.contains("myhash"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = HscanInput::new(RedisKey::String("myhash".into()), 0).with_match("f*").with_count(10).with_no_values();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("MATCH"));
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("NOVALUES"));
        }

        #[test]
        fn test_decode_empty() {
            let output = HscanOutput::decode(b"*2\r\n$1\r\n0\r\n*0\r\n").unwrap();
            assert!(output.is_complete() && output.is_empty());
        }

        #[test]
        fn test_decode_with_entries() {
            let output = HscanOutput::decode(b"*2\r\n$1\r\n0\r\n*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].field, "f1");
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(0)];
            let input = HscanInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let err = HscanInput::decode(vec![RedisJsonValue::String("key".into())]).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HscanInput::new(RedisKey::String("myhash".into()), 0);
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hscan_with_data() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhscan_data\r\n").await.expect("raw failed");
            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hscan_data".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                        Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HscanInput::new(RedisKey::String("hscan_data".into()), 0).command()).await.expect("raw failed");
            let output = HscanOutput::decode(&result).expect("decode failed");
            assert!(output.is_complete());
            assert_eq!(output.len(), 2);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hscan_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhscan_r2\r\n").await.expect("raw failed");
            let result = ctx.raw(&HscanInput::new(RedisKey::String("hscan_r2".into()), 0).command()).await.expect("raw failed");
            assert!(result.starts_with(b"*"));
            ctx.stop().await;
        }
    }
}
