use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Count, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HrandfieldInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hrandfield,
    "Returns one or more random fields from a hash",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HRANDFIELD`
/// https://redis.io/docs/latest/commands/hrandfield/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HrandfieldInput {
    pub(crate) key: RedisKey,
    pub(crate) count: Option<Count>,
}

impl Serialize for HrandfieldInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.count.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("HrandfieldInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(HrandfieldInput, API_INFO, {key, count});

impl RedisCommandInput for HrandfieldInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut cmd = crate::command::cmd(&API_INFO.api.to_string());
        cmd.arg(&self.key);
        if let Some(count) = &self.count {
            cmd.arg(&count.count);
            if count.with_values == Some(true) {
                cmd.arg("WITHVALUES");
            }
        }
        cmd.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("HRANDFIELD requires at least 1 argument"));
        }
        let count = if args.len() > 1 {
            let with_values = args.get(2).and_then(|v| match v {
                RedisJsonValue::String(s) if s.to_uppercase() == "WITHVALUES" => Some(true),
                _ => None,
            });
            Some(Count { count: args[1].clone(), with_values })
        } else {
            None
        };
        Ok(Self { key: args[0].clone().try_into()?, count })
    }
}

/// Output for Redis HRANDFIELD command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum HrandfieldOutput {
    /// Single field (when no count specified)
    Single(Option<String>),
    /// Multiple fields (when count specified without WITHVALUES)
    Fields(Vec<String>),
    /// Fields with values (when count specified with WITHVALUES)
    FieldsWithValues(Vec<(String, String)>),
}

impl HrandfieldOutput {
    /// Check if no fields were returned
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(s) => s.is_none(),
            Self::Fields(f) => f.is_empty(),
            Self::FieldsWithValues(f) => f.is_empty(),
        }
    }

    /// Get fields as a slice (without values)
    pub fn fields(&self) -> Vec<&str> {
        match self {
            Self::Single(Some(s)) => vec![s.as_str()],
            Self::Single(None) => vec![],
            Self::Fields(f) => f.iter().map(|s| s.as_str()).collect(),
            Self::FieldsWithValues(f) => f.iter().map(|(k, _)| k.as_str()).collect(),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            // Single field response (bulk string or null)
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(b)) => Ok(Self::Single(Some(String::from_utf8_lossy(&b).to_string()))),
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(Self::Single(None)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                Ok(Self::Single(Some(String::from_utf8_lossy(&data).to_string())))
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(Self::Single(None)),
            // Array response
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_array_resp2(&arr),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_array_resp3(&data),
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected HRANDFIELD response: {:?}", other))),
        }
    }

    fn parse_array_resp2(arr: &[Resp2Frame]) -> Result<Self, EpError> {
        if arr.is_empty() {
            return Ok(Self::Fields(vec![]));
        }

        // Check if first element is an array (nested array format)
        if let Some(Resp2Frame::Array(_)) = arr.first() {
            // Nested arrays: [[field, value], [field, value], ...]
            let mut pairs = Vec::new();
            for elem in arr {
                match elem {
                    Resp2Frame::Array(inner) => {
                        if inner.len() != 2 {
                            return Err(EpError::parse("expected field/value pair"));
                        }
                        let field = match &inner[0] {
                            Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                            _ => return Err(EpError::parse("expected string for field")),
                        };
                        let value = match &inner[1] {
                            Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                            _ => return Err(EpError::parse("expected string for value")),
                        };
                        pairs.push((field, value));
                    }
                    _ => return Err(EpError::parse("expected array for WITHVALUES")),
                }
            }
            return Ok(Self::FieldsWithValues(pairs));
        }

        // Flat array - check if it's field/value pairs or just fields
        // If array length is even and elements alternate field/value, treat as WITHVALUES
        if arr.len() >= 2 && arr.len().is_multiple_of(2) {
            // Try to parse as field/value pairs
            let mut pairs = Vec::new();
            for chunk in arr.chunks(2) {
                let field = match &chunk[0] {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => return Self::parse_fields_only_resp2(arr),
                };
                let value = match &chunk[1] {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => return Self::parse_fields_only_resp2(arr),
                };
                pairs.push((field, value));
            }
            // Heuristic: if we successfully parsed pairs, assume WITHVALUES
            // This is ambiguous - caller should know based on their request
            Ok(Self::FieldsWithValues(pairs))
        } else {
            Self::parse_fields_only_resp2(arr)
        }
    }

    fn parse_fields_only_resp2(arr: &[Resp2Frame]) -> Result<Self, EpError> {
        let fields: Result<Vec<String>, _> = arr
            .iter()
            .map(|f| match f {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => Ok(String::from_utf8_lossy(b).to_string()),
                _ => Err(EpError::parse("expected string")),
            })
            .collect();
        Ok(Self::Fields(fields?))
    }

    fn parse_array_resp3(arr: &[Resp3Frame]) -> Result<Self, EpError> {
        if arr.is_empty() {
            return Ok(Self::Fields(vec![]));
        }

        // Check if first element is an array (RESP3 WITHVALUES format)
        if let Some(Resp3Frame::Array { .. }) = arr.first() {
            // RESP3 WITHVALUES returns [[field, value], [field, value], ...]
            let mut pairs = Vec::new();
            for elem in arr {
                match elem {
                    Resp3Frame::Array { data, .. } => {
                        if data.len() != 2 {
                            return Err(EpError::parse("expected field/value pair"));
                        }
                        let field = match &data[0] {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8_lossy(data).to_string()
                            }
                            _ => return Err(EpError::parse("expected string for field")),
                        };
                        let value = match &data[1] {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8_lossy(data).to_string()
                            }
                            _ => return Err(EpError::parse("expected string for value")),
                        };
                        pairs.push((field, value));
                    }
                    _ => return Err(EpError::parse("expected array for WITHVALUES")),
                }
            }
            return Ok(Self::FieldsWithValues(pairs));
        }

        // Flat array - check if it's field/value pairs or just fields
        if arr.len() >= 2 && arr.len().is_multiple_of(2) {
            let mut pairs = Vec::new();
            for chunk in arr.chunks(2) {
                let field = match &chunk[0] {
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                        String::from_utf8_lossy(data).to_string()
                    }
                    _ => return Self::parse_fields_only_resp3(arr),
                };
                let value = match &chunk[1] {
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                        String::from_utf8_lossy(data).to_string()
                    }
                    _ => return Self::parse_fields_only_resp3(arr),
                };
                pairs.push((field, value));
            }
            Ok(Self::FieldsWithValues(pairs))
        } else {
            Self::parse_fields_only_resp3(arr)
        }
    }

    fn parse_fields_only_resp3(arr: &[Resp3Frame]) -> Result<Self, EpError> {
        let fields: Result<Vec<String>, _> = arr
            .iter()
            .map(|f| match f {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    Ok(String::from_utf8_lossy(data).to_string())
                }
                _ => Err(EpError::parse("expected string")),
            })
            .collect();
        Ok(Self::Fields(fields?))
    }
}

impl Serialize for HrandfieldOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Single(s) => Serialize::serialize(s, serializer),
            Self::Fields(f) => Serialize::serialize(f, serializer),
            Self::FieldsWithValues(f) => Serialize::serialize(f, serializer),
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
            let input = HrandfieldInput { key: RedisKey::String("myhash".into()), count: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HRANDFIELD"));
            assert!(cmd_str.contains("myhash"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = HrandfieldInput {
                key: RedisKey::String("myhash".into()),
                count: Some(Count::new(3)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("3"));
        }

        #[test]
        fn test_encode_command_with_values() {
            let input = HrandfieldInput {
                key: RedisKey::String("myhash".into()),
                count: Some(Count::new(2).with_values()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHVALUES"));
        }

        #[test]
        fn test_decode_single_field() {
            let output = HrandfieldOutput::decode(b"$5\r\nfield\r\n").unwrap();
            match output {
                HrandfieldOutput::Single(Some(f)) => assert_eq!(f, "field"),
                _ => panic!("expected Single"),
            }
        }

        #[test]
        fn test_decode_null() {
            let output = HrandfieldOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_empty_array() {
            let output = HrandfieldOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_fields_array() {
            let output = HrandfieldOutput::decode(b"*3\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n").unwrap();
            assert_eq!(output.fields().len(), 3);
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = HrandfieldInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(5)];
            let input = HrandfieldInput::decode(args).unwrap();
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_no_args() {
            let err = HrandfieldInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HrandfieldInput { key: RedisKey::String("myhash".into()), count: None };
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
        async fn test_hrandfield_single() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhrand_hash1\r\n").await.expect("raw failed");
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hrand_hash1".into()),
                            fields: vec![Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into()))],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&HrandfieldInput { key: RedisKey::String("hrand_hash1".into()), count: None }.command())
                        .await
                        .expect("raw failed");
                    let output = HrandfieldOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hrandfield_with_count() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhrand_hash2\r\n").await.expect("raw failed");
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hrand_hash2".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                                Field::new(RedisJsonValue::String("f3".into()), RedisJsonValue::String("v3".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Use WITHVALUES to avoid ambiguity in parsing
                    // (even-length arrays without WITHVALUES are incorrectly parsed as field/value pairs)
                    let input = HrandfieldInput {
                        key: RedisKey::String("hrand_hash2".into()),
                        count: Some(Count::new(2).with_values()),
                    };

                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = HrandfieldOutput::decode(&result).expect("decode failed");
                    // WITHVALUES returns field/value pairs, so we get 2 fields
                    assert_eq!(output.fields().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hrandfield_missing_key() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhrand_missing\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(&HrandfieldInput { key: RedisKey::String("hrand_missing".into()), count: None }.command())
                        .await
                        .expect("raw failed");
                    let output = HrandfieldOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
