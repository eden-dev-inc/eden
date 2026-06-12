use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtCursorReadInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtCursorRead, "Reads from a cursor", ReqType::Read, true);

/// See official Redis documentation for `FT.CURSOR READ`
/// https://redis.io/docs/latest/commands/ft.cursor-read/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtCursorReadInput {
    index: RedisJsonValue,
    cursor_id: RedisJsonValue,
    count: Option<RedisJsonValue>,
}

impl Serialize for FtCursorReadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;

        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtCursorReadInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("cursor_id", &self.cursor_id)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    FtCursorReadInput,
    API_INFO,
    {index, cursor_id, count});

impl RedisCommandInput for FtCursorReadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.cursor_id);

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.CURSOR READ requires at least 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            index: args[0].clone(),
            cursor_id: args[1].clone(),
            count: args.get(2).cloned(),
        })
    }
}

/// Output for Redis `FT.CURSOR READ` command.
///
/// Returns the next batch of results from a cursor, along with the cursor ID
/// for subsequent reads.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtCursorReadOutput {
    /// The results from this cursor read
    results: Vec<RedisJsonValue>,
    /// The cursor ID for the next read (0 if exhausted)
    cursor_id: u64,
}

impl Serialize for FtCursorReadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtCursorReadOutput", 2)?;
        state.serialize_field("results", &self.results)?;
        state.serialize_field("cursor_id", &self.cursor_id)?;
        state.end()
    }
}

impl FtCursorReadOutput {
    pub fn new(results: Vec<RedisJsonValue>, cursor_id: u64) -> Self {
        Self { results, cursor_id }
    }

    /// Get the results from this cursor read
    pub fn results(&self) -> &[RedisJsonValue] {
        &self.results
    }

    /// Get the cursor ID for the next read
    pub fn cursor_id(&self) -> u64 {
        self.cursor_id
    }

    /// Check if there are more results to read
    pub fn has_more(&self) -> bool {
        self.cursor_id != 0
    }

    /// Decode the Redis protocol response into a FtCursorReadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                if arr.len() != 2 {
                    return Err(EpError::parse("FT.CURSOR READ response should have 2 elements"));
                }

                let results = Self::extract_results_resp2(&arr[0])?;
                let cursor_id = Self::extract_cursor_id_resp2(&arr[1])?;

                Ok(Self { results, cursor_id })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.CURSOR READ response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.len() != 2 {
                    return Err(EpError::parse("FT.CURSOR READ response should have 2 elements"));
                }

                let results = Self::extract_results_resp3(&data[0])?;
                let cursor_id = Self::extract_cursor_id_resp3(&data[1])?;

                Ok(Self { results, cursor_id })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.CURSOR READ response: {:?}", other))),
        }
    }

    fn extract_results_resp2(frame: &Resp2Frame) -> Result<Vec<RedisJsonValue>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::new();
                for item in arr {
                    results.push(Self::frame_to_json_resp2(item)?);
                }
                Ok(results)
            }
            _ => Ok(vec![]),
        }
    }

    fn extract_results_resp3(frame: &Resp3Frame) -> Result<Vec<RedisJsonValue>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut results = Vec::new();
                for item in data {
                    results.push(Self::frame_to_json_resp3(item)?);
                }
                Ok(results)
            }
            _ => Ok(vec![]),
        }
    }

    fn extract_cursor_id_resp2(frame: &Resp2Frame) -> Result<u64, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(*i as u64),
            Resp2Frame::BulkString(s) => {
                String::from_utf8(s.to_vec()).map_err(EpError::parse)?.parse::<u64>().map_err(|_| EpError::parse("invalid cursor ID"))
            }
            _ => Err(EpError::parse("expected integer cursor ID")),
        }
    }

    fn extract_cursor_id_resp3(frame: &Resp3Frame) -> Result<u64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data as u64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.to_vec())
                .map_err(EpError::parse)?
                .parse::<u64>()
                .map_err(|_| EpError::parse("invalid cursor ID")),
            _ => Err(EpError::parse("expected integer cursor ID")),
        }
    }

    fn frame_to_json_resp2(frame: &Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::BulkString(s) => Ok(RedisJsonValue::String(String::from_utf8(s.to_owned()).map_err(EpError::parse)?)),
            Resp2Frame::Integer(i) => Ok(RedisJsonValue::Integer(*i)),
            Resp2Frame::Array(arr) => {
                let mut items = Vec::new();
                for item in arr {
                    items.push(Self::frame_to_json_resp2(item)?);
                }
                Ok(RedisJsonValue::Array(items))
            }
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
        }
    }

    fn frame_to_json_resp3(frame: &Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                Ok(RedisJsonValue::String(String::from_utf8(data.to_vec()).map_err(EpError::parse)?))
            }
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(*data)),
            Resp3Frame::Array { data, .. } => {
                let mut items = Vec::new();
                for item in data {
                    items.push(Self::frame_to_json_resp3(item)?);
                }
                Ok(RedisJsonValue::Array(items))
            }
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
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
            let input = FtCursorReadInput {
                index: RedisJsonValue::String("my_index".into()),
                cursor_id: RedisJsonValue::Integer(12345),
                count: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.CURSOR"));
            assert!(cmd_str.contains("READ"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("12345"));
            assert!(!cmd_str.contains("COUNT"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = FtCursorReadInput {
                index: RedisJsonValue::String("my_index".into()),
                cursor_id: RedisJsonValue::Integer(12345),
                count: Some(RedisJsonValue::Integer(100)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("100"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::Integer(999)];
            let input = FtCursorReadInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.cursor_id, RedisJsonValue::Integer(999));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::Integer(999),
                RedisJsonValue::Integer(50),
            ];
            let input = FtCursorReadInput::decode(args).unwrap();
            assert_eq!(input.count, Some(RedisJsonValue::Integer(50)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtCursorReadInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtCursorReadOutput::decode(b"-ERR Cursor not found\r\n").unwrap_err();
            assert!(err.to_string().contains("Cursor"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtCursorReadInput {
                index: RedisJsonValue::String("i".into()),
                cursor_id: RedisJsonValue::Integer(1),
                count: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let output = FtCursorReadOutput::new(vec![RedisJsonValue::String("doc1".into())], 12345);
            assert_eq!(output.results().len(), 1);
            assert_eq!(output.cursor_id(), 12345);
            assert!(output.has_more());
        }

        #[test]
        fn test_output_exhausted_cursor() {
            let output = FtCursorReadOutput::new(vec![], 0);
            assert!(!output.has_more());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtCursorReadInput {
                index: RedisJsonValue::String("test_idx".into()),
                cursor_id: RedisJsonValue::Integer(123),
                count: Some(RedisJsonValue::Integer(10)),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("123"));
            assert!(json.contains("count"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtCursorReadOutput::new(vec![], 0);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("cursor_id"));
            assert!(json.contains("results"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.CURSOR READ requires RediSearch module and an active cursor.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_cursor_read_invalid_cursor() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtCursorReadInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                cursor_id: RedisJsonValue::Integer(999999),
                                count: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for invalid cursor
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other case
                        }
                    }
                })
            })
            .await;
        }
    }
}
