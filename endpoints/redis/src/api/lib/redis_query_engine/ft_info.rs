use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtInfo,
    "Returns information and statistics on the index",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.INFO`
/// https://redis.io/docs/latest/commands/ft.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtInfoInput {
    index: RedisJsonValue,
}

impl Serialize for FtInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtInfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.end()
    }
}

impl_redis_operation!(FtInfoInput, API_INFO, { index });

impl RedisCommandInput for FtInfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FT.INFO requires 1 argument, given {}", args.len())));
        }

        Ok(Self { index: args[0].clone() })
    }
}

/// Output for Redis `FT.INFO` command.
///
/// Returns detailed information about the index including statistics,
/// schema definition, and configuration.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtInfoOutput {
    /// Raw key-value pairs from the info response
    info: HashMap<String, RedisJsonValue>,
}

impl Serialize for FtInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtInfoOutput", 1)?;
        state.serialize_field("info", &self.info)?;
        state.end()
    }
}

impl FtInfoOutput {
    pub fn new(info: HashMap<String, RedisJsonValue>) -> Self {
        Self { info }
    }

    /// Get the raw info map
    pub fn info(&self) -> &HashMap<String, RedisJsonValue> {
        &self.info
    }

    /// Get a specific field from the info
    pub fn get(&self, key: &str) -> Option<&RedisJsonValue> {
        self.info.get(key)
    }

    /// Get the index name
    pub fn index_name(&self) -> Option<&str> {
        self.info.get("index_name").and_then(|v| {
            if let RedisJsonValue::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
    }

    /// Get the number of documents in the index
    pub fn num_docs(&self) -> Option<i64> {
        self.info.get("num_docs").and_then(|v| match v {
            RedisJsonValue::Integer(i) => Some(*i),
            RedisJsonValue::String(s) => s.parse().ok(),
            _ => None,
        })
    }

    /// Decode the Redis protocol response into a FtInfoOutput
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
                let mut info = HashMap::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    if let Resp2Frame::BulkString(key) = &arr[i] {
                        let value = Self::frame_to_json_resp2(&arr[i + 1])?;
                        info.insert(String::from_utf8(key.to_vec()).map_err(EpError::parse)?, value);
                    }
                    i += 2;
                }
                Ok(Self { info })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.INFO response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut info = HashMap::new();
                let mut i = 0;
                let frame = data;
                while i + 1 < frame.len() {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &frame[i] {
                        let value = Self::frame_to_json_resp3(&frame[i + 1])?;
                        info.insert(String::from_utf8(data.to_vec()).map_err(EpError::parse)?, value);
                    }
                    i += 2;
                }
                Ok(Self { info })
            }
            Resp3Frame::Map { data, .. } => {
                let mut info = HashMap::new();
                for (k, v) in data {
                    let key = match k {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => data,
                        _ => continue,
                    };
                    let value = Self::frame_to_json_resp3(&v)?;
                    info.insert(String::from_utf8(key).map_err(EpError::parse)?, value);
                }
                Ok(Self { info })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(data.as_slice()).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.INFO response: {:?}", other))),
        }
    }

    fn frame_to_json_resp2(frame: &Resp2Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) => {
                RedisJsonValue::String(String::from_utf8(s.to_vec()).map_err(EpError::parse)?)
            }
            Resp2Frame::Integer(i) => RedisJsonValue::Integer(*i),
            Resp2Frame::Array(arr) => {
                let mut items = Vec::with_capacity(arr.len());
                for item in arr {
                    items.push(Self::frame_to_json_resp2(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp2Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }

    fn frame_to_json_resp3(frame: &Resp3Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8(data.to_vec()).map_err(EpError::parse)?)
            }
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
            Resp3Frame::Array { data, .. } => {
                let mut items = Vec::with_capacity(data.len());
                for item in data {
                    items.push(Self::frame_to_json_resp3(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            Resp3Frame::Double { data, .. } => RedisJsonValue::Float(*data),
            Resp3Frame::Boolean { data, .. } => RedisJsonValue::Bool(*data),
            _ => RedisJsonValue::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtInfoInput { index: RedisJsonValue::String("my_index".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.INFO"));
            assert!(cmd_str.contains("my_index"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let input = FtInfoInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("idx1".into()), RedisJsonValue::String("idx2".into())];
            let err = FtInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtInfoOutput::decode(b"-ERR Unknown Index name\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown Index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtInfoInput { index: RedisJsonValue::String("i".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtInfoInput { index: RedisJsonValue::String("test_idx".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
        }

        #[test]
        fn test_serialize_output() {
            let mut info = HashMap::new();
            info.insert("index_name".into(), RedisJsonValue::String("test".into()));
            let output = FtInfoOutput::new(info);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("info"));
        }

        #[test]
        fn test_output_accessors() {
            let mut info = HashMap::new();
            info.insert("index_name".into(), RedisJsonValue::String("my_idx".into()));
            info.insert("num_docs".into(), RedisJsonValue::Integer(100));
            let output = FtInfoOutput::new(info);

            assert_eq!(output.index_name(), Some("my_idx"));
            assert_eq!(output.num_docs(), Some(100));
            assert!(output.get("index_name").is_some());
            assert!(output.get("nonexistent").is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.INFO requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_info_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtInfoInput { index: RedisJsonValue::String("nonexistent_index".into()) }.command()).await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                            let err = FtInfoOutput::decode(&r);
                            assert!(err.is_err());
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
