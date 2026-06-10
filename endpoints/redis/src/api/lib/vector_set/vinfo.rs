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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, VinfoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vinfo, "Retrieve information about a vector set", ReqType::Read, true);

/// See official Redis documentation for `VINFO`
/// https://redis.io/docs/latest/commands/vinfo/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VinfoInput {
    key: RedisKey,
}

impl VinfoInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for VinfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VinfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(VinfoInput, API_INFO, { key });

impl RedisCommandInput for VinfoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("VINFO requires 1 argument, given None"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "VINFO expects 1 argument, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis VINFO command
///
/// Returns information about a vector set as key-value pairs.
/// Returns None if the key does not exist.
///
/// See official Redis documentation for `VINFO`
/// https://redis.io/docs/latest/commands/vinfo/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VinfoOutput {
    /// Information about the vector set as key-value pairs
    info: Option<HashMap<String, RedisJsonValue>>,
}

impl VinfoOutput {
    pub fn new(info: Option<HashMap<String, RedisJsonValue>>) -> Self {
        Self { info }
    }

    /// Get the info map
    pub fn info(&self) -> Option<&HashMap<String, RedisJsonValue>> {
        self.info.as_ref()
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.info.is_some()
    }

    /// Get a specific field from the info
    pub fn get(&self, key: &str) -> Option<&RedisJsonValue> {
        self.info.as_ref()?.get(key)
    }

    /// Helper to parse array response into key-value pairs
    fn parse_array_to_map(items: Vec<RedisJsonValue>) -> HashMap<String, RedisJsonValue> {
        let mut map = HashMap::new();
        let mut iter = items.into_iter();
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            if let RedisJsonValue::String(k) = key {
                map.insert(k, value);
            }
        }
        map
    }

    /// Decode the Redis protocol response into a VinfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let info = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let items: Vec<RedisJsonValue> = arr.into_iter().map(Self::resp2_frame_to_json).collect::<Result<_, _>>()?;
                    Some(Self::parse_array_to_map(items))
                }
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VINFO response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let items: Vec<RedisJsonValue> = data.into_iter().map(Self::resp3_frame_to_json).collect::<Result<_, _>>()?;
                    Some(Self::parse_array_to_map(items))
                }
                Resp3Frame::Map { data, .. } => {
                    let mut map = HashMap::new();
                    for (k, v) in data {
                        let key = Self::resp3_frame_to_json(k)?;
                        let value = Self::resp3_frame_to_json(v)?;
                        if let RedisJsonValue::String(k) = key {
                            map.insert(k, value);
                        }
                    }
                    Some(map)
                }
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VINFO response: {:?}", other)));
                }
            },
        };

        Ok(Self { info })
    }

    fn resp2_frame_to_json(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) => Ok(RedisJsonValue::String(String::from_utf8(s).map_err(EpError::parse)?)),
            Resp2Frame::BulkString(s) => Ok(RedisJsonValue::String(String::from_utf8(s).map_err(EpError::parse)?)),
            Resp2Frame::Integer(n) => Ok(RedisJsonValue::Integer(n)),
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            Resp2Frame::Array(arr) => {
                let items: Vec<RedisJsonValue> = arr.into_iter().map(Self::resp2_frame_to_json).collect::<Result<_, _>>()?;
                Ok(RedisJsonValue::Array(items))
            }
            other => Err(EpError::parse(format!("unexpected frame: {:?}", other))),
        }
    }

    fn resp3_frame_to_json(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::BlobString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
            Resp3Frame::Double { data, .. } => Ok(RedisJsonValue::Float(data)),
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            Resp3Frame::Array { data, .. } => {
                let items: Vec<RedisJsonValue> = data.into_iter().map(Self::resp3_frame_to_json).collect::<Result<_, _>>()?;
                Ok(RedisJsonValue::Array(items))
            }
            other => Err(EpError::parse(format!("unexpected frame: {:?}", other))),
        }
    }
}

impl Serialize for VinfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VinfoOutput", 1)?;
        state.serialize_field("info", &self.info)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = VinfoInput { key: RedisKey::String("myvset".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nVINFO\r\n$6\r\nmyvset\r\n");
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VinfoOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert!(output.info().is_none());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VinfoOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert!(output.info().is_none());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VinfoOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = VinfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = VinfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VinfoInput { key: RedisKey::String("test".into()) };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }

        #[test]
        fn test_parse_array_to_map() {
            let items = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::Integer(42),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("value2".into()),
            ];
            let map = VinfoOutput::parse_array_to_map(items);
            assert_eq!(map.get("key1"), Some(&RedisJsonValue::Integer(42)));
            assert_eq!(map.get("key2"), Some(&RedisJsonValue::String("value2".into())));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VINFO requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vinfo_nonexistent_key() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VinfoInput::new("nonexistent_vset").command()).await.expect("raw failed");

                    let output = VinfoOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vinfo_after_vadd() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // Add element to vector set
                    ctx.raw(
                        b"*8\r\n$4\r\nVADD\r\n$14\r\nvinfo_testvset\r\n$6\r\nVALUES\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$3\r\n3.0\r\n$4\r\nelem\r\n"
                    )
                        .await
                        .expect("vadd failed");

                    let result = ctx
                        .raw(&VinfoInput::new("vinfo_testvset").command())
                        .await
                        .expect("raw failed");

                    let output = VinfoOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    // VINFO returns various fields about the vector set
                    assert!(output.info().is_some());
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vinfo_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VinfoInput::new("missing_vset").command()).await.expect("raw failed");

            let output = VinfoOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vinfo_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VinfoInput::new("missing_vset").command()).await.expect("raw failed");

            let output = VinfoOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());

            ctx.stop().await;
        }
    }
}
