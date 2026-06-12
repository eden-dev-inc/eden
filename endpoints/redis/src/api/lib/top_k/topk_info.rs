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
use redis_protocol::resp3::types::FrameMap;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TopkInfoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TopkInfo, "Returns information about a sketch", ReqType::Read, true);

/// See official Redis documentation for `TOPK.INFO`
/// https://redis.io/docs/latest/commands/topk.info/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkInfoInput {
    key: RedisKey,
}

impl Serialize for TopkInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TopkInfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TopkInfoInput, API_INFO, { key });

impl RedisCommandInput for TopkInfoInput {
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
        if args.len() != 1 {
            return Err(EpError::request("TOPK.INFO requires exactly 1 argument"));
        }
        Ok(TopkInfoInput { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis TOPK.INFO command
///
/// Returns information about a Top-K sketch including k, width, depth, and decay.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkInfoOutput {
    /// Number of top items to keep
    k: i64,
    /// Width of the underlying Count-Min Sketch
    width: i64,
    /// Depth of the underlying Count-Min Sketch
    depth: i64,
    /// Decay factor for aging items
    decay: f64,
}

impl TopkInfoOutput {
    pub fn new(k: i64, width: i64, depth: i64, decay: f64) -> Self {
        Self { k, width, depth, decay }
    }

    /// Get the k value (number of top items tracked)
    pub fn k(&self) -> i64 {
        self.k
    }

    /// Get the width of the Count-Min Sketch
    pub fn width(&self) -> i64 {
        self.width
    }

    /// Get the depth of the Count-Min Sketch
    pub fn depth(&self) -> i64 {
        self.depth
    }

    /// Get the decay factor
    pub fn decay(&self) -> f64 {
        self.decay
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        // TOPK.INFO returns an array: [k, <value>, width, <value>, depth, <value>, decay, <value>]
        // or a map in RESP3
        let (k, width, depth, decay) = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_info_array_resp2(&arr)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => Self::parse_info_map_resp3(&data)?,
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_info_array_resp3(&data)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("unexpected TOPK.INFO response format")),
        };

        Ok(Self { k, width, depth, decay })
    }

    fn parse_info_array_resp2(arr: &[Resp2Frame]) -> Result<(i64, i64, i64, f64), EpError> {
        if arr.len() < 8 {
            return Err(EpError::parse("TOPK.INFO response too short"));
        }

        let mut k = 0i64;
        let mut width = 0i64;
        let mut depth = 0i64;
        let mut decay = 0.0f64;

        let mut i = 0;
        while i < arr.len() - 1 {
            let key = match &arr[i] {
                Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => String::from_utf8_lossy(b).to_lowercase(),
                _ => {
                    i += 1;
                    continue;
                }
            };

            match key.as_str() {
                "k" => k = Self::extract_int_resp2(&arr[i + 1])?,
                "width" => width = Self::extract_int_resp2(&arr[i + 1])?,
                "depth" => depth = Self::extract_int_resp2(&arr[i + 1])?,
                "decay" => decay = Self::extract_float_resp2(&arr[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok((k, width, depth, decay))
    }

    fn parse_info_array_resp3(arr: &[Resp3Frame]) -> Result<(i64, i64, i64, f64), EpError> {
        if arr.len() < 8 {
            return Err(EpError::parse("TOPK.INFO response too short"));
        }

        let mut k = 0i64;
        let mut width = 0i64;
        let mut depth = 0i64;
        let mut decay = 0.0f64;

        let mut i = 0;
        while i < arr.len() - 1 {
            let key = match &arr[i] {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_lowercase(),
                _ => {
                    i += 1;
                    continue;
                }
            };

            match key.as_str() {
                "k" => k = Self::extract_int_resp3(&arr[i + 1])?,
                "width" => width = Self::extract_int_resp3(&arr[i + 1])?,
                "depth" => depth = Self::extract_int_resp3(&arr[i + 1])?,
                "decay" => decay = Self::extract_float_resp3(&arr[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok((k, width, depth, decay))
    }

    fn parse_info_map_resp3(data: &FrameMap<Resp3Frame, Resp3Frame>) -> Result<(i64, i64, i64, f64), EpError> {
        let mut k = 0i64;
        let mut width = 0i64;
        let mut depth = 0i64;
        let mut decay = 0.0f64;

        for (key_frame, value_frame) in data {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => String::from_utf8_lossy(data).to_lowercase(),
                _ => continue,
            };

            match key.as_str() {
                "k" => k = Self::extract_int_resp3(value_frame)?,
                "width" => width = Self::extract_int_resp3(value_frame)?,
                "depth" => depth = Self::extract_int_resp3(value_frame)?,
                "decay" => decay = Self::extract_float_resp3(value_frame)?,
                _ => {}
            }
        }

        Ok((k, width, depth, decay))
    }

    fn extract_int_resp2(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(*i),
            Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                String::from_utf8_lossy(b).parse::<i64>().map_err(|_| EpError::parse("invalid integer value"))
            }
            _ => Err(EpError::parse("expected integer value")),
        }
    }

    fn extract_int_resp3(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8_lossy(data).parse::<i64>().map_err(|_| EpError::parse("invalid integer value"))
            }
            _ => Err(EpError::parse("expected integer value")),
        }
    }

    fn extract_float_resp2(frame: &Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                String::from_utf8_lossy(b).parse::<f64>().map_err(|_| EpError::parse("invalid float value"))
            }
            Resp2Frame::Integer(i) => Ok(*i as f64),
            _ => Err(EpError::parse("expected float value")),
        }
    }

    fn extract_float_resp3(frame: &Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(*data),
            Resp3Frame::Number { data, .. } => Ok(*data as f64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8_lossy(data).parse::<f64>().map_err(|_| EpError::parse("invalid float value"))
            }
            _ => Err(EpError::parse("expected float value")),
        }
    }
}

impl Serialize for TopkInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkInfoOutput", 4)?;
        state.serialize_field("k", &self.k)?;
        state.serialize_field("width", &self.width)?;
        state.serialize_field("depth", &self.depth)?;
        state.serialize_field("decay", &self.decay)?;
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
            let input = TopkInfoInput { key: RedisKey::String("mytopk".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.INFO"));
            assert!(cmd_str.contains("mytopk"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TopkInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args_fails() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let err = TopkInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkInfoInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = TopkInfoOutput::new(10, 2000, 7, 0.9);
            assert_eq!(output.k(), 10);
            assert_eq!(output.width(), 2000);
            assert_eq!(output.depth(), 7);
            assert!((output.decay() - 0.9).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_resp2_array() {
            // RESP2 array format: *8\r\n$1\r\nk\r\n:10\r\n$5\r\nwidth\r\n:2000\r\n$5\r\ndepth\r\n:7\r\n$5\r\ndecay\r\n$3\r\n0.9\r\n
            let bytes = b"*8\r\n$1\r\nk\r\n:10\r\n$5\r\nwidth\r\n:2000\r\n$5\r\ndepth\r\n:7\r\n$5\r\ndecay\r\n$3\r\n0.9\r\n";
            let output = TopkInfoOutput::decode(bytes).unwrap();
            assert_eq!(output.k(), 10);
            assert_eq!(output.width(), 2000);
            assert_eq!(output.depth(), 7);
            assert!((output.decay() - 0.9).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_error_resp2() {
            let err = TopkInfoOutput::decode(b"-ERR unknown key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TOPK commands require RedisBloom module
        // These tests will be skipped if RedisBloom is not available

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_info_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First create a TopK structure
                    let reserve_result = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$11\r\ninfo_topk\r\n$2\r\n10\r\n").await;

                    // Skip if RedisBloom not available
                    if reserve_result.is_err() {
                        return;
                    }
                    let reserve_bytes = reserve_result.unwrap();
                    if reserve_bytes.starts_with(b"-") {
                        return; // RedisBloom not available
                    }

                    let result = ctx.raw(&TopkInfoInput { key: RedisKey::String("info_topk".into()) }.command()).await.expect("raw failed");

                    let output = TopkInfoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.k(), 10);
                    assert!(output.width() > 0);
                    assert!(output.depth() > 0);
                })
            })
            .await;
        }
    }
}
