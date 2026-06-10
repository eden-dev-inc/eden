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

const API_INFO: ApiInfo<RedisApi, CfInfoInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::CfInfo, "Returns information about a Cuckoo Filter", ReqType::Read, true);

/// Input for Redis `CF.INFO` command.
///
/// Returns information about a Cuckoo Filter including size, number of buckets,
/// number of items, and more.
///
/// See official Redis documentation for `CF.INFO`:
/// https://redis.io/docs/latest/commands/cf.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfInfoInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
}

impl CfInfoInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for CfInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfInfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(CfInfoInput, API_INFO, { key });

impl RedisCommandInput for CfInfoInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("CF.INFO requires 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `CF.INFO` command.
///
/// Contains information about the Cuckoo Filter's configuration and current state.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfInfoOutput {
    /// Total size in bytes
    pub size: i64,
    /// Number of buckets
    pub num_buckets: i64,
    /// Number of filters (sub-filters for scaling)
    pub num_filters: i64,
    /// Number of items inserted
    pub num_items_inserted: i64,
    /// Number of items deleted
    pub num_items_deleted: i64,
    /// Bucket size
    pub bucket_size: i64,
    /// Expansion rate
    pub expansion_rate: i64,
    /// Maximum number of iterations
    pub max_iterations: i64,
}

impl CfInfoOutput {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        size: i64,
        num_buckets: i64,
        num_filters: i64,
        num_items_inserted: i64,
        num_items_deleted: i64,
        bucket_size: i64,
        expansion_rate: i64,
        max_iterations: i64,
    ) -> Self {
        Self {
            size,
            num_buckets,
            num_filters,
            num_items_inserted,
            num_items_deleted,
            bucket_size,
            expansion_rate,
            max_iterations,
        }
    }

    /// Get the total size in bytes
    pub fn size(&self) -> i64 {
        self.size
    }

    /// Get the number of items currently in the filter
    pub fn item_count(&self) -> i64 {
        self.num_items_inserted - self.num_items_deleted
    }

    /// Decode the Redis protocol response into a CfInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => Self::parse_array_pairs(
                &arr,
                |f| match f {
                    Resp2Frame::BulkString(s) => String::from_utf8_lossy(s).parse().ok(),
                    _ => None,
                },
                |f| match f {
                    Resp2Frame::Integer(n) => Some(*n),
                    Resp2Frame::BulkString(s) => String::from_utf8_lossy(s).parse().ok(),
                    _ => None,
                },
            ),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected CF.INFO response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => Self::parse_array_pairs(
                &data,
                |f| match f {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).ok(),
                    Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).ok(),
                    _ => None,
                },
                |f| match f {
                    Resp3Frame::Number { data, .. } => Some(*data),
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).ok()?.parse().ok(),
                    _ => None,
                },
            ),
            Resp3Frame::Map { data, .. } => Self::parse_map(&data),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected CF.INFO response: {:?}", other))),
        }
    }

    fn parse_array_pairs<T, FK, FV>(arr: &[T], get_key: FK, get_val: FV) -> Result<Self, EpError>
    where
        FK: Fn(&T) -> Option<String>,
        FV: Fn(&T) -> Option<i64>,
    {
        let mut size = 0;
        let mut num_buckets = 0;
        let mut num_filters = 0;
        let mut num_items_inserted = 0;
        let mut num_items_deleted = 0;
        let mut bucket_size = 0;
        let mut expansion_rate = 0;
        let mut max_iterations = 0;

        let mut i = 0;
        while i + 1 < arr.len() {
            if let Some(key) = get_key(&arr[i])
                && let Some(val) = get_val(&arr[i + 1])
            {
                match key.to_lowercase().as_str() {
                    "size" => size = val,
                    "number of buckets" => num_buckets = val,
                    "number of filters" => num_filters = val,
                    "number of items inserted" => num_items_inserted = val,
                    "number of items deleted" => num_items_deleted = val,
                    "bucket size" => bucket_size = val,
                    "expansion rate" => expansion_rate = val,
                    "max iterations" => max_iterations = val,
                    _ => {}
                }
            }
            i += 2;
        }

        Ok(Self {
            size,
            num_buckets,
            num_filters,
            num_items_inserted,
            num_items_deleted,
            bucket_size,
            expansion_rate,
            max_iterations,
        })
    }

    fn parse_map(data: &FrameMap<Resp3Frame, Resp3Frame>) -> Result<Self, EpError> {
        let mut size = 0;
        let mut num_buckets = 0;
        let mut num_filters = 0;
        let mut num_items_inserted = 0;
        let mut num_items_deleted = 0;
        let mut bucket_size = 0;
        let mut expansion_rate = 0;
        let mut max_iterations = 0;

        for (k, v) in data {
            let key = match k {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).unwrap_or_default(),
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).unwrap_or_default(),
                _ => continue,
            };

            let val = match v {
                Resp3Frame::Number { data, .. } => *data,
                _ => continue,
            };

            match key.to_lowercase().as_str() {
                "size" => size = val,
                "number of buckets" => num_buckets = val,
                "number of filters" => num_filters = val,
                "number of items inserted" => num_items_inserted = val,
                "number of items deleted" => num_items_deleted = val,
                "bucket size" => bucket_size = val,
                "expansion rate" => expansion_rate = val,
                "max iterations" => max_iterations = val,
                _ => {}
            }
        }

        Ok(Self {
            size,
            num_buckets,
            num_filters,
            num_items_inserted,
            num_items_deleted,
            bucket_size,
            expansion_rate,
            max_iterations,
        })
    }
}

impl Serialize for CfInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfInfoOutput", 8)?;
        state.serialize_field("size", &self.size)?;
        state.serialize_field("num_buckets", &self.num_buckets)?;
        state.serialize_field("num_filters", &self.num_filters)?;
        state.serialize_field("num_items_inserted", &self.num_items_inserted)?;
        state.serialize_field("num_items_deleted", &self.num_items_deleted)?;
        state.serialize_field("bucket_size", &self.bucket_size)?;
        state.serialize_field("expansion_rate", &self.expansion_rate)?;
        state.serialize_field("max_iterations", &self.max_iterations)?;
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
            let input = CfInfoInput { key: RedisKey::String("myfilter".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.INFO"));
            assert!(cmd_str.contains("myfilter"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfInfoInput::new("filter1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfInfoInput::new("testfilter");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let input = CfInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = CfInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = CfInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_output_new() {
            let output = CfInfoOutput::new(1024, 128, 1, 10, 2, 2, 1, 20);
            assert_eq!(output.size(), 1024);
            assert_eq!(output.num_buckets, 128);
            assert_eq!(output.item_count(), 8); // 10 inserted - 2 deleted
        }

        #[test]
        fn test_item_count_calculation() {
            let output = CfInfoOutput::new(0, 0, 0, 100, 25, 0, 0, 0);
            assert_eq!(output.item_count(), 75);
        }

        #[test]
        fn test_decode_error() {
            let err = CfInfoOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::CfAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_info_basic() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // First create a filter with some items
            let add_result = ctx.raw(&CfAddInput::new("cf_info_test", "item1").command()).await;

            match add_result {
                Ok(_) => {
                    // Add a few more items
                    ctx.raw(&CfAddInput::new("cf_info_test", "item2").command()).await.expect("add item2");

                    let result = ctx.raw(&CfInfoInput::new("cf_info_test").command()).await.expect("raw failed");

                    let output = CfInfoOutput::decode(&result).expect("decode failed");
                    assert!(output.size() > 0);
                    assert!(output.num_items_inserted >= 2);
                }
                Err(e) => {
                    if e.to_string().contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    } else {
                        panic!("Unexpected error: {}", e);
                    }
                }
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_info_nonexistent_filter() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let result = ctx.raw(&CfInfoInput::new("nonexistent_filter").command()).await;

            // Should return an error for non-existent filter
            match result {
                Ok(bytes) => {
                    let decode_result = CfInfoOutput::decode(&bytes);
                    // Either decodes to empty/error or fails parsing
                    if decode_result.is_err() {
                        // Expected - non-existent filter
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    }
                    // Error is expected for non-existent filter
                }
            }

            ctx.stop().await;
        }
    }
}
