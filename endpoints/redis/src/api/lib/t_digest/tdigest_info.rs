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

const API_INFO: ApiInfo<RedisApi, TdigestInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestInfo,
    "Returns information and statistics about a t-digest sketch",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.INFO` command.
///
/// Returns information and statistics about a t-digest sketch.
///
/// See official Redis documentation for `TDIGEST.INFO`:
/// https://redis.io/docs/latest/commands/tdigest.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestInfoInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
}

impl Serialize for TdigestInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestInfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TdigestInfoInput, API_INFO, { key });

impl RedisCommandInput for TdigestInfoInput {
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
            return Err(EpError::parse(format!("TDIGEST.INFO requires exactly 1 argument (key), given {}", args.len())));
        }
        Ok(TdigestInfoInput { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `TDIGEST.INFO` command.
///
/// Contains information and statistics about the t-digest sketch.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestInfoOutput {
    /// The compression parameter used
    compression: i64,
    /// The capacity of the sketch
    capacity: i64,
    /// Number of centroids merged
    merged_nodes: i64,
    /// Number of unmerged centroids
    unmerged_nodes: i64,
    /// Weight of merged centroids
    merged_weight: f64,
    /// Weight of unmerged centroids
    unmerged_weight: f64,
    /// Total number of compressions performed
    total_compressions: i64,
    /// Memory usage in bytes
    memory_usage: i64,
}

impl TdigestInfoOutput {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compression: i64,
        capacity: i64,
        merged_nodes: i64,
        unmerged_nodes: i64,
        merged_weight: f64,
        unmerged_weight: f64,
        total_compressions: i64,
        memory_usage: i64,
    ) -> Self {
        Self {
            compression,
            capacity,
            merged_nodes,
            unmerged_nodes,
            merged_weight,
            unmerged_weight,
            total_compressions,
            memory_usage,
        }
    }

    /// Get the compression parameter
    pub fn compression(&self) -> i64 {
        self.compression
    }

    /// Get the capacity
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Get the number of merged nodes
    pub fn merged_nodes(&self) -> i64 {
        self.merged_nodes
    }

    /// Get the number of unmerged nodes
    pub fn unmerged_nodes(&self) -> i64 {
        self.unmerged_nodes
    }

    /// Get the merged weight
    pub fn merged_weight(&self) -> f64 {
        self.merged_weight
    }

    /// Get the unmerged weight
    pub fn unmerged_weight(&self) -> f64 {
        self.unmerged_weight
    }

    /// Get the total number of compressions
    pub fn total_compressions(&self) -> i64 {
        self.total_compressions
    }

    /// Get the memory usage in bytes
    pub fn memory_usage(&self) -> i64 {
        self.memory_usage
    }

    /// Get the total observations (merged_weight + unmerged_weight)
    pub fn total_observations(&self) -> f64 {
        self.merged_weight + self.unmerged_weight
    }

    /// Decode the Redis protocol response into a TdigestInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        Self::parse_frame(frame)
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Self, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => Self::parse_array_pairs(&arr),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TDIGEST.INFO response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Map { data, .. } => {
                let mut compression = 0i64;
                let mut capacity = 0i64;
                let mut merged_nodes = 0i64;
                let mut unmerged_nodes = 0i64;
                let mut merged_weight = 0.0f64;
                let mut unmerged_weight = 0.0f64;
                let mut total_compressions = 0i64;
                let mut memory_usage = 0i64;

                for (key, value) in data {
                    let key_str = Self::extract_resp3_string(&key)?;
                    match key_str.to_lowercase().as_str() {
                        "compression" => compression = Self::extract_resp3_integer(&value)?,
                        "capacity" => capacity = Self::extract_resp3_integer(&value)?,
                        "merged nodes" | "merged_nodes" => merged_nodes = Self::extract_resp3_integer(&value)?,
                        "unmerged nodes" | "unmerged_nodes" => unmerged_nodes = Self::extract_resp3_integer(&value)?,
                        "merged weight" | "merged_weight" => merged_weight = Self::extract_resp3_float(&value)?,
                        "unmerged weight" | "unmerged_weight" => unmerged_weight = Self::extract_resp3_float(&value)?,
                        "total compressions" | "total_compressions" => total_compressions = Self::extract_resp3_integer(&value)?,
                        "memory usage" | "memory_usage" => memory_usage = Self::extract_resp3_integer(&value)?,
                        _ => {} // Ignore unknown fields
                    }
                }

                Ok(Self {
                    compression,
                    capacity,
                    merged_nodes,
                    unmerged_nodes,
                    merged_weight,
                    unmerged_weight,
                    total_compressions,
                    memory_usage,
                })
            }
            Resp3Frame::Array { data, .. } => {
                let frames: Vec<Resp2Frame> = data.into_iter().filter_map(Self::resp3_to_resp2).collect();
                Self::parse_array_pairs(&frames)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.INFO response: {:?}", other))),
        }
    }

    fn resp3_to_resp2(frame: Resp3Frame) -> Option<Resp2Frame> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Some(Resp2Frame::BulkString(data)),
            Resp3Frame::SimpleString { data, .. } => Some(Resp2Frame::SimpleString(data)),
            Resp3Frame::Number { data, .. } => Some(Resp2Frame::Integer(data)),
            Resp3Frame::Double { data, .. } => Some(Resp2Frame::BulkString(data.to_string().into_bytes())),
            _ => None,
        }
    }

    fn parse_array_pairs(arr: &[Resp2Frame]) -> Result<Self, EpError> {
        let mut compression = 0i64;
        let mut capacity = 0i64;
        let mut merged_nodes = 0i64;
        let mut unmerged_nodes = 0i64;
        let mut merged_weight = 0.0f64;
        let mut unmerged_weight = 0.0f64;
        let mut total_compressions = 0i64;
        let mut memory_usage = 0i64;

        let mut i = 0;
        while i + 1 < arr.len() {
            let key = Self::extract_resp2_string(&arr[i])?;
            let value = &arr[i + 1];

            match key.to_lowercase().as_str() {
                "compression" => compression = Self::extract_resp2_integer(value)?,
                "capacity" => capacity = Self::extract_resp2_integer(value)?,
                "merged nodes" | "merged_nodes" => merged_nodes = Self::extract_resp2_integer(value)?,
                "unmerged nodes" | "unmerged_nodes" => unmerged_nodes = Self::extract_resp2_integer(value)?,
                "merged weight" | "merged_weight" => merged_weight = Self::extract_resp2_float(value)?,
                "unmerged weight" | "unmerged_weight" => unmerged_weight = Self::extract_resp2_float(value)?,
                "total compressions" | "total_compressions" => total_compressions = Self::extract_resp2_integer(value)?,
                "memory usage" | "memory_usage" => memory_usage = Self::extract_resp2_integer(value)?,
                _ => {} // Ignore unknown fields
            }
            i += 2;
        }

        Ok(Self {
            compression,
            capacity,
            merged_nodes,
            unmerged_nodes,
            merged_weight,
            unmerged_weight,
            total_compressions,
            memory_usage,
        })
    }

    fn extract_resp2_string(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            Resp2Frame::SimpleString(s) => Ok(String::from_utf8(s.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_resp2_integer(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(*n),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_resp2_float(frame: &Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(*n as f64),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }

    fn extract_resp3_string(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_resp3_integer(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<i64>().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_resp3_float(frame: &Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(*data),
            Resp3Frame::Number { data, .. } => Ok(*data as f64),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<f64>().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }
}

impl Serialize for TdigestInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestInfoOutput", 8)?;
        state.serialize_field("compression", &self.compression)?;
        state.serialize_field("capacity", &self.capacity)?;
        state.serialize_field("merged_nodes", &self.merged_nodes)?;
        state.serialize_field("unmerged_nodes", &self.unmerged_nodes)?;
        state.serialize_field("merged_weight", &self.merged_weight)?;
        state.serialize_field("unmerged_weight", &self.unmerged_weight)?;
        state.serialize_field("total_compressions", &self.total_compressions)?;
        state.serialize_field("memory_usage", &self.memory_usage)?;
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
            let input = TdigestInfoInput { key: RedisKey::String("td".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.INFO"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_decode_output_resp2_array() {
            // RESP2 array format: [key, value, key, value, ...]
            let resp = b"*16\r\n\
                $11\r\nCompression\r\n:100\r\n\
                $8\r\nCapacity\r\n:610\r\n\
                $12\r\nMerged nodes\r\n:0\r\n\
                $14\r\nUnmerged nodes\r\n:3\r\n\
                $13\r\nMerged weight\r\n$1\r\n0\r\n\
                $15\r\nUnmerged weight\r\n$1\r\n3\r\n\
                $18\r\nTotal compressions\r\n:0\r\n\
                $12\r\nMemory usage\r\n:9768\r\n";

            let output = TdigestInfoOutput::decode(resp).unwrap();
            assert_eq!(output.compression(), 100);
            assert_eq!(output.capacity(), 610);
            assert_eq!(output.merged_nodes(), 0);
            assert_eq!(output.unmerged_nodes(), 3);
            assert_eq!(output.total_compressions(), 0);
            assert_eq!(output.memory_usage(), 9768);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestInfoOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TdigestInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("extra".into())];
            let err = TdigestInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestInfoInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_total_observations() {
            let output = TdigestInfoOutput::new(100, 610, 5, 3, 100.0, 50.0, 2, 9768);
            assert!((output.total_observations() - 150.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestInfoOutput::new(100, 610, 0, 3, 0.0, 3.0, 0, 9768);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("compression"));
            assert!(json.contains("100"));
            assert!(json.contains("capacity"));
            assert!(json.contains("610"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::t_digest::tdigest_add::TdigestAddInput;
        use crate::api::lib::t_digest::tdigest_create::TdigestCreateInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_info_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_info_test".into()),
                                compression: Some(RedisJsonValue::Integer(100)),
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    let result =
                        ctx.raw(&TdigestInfoInput { key: RedisKey::String("td_info_test".into()) }.command()).await.expect("raw failed");

                    let output = TdigestInfoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.compression(), 100);
                    assert!(output.capacity() > 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_info_after_add() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_info_add".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_info_add".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result =
                        ctx.raw(&TdigestInfoInput { key: RedisKey::String("td_info_add".into()) }.command()).await.expect("raw failed");

                    let output = TdigestInfoOutput::decode(&result).expect("decode failed");
                    // After adding 3 values, total observations should be 3
                    assert!((output.total_observations() - 3.0).abs() < f64::EPSILON);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_info_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TdigestInfoInput { key: RedisKey::String("nonexistent_td".into()) }.command()).await;

                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
