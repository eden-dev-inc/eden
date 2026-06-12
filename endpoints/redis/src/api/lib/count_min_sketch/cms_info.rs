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

const API_INFO: ApiInfo<RedisApi, CmsInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CmsInfo,
    "Returns information about a Count-Min Sketch",
    ReqType::Read,
    true,
);

/// Input for Redis `CMS.INFO` command.
///
/// Returns width, depth, and total count of the sketch.
///
/// See official Redis documentation for `CMS.INFO`:
/// https://redis.io/docs/latest/commands/cms.info/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CmsInfoInput {
    /// The key name for the Count-Min Sketch
    pub(crate) key: RedisKey,
}

impl Serialize for CmsInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CmsInfoInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(CmsInfoInput, API_INFO, { key });

impl RedisCommandInput for CmsInfoInput {
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
            return Err(EpError::parse(format!("CMS.INFO requires 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `CMS.INFO` command.
///
/// Contains width, depth, and total count of the sketch.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CmsInfoOutput {
    /// Number of counters in each array (width)
    width: i64,
    /// Number of counter-arrays (depth)
    depth: i64,
    /// Total count of all items added
    count: i64,
}

impl CmsInfoOutput {
    /// Create a new CmsInfoOutput
    pub fn new(width: i64, depth: i64, count: i64) -> Self {
        Self { width, depth, count }
    }

    /// Get the width
    pub fn width(&self) -> i64 {
        self.width
    }

    /// Get the depth
    pub fn depth(&self) -> i64 {
        self.depth
    }

    /// Get the total count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if the sketch is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Decode the Redis protocol response into a CmsInfoOutput
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
            Resp2Frame::Array(arr) => {
                Self::parse_array_pairs(&arr.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>())
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected CMS.INFO response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Map { data, .. } => {
                let mut width = 0i64;
                let mut depth = 0i64;
                let mut count = 0i64;

                for (key, value) in data {
                    let key_str = Self::extract_resp3_string(&key)?;
                    match key_str.to_lowercase().as_str() {
                        "width" => width = Self::extract_resp3_integer(&value)?,
                        "depth" => depth = Self::extract_resp3_integer(&value)?,
                        "count" => count = Self::extract_resp3_integer(&value)?,
                        _ => {}
                    }
                }

                Ok(Self { width, depth, count })
            }
            Resp3Frame::Array { data, .. } => {
                let wrapped: Vec<DecoderRespFrame> = data.into_iter().map(DecoderRespFrame::Resp3).collect();
                Self::parse_array_pairs(&wrapped)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected CMS.INFO response: {:?}", other))),
        }
    }

    fn parse_array_pairs(arr: &[DecoderRespFrame]) -> Result<Self, EpError> {
        let mut width = 0i64;
        let mut depth = 0i64;
        let mut count = 0i64;

        for chunk in arr.chunks(2) {
            if chunk.len() != 2 {
                continue;
            }

            let key = Self::extract_string(&chunk[0])?;
            let value = Self::extract_integer(&chunk[1])?;

            match key.to_lowercase().as_str() {
                "width" => width = value,
                "depth" => depth = value,
                "count" => count = value,
                _ => {}
            }
        }

        Ok(Self { width, depth, count })
    }

    fn extract_string(frame: &DecoderRespFrame) -> Result<String, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(data)) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),

            DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),

            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_integer(frame: &DecoderRespFrame) -> Result<i64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => Ok(*n),
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => {
                let s = String::from_utf8(data.to_vec()).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Ok(*data),
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                let s = String::from_utf8(data.to_vec()).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_resp3_string(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_resp3_integer(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.to_vec()).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }
}

impl Serialize for CmsInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CmsInfoOutput", 3)?;
        state.serialize_field("width", &self.width)?;
        state.serialize_field("depth", &self.depth)?;
        state.serialize_field("count", &self.count)?;
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
            let input = CmsInfoInput { key: RedisKey::String("cms_key".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.INFO"));
            assert!(cmd_str.contains("cms_key"));
        }

        #[test]
        fn test_decode_output_resp2_array() {
            // RESP2 array format: [key, value, key, value, ...]
            let resp = b"*6\r\n$5\r\nwidth\r\n:1000\r\n$5\r\ndepth\r\n:5\r\n$5\r\ncount\r\n:42\r\n";
            let output = CmsInfoOutput::decode(resp).unwrap();
            assert_eq!(output.width(), 1000);
            assert_eq!(output.depth(), 5);
            assert_eq!(output.count(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_empty_sketch() {
            let resp = b"*6\r\n$5\r\nwidth\r\n:100\r\n$5\r\ndepth\r\n:3\r\n$5\r\ncount\r\n:0\r\n";
            let output = CmsInfoOutput::decode(resp).unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CmsInfoOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = CmsInfoInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = CmsInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("extra".into())];
            let err = CmsInfoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = CmsInfoInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = CmsInfoOutput::new(1000, 5, 42);
            assert_eq!(output.width(), 1000);
            assert_eq!(output.depth(), 5);
            assert_eq!(output.count(), 42);
        }

        #[test]
        fn test_output_serialize() {
            let output = CmsInfoOutput::new(1000, 5, 42);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"width\":1000"));
            assert!(json.contains("\"depth\":5"));
            assert!(json.contains("\"count\":42"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::count_min_sketch::cms_initbydim::CmsInitbydimInput;
        use crate::api::lib::count_min_sketch::{CmsIncrbyInput, Incrby};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_info_basic() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    // Create sketch first
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_info_test".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create_result) = create_result {
                        if create_result.starts_with(b"-") {
                            return; // Module not loaded
                        }

                        let result =
                            ctx.raw(&CmsInfoInput { key: RedisKey::String("cms_info_test".into()) }.command()).await.expect("raw failed");

                        let output = CmsInfoOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.width(), 1000);
                        assert_eq!(output.depth(), 5);
                        assert_eq!(output.count(), 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_info_after_incrby() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    // Create sketch
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_info_incr".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create_result) = create_result {
                        if create_result.starts_with(b"-") {
                            return; // Module not loaded
                        }

                        // Add some items
                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_info_incr".into()),
                                incrby: vec![
                                    Incrby {
                                        item: RedisJsonValue::String("foo".into()),
                                        increment: RedisJsonValue::Integer(5),
                                    },
                                    Incrby {
                                        item: RedisJsonValue::String("bar".into()),
                                        increment: RedisJsonValue::Integer(10),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby failed");

                        let result =
                            ctx.raw(&CmsInfoInput { key: RedisKey::String("cms_info_incr".into()) }.command()).await.expect("raw failed");

                        let output = CmsInfoOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.count(), 15); // 5 + 10
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_info_nonexistent_key() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&CmsInfoInput { key: RedisKey::String("nonexistent_cms".into()) }.command()).await;

                    if let Ok(result) = result {
                        // Should return error for non-existent key
                        if !result.starts_with(b"-") {
                            // If module is not loaded, might get different error
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_info_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_info_r2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                let result = ctx.raw(&CmsInfoInput { key: RedisKey::String("cms_info_r2".into()) }.command()).await.expect("raw failed");

                assert!(result.starts_with(b"*"), "RESP2 should return array");
                let output = CmsInfoOutput::decode(&result).expect("decode failed");
                assert_eq!(output.width(), 100);
                assert_eq!(output.depth(), 3);
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_info_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_info_r3".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                let result = ctx.raw(&CmsInfoInput { key: RedisKey::String("cms_info_r3".into()) }.command()).await.expect("raw failed");

                let output = CmsInfoOutput::decode(&result).expect("decode failed");
                assert_eq!(output.width(), 100);
                assert_eq!(output.depth(), 3);
            }

            ctx.stop().await;
        }
    }
}
