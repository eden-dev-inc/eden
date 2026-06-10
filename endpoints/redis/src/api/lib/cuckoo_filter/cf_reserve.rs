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

const API_INFO: ApiInfo<RedisApi, CfReserveInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::CfReserve, "Creates a new Cuckoo Filter", ReqType::Write, true);

/// Input for Redis `CF.RESERVE` command.
///
/// Creates an empty Cuckoo Filter with a given capacity.
///
/// See official Redis documentation for `CF.RESERVE`:
/// https://redis.io/docs/latest/commands/cf.reserve/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfReserveInput {
    /// The name of the Cuckoo Filter to create
    key: RedisKey,
    /// Estimated capacity of the filter (number of unique items)
    capacity: RedisJsonValue,
    /// Number of items in each bucket (default: 2)
    bucket_size: Option<RedisJsonValue>,
    /// Maximum number of relocations before declaring filter full (default: 20)
    max_iterations: Option<RedisJsonValue>,
    /// Expansion rate when filter is full (default: 1)
    expansion: Option<RedisJsonValue>,
}

impl CfReserveInput {
    pub fn new(key: impl Into<RedisKey>, capacity: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            capacity: capacity.into(),
            bucket_size: None,
            max_iterations: None,
            expansion: None,
        }
    }

    pub fn with_bucket_size(mut self, bucket_size: impl Into<RedisJsonValue>) -> Self {
        self.bucket_size = Some(bucket_size.into());
        self
    }

    pub fn with_max_iterations(mut self, max_iterations: impl Into<RedisJsonValue>) -> Self {
        self.max_iterations = Some(max_iterations.into());
        self
    }

    pub fn with_expansion(mut self, expansion: impl Into<RedisJsonValue>) -> Self {
        self.expansion = Some(expansion.into());
        self
    }
}

impl Serialize for CfReserveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, capacity
        if self.bucket_size.is_some() {
            fields += 1;
        }
        if self.max_iterations.is_some() {
            fields += 1;
        }
        if self.expansion.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CfReserveInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("capacity", &self.capacity)?;
        if let Some(bucket_size) = &self.bucket_size {
            state.serialize_field("bucket_size", bucket_size)?;
        }
        if let Some(max_iterations) = &self.max_iterations {
            state.serialize_field("max_iterations", max_iterations)?;
        }
        if let Some(expansion) = &self.expansion {
            state.serialize_field("expansion", expansion)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    CfReserveInput,
    API_INFO,
    { key, capacity, bucket_size, max_iterations, expansion }
);

impl RedisCommandInput for CfReserveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.capacity);

        if let Some(bucket_size) = &self.bucket_size {
            command.arg("BUCKETSIZE").arg(bucket_size);
        }

        if let Some(max_iterations) = &self.max_iterations {
            command.arg("MAXITERATIONS").arg(max_iterations);
        }

        if let Some(expansion) = &self.expansion {
            command.arg("EXPANSION").arg(expansion);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("CF.RESERVE requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let capacity = args[1].clone();

        let mut bucket_size = None;
        let mut max_iterations = None;
        let mut expansion = None;

        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "BUCKETSIZE" if i + 1 < args.len() => {
                        bucket_size = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "MAXITERATIONS" if i + 1 < args.len() => {
                        max_iterations = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "EXPANSION" if i + 1 < args.len() => {
                        expansion = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown option: {}", cmd)));
                    }
                }
            } else {
                return Err(EpError::parse("Expected option keyword"));
            }
        }

        Ok(Self { key, capacity, bucket_size, max_iterations, expansion })
    }
}

/// Output for Redis `CF.RESERVE` command.
///
/// Returns OK if the filter was created successfully.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfReserveOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl CfReserveOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if the filter was created successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a CfReserveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CF.RESERVE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8_lossy(&data);
                    if s.to_uppercase() == "OK" {
                        Ok(Self { success: true })
                    } else {
                        Err(EpError::parse(format!("unexpected response: {}", s)))
                    }
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CF.RESERVE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for CfReserveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfReserveOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = CfReserveInput::new("myfilter", 1000i64);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.RESERVE"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("1000"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = CfReserveInput::new("myfilter", 1000i64).with_bucket_size(4i64).with_max_iterations(500i64).with_expansion(2i64);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.RESERVE"));
            assert!(cmd_str.contains("BUCKETSIZE"));
            assert!(cmd_str.contains("MAXITERATIONS"));
            assert!(cmd_str.contains("EXPANSION"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfReserveInput::new("filter1", 500i64);
            assert_eq!(input.key, RedisKey::String("filter1".into()));
            assert!(input.bucket_size.is_none());
            assert!(input.max_iterations.is_none());
            assert!(input.expansion.is_none());
        }

        #[test]
        fn test_builder_methods() {
            let input = CfReserveInput::new("filter1", 1000i64).with_bucket_size(4i64).with_expansion(2i64);

            assert!(input.bucket_size.is_some());
            assert!(input.max_iterations.is_none());
            assert!(input.expansion.is_some());
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfReserveInput::new("testfilter", 100i64);
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::Integer(1000)];
            let input = CfReserveInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
            assert!(input.bucket_size.is_none());
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("BUCKETSIZE".into()),
                RedisJsonValue::Integer(4),
                RedisJsonValue::String("EXPANSION".into()),
                RedisJsonValue::Integer(2),
            ];
            let input = CfReserveInput::decode(args).unwrap();
            assert!(input.bucket_size.is_some());
            assert!(input.expansion.is_some());
            assert!(input.max_iterations.is_none());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("UNKNOWN".into()),
                RedisJsonValue::Integer(1),
            ];
            let err = CfReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown option"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = CfReserveOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfReserveOutput::decode(b"-ERR item exists\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfReserveOutput::new(true);
            assert!(output.is_ok());

            let output = CfReserveOutput::new(false);
            assert!(!output.is_ok());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_reserve_basic() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // Use unique key to avoid conflicts
            let key = format!(
                "cf_reserve_test_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            let result = ctx.raw(&CfReserveInput::new(&key, 1000i64).command()).await;

            match result {
                Ok(bytes) => {
                    let output = CfReserveOutput::decode(&bytes).expect("decode failed");
                    assert!(output.is_ok());
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
        async fn test_cf_reserve_with_options() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_reserve_opts_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            let result = ctx.raw(&CfReserveInput::new(&key, 1000i64).with_bucket_size(4i64).with_expansion(2i64).command()).await;

            match result {
                Ok(bytes) => {
                    let output = CfReserveOutput::decode(&bytes).expect("decode failed");
                    assert!(output.is_ok());
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
        async fn test_cf_reserve_already_exists() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_reserve_dup_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            // Create first time
            let result = ctx.raw(&CfReserveInput::new(&key, 1000i64).command()).await;

            match result {
                Ok(_) => {
                    // Try to create again - should fail
                    let result2 = ctx.raw(&CfReserveInput::new(&key, 1000i64).command()).await;

                    match result2 {
                        Ok(bytes) => {
                            // Should be an error response
                            let decode_result = CfReserveOutput::decode(&bytes);
                            assert!(decode_result.is_err());
                        }
                        Err(_) => {
                            // Error is expected
                        }
                    }
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
    }
}
