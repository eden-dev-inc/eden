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
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, BfScandumpInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BfScandump,
    "Begins an incremental save of the Bloom Filter",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BF.SCANDUMP`
/// https://redis.io/docs/latest/commands/bf.scandump/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfScandumpInput {
    pub(crate) key: RedisKey,
    pub(crate) iterator: RedisJsonValue,
}

impl Serialize for BfScandumpInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfScandumpInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("iterator", &self.iterator)?;
        state.end()
    }
}

impl_redis_operation!(BfScandumpInput, API_INFO, { key, iterator });

impl RedisCommandInput for BfScandumpInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.iterator);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("BF.SCANDUMP requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.SCANDUMP expects 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, iterator: args[1].clone() })
    }
}

/// Output for Redis BF.SCANDUMP command
///
/// Returns an array of [iterator, data]. When iterator is 0, the dump is complete.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfScandumpOutput {
    /// The iterator value for the next call (0 means complete)
    pub iterator: i64,
    /// The chunk data (empty when complete)
    pub data: Option<Vec<u8>>,
}

impl BfScandumpOutput {
    pub fn new(iterator: i64, data: Option<Vec<u8>>) -> Self {
        Self { iterator, data }
    }

    /// Get the iterator value
    pub fn iterator(&self) -> i64 {
        self.iterator
    }

    /// Get the chunk data
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Check if the dump is complete
    pub fn is_complete(&self) -> bool {
        self.iterator == 0
    }

    /// Decode the Redis protocol response into a BfScandumpOutput
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
                    return Err(EpError::parse(format!("BF.SCANDUMP expected 2-element array, got {}", arr.len())));
                }

                let iterator = match &arr[0] {
                    Resp2Frame::Integer(i) => *i,
                    other => {
                        return Err(EpError::parse(format!("unexpected iterator type: {:?}", other)));
                    }
                };

                let data = match &arr[1] {
                    Resp2Frame::BulkString(bytes) => Some(bytes.clone()),
                    Resp2Frame::Null => None,
                    other => {
                        return Err(EpError::parse(format!("unexpected data type: {:?}", other)));
                    }
                };

                Ok(Self { iterator, data })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected BF.SCANDUMP response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data: arr, .. } => {
                if arr.len() != 2 {
                    return Err(EpError::parse(format!("BF.SCANDUMP expected 2-element array, got {}", arr.len())));
                }

                let iterator = match &arr[0] {
                    Resp3Frame::Number { data, .. } => *data,
                    other => {
                        return Err(EpError::parse(format!("unexpected iterator type: {:?}", other)));
                    }
                };

                let data = match &arr[1] {
                    Resp3Frame::BlobString { data, .. } => Some(data.clone()),
                    Resp3Frame::Null => None,
                    other => {
                        return Err(EpError::parse(format!("unexpected data type: {:?}", other)));
                    }
                };

                Ok(Self { iterator, data })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected BF.SCANDUMP response: {:?}", other))),
        }
    }
}

impl Serialize for BfScandumpOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfScandumpOutput", 2)?;
        state.serialize_field("iterator", &self.iterator)?;
        // Serialize data as base64 or null
        if let Some(data) = &self.data {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            state.serialize_field("data", &STANDARD.encode(data))?;
        } else {
            state.serialize_field::<Option<String>>("data", &None)?;
        }
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
            let input = BfScandumpInput {
                key: RedisKey::String("myfilter".into()),
                iterator: RedisJsonValue::Integer(0),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$11\r\nBF.SCANDUMP\r\n"));
        }

        #[test]
        fn test_decode_array_with_data() {
            // *2\r\n:1\r\n$4\r\ndata\r\n
            let output = BfScandumpOutput::decode(b"*2\r\n:1\r\n$4\r\ndata\r\n").unwrap();
            assert_eq!(output.iterator(), 1);
            assert!(!output.is_complete());
            assert_eq!(output.data(), Some(b"data".as_slice()));
        }

        #[test]
        fn test_decode_complete() {
            // *2\r\n:0\r\n$-1\r\n (iterator 0, null data)
            let output = BfScandumpOutput::decode(b"*2\r\n:0\r\n$-1\r\n").unwrap();
            assert_eq!(output.iterator(), 0);
            assert!(output.is_complete());
            assert!(output.data().is_none());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfScandumpOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::Integer(0)];
            let input = BfScandumpInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let err = BfScandumpInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfScandumpInput {
                key: RedisKey::String("testkey".into()),
                iterator: RedisJsonValue::Integer(0),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_add::BfAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_scandump_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Create and populate filter first
                    ctx.raw(
                        &BfAddInput {
                            key: RedisKey::String("bf_scandump_test".into()),
                            item: RedisJsonValue::String("item1".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Start scandump
                    let result = ctx
                        .raw(
                            &BfScandumpInput {
                                key: RedisKey::String("bf_scandump_test".into()),
                                iterator: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfScandumpOutput::decode(&result).expect("decode failed");
                    // First call should return data
                    if !output.is_complete() {
                        assert!(output.data().is_some());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_scandump_iterate() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Create filter
                    ctx.raw(
                        &BfAddInput {
                            key: RedisKey::String("bf_scandump_iter".into()),
                            item: RedisJsonValue::String("item".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut iterator = 0i64;
                    let mut chunks = 0;

                    loop {
                        let result = ctx
                            .raw(
                                &BfScandumpInput {
                                    key: RedisKey::String("bf_scandump_iter".into()),
                                    iterator: RedisJsonValue::Integer(iterator),
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = BfScandumpOutput::decode(&result).expect("decode failed");
                        chunks += 1;

                        if output.is_complete() {
                            break;
                        }

                        iterator = output.iterator();

                        // Safety: prevent infinite loop
                        if chunks > 100 {
                            panic!("Too many iterations");
                        }
                    }

                    assert!(chunks >= 1);
                })
            })
            .await;
        }
    }
}
