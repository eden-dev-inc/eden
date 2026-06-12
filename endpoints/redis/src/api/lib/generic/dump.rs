use crate::api::RedisCommandOutput;
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

const API_INFO: ApiInfo<RedisApi, DumpInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Dump,
    "Returns a serialized representation of the value stored at a key",
    ReqType::Read, // Fixed: DUMP is a read operation
    true,
);

/// See official Redis documentation for `DUMP`
/// https://redis.io/docs/latest/commands/dump/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DumpInput {
    pub(crate) key: RedisKey,
}

impl Serialize for DumpInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DumpInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(DumpInput, API_INFO, { key });

impl RedisCommandInput for DumpInput {
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
            return Err(EpError::parse("DUMP requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "DUMP takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis DUMP command
///
/// Returns the serialized value if the key exists, or None if the key does not exist.
/// The serialized format is opaque and can be used with RESTORE.
#[derive(Debug, Clone, ToSchema, JsonSchema)]
pub struct DumpOutput {
    /// The serialized value, or None if key doesn't exist
    value: Option<Vec<u8>>,
}

impl DumpOutput {
    pub fn new(value: Option<Vec<u8>>) -> Self {
        Self { value }
    }

    /// Get the serialized value from the output
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Check if the key exists (value is Some)
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }
}

impl Serialize for DumpOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("DumpOutput", 1)?;
        // Serialize bytes as base64 for JSON compatibility
        match &self.value {
            Some(bytes) => {
                use base64::{Engine, engine::general_purpose::STANDARD};
                state.serialize_field("value", &STANDARD.encode(bytes))?;
            }
            None => {
                state.serialize_field("value", &None::<String>)?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for DumpOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            value: Option<String>,
        }
        let helper = Helper::deserialize(deserializer)?;
        let value = match helper.value {
            Some(s) => {
                use base64::{Engine, engine::general_purpose::STANDARD};
                Some(STANDARD.decode(&s).map_err(serde::de::Error::custom)?)
            }
            None => None,
        };
        Ok(DumpOutput { value })
    }
}

impl RedisCommandOutput for DumpOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Dump
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Some(data),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected DUMP response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(data),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected DUMP response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = DumpInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nDUMP\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = DumpOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(b"hello".as_slice()));
        }

        #[test]
        fn test_decode_binary_data() {
            // Test with binary data containing null bytes
            let binary = b"$6\r\n\x00\x01\x02\x03\x04\x05\r\n";
            let output = DumpOutput::decode(binary).unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some([0u8, 1, 2, 3, 4, 5].as_slice()));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = DumpOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = DumpOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = DumpOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_serialization_roundtrip() {
            let output = DumpOutput::new(Some(vec![0, 1, 2, 255]));
            let json = serde_json::to_string(&output).unwrap();
            let decoded: DumpOutput = serde_json::from_str(&json).unwrap();
            assert_eq!(output.value(), decoded.value());
        }

        #[test]
        fn test_serialization_none() {
            let output = DumpOutput::new(None);
            let json = serde_json::to_string(&output).unwrap();
            let decoded: DumpOutput = serde_json::from_str(&json).unwrap();
            assert!(!decoded.exists());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::lib::generic::restore::RestoreInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&DumpInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = DumpOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return null");
                    assert_eq!(output.value(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("dumpkey".into()),
                        value: RedisJsonValue::String("dumpvalue".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&DumpInput { key: RedisKey::String("dumpkey".into()) }.command()).await.expect("raw failed");

                    let output = DumpOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    // DUMP returns RDB-serialized data, not the raw value
                    assert!(!output.value().unwrap().is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_restore_roundtrip() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a value
                    ctx.write(SetInput {
                        key: RedisKey::String("original".into()),
                        value: RedisJsonValue::String("test_value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Dump it
                    let dump_result = ctx.raw(&DumpInput { key: RedisKey::String("original".into()) }.command()).await.expect("raw failed");

                    let dump_output = DumpOutput::decode(&dump_result).expect("decode failed");
                    assert!(dump_output.exists());

                    let serialized = dump_output.value().unwrap().to_vec();

                    // Restore to a new key
                    ctx.write(RestoreInput {
                        key: RedisKey::String("restored".into()),
                        ttl: RedisJsonValue::from(0),
                        serialized_value: serialized,
                        replace: None,
                        absttl: None,
                        idletime: None,
                        freq: None,
                    })
                    .await;

                    // Verify the restored value matches
                    let get_result = ctx
                        .raw(&crate::api::lib::string::get::GetInput { key: RedisKey::String("restored".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = crate::api::lib::string::get::GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("test_value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_pipeline_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("d1".into()),
                        value: RedisJsonValue::String("val1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("d2".into()),
                        value: RedisJsonValue::String("val2".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&DumpInput { key: RedisKey::String("d1".into()) }.command());
                    pipeline.extend_from_slice(&DumpInput { key: RedisKey::String("missing".into()) }.command());
                    pipeline.extend_from_slice(&DumpInput { key: RedisKey::String("d2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = DumpOutput::decode(responses[0]).expect("decode d1");
                    assert!(out1.exists());

                    let out2 = DumpOutput::decode(responses[1]).expect("decode missing");
                    assert!(!out2.exists());

                    let out3 = DumpOutput::decode(responses[2]).expect("decode d2");
                    assert!(out3.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&DumpInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = DumpOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dump_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&DumpInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = DumpOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
