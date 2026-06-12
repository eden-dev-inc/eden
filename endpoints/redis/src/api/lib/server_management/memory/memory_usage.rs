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

const API_INFO: ApiInfo<RedisApi, MemoryUsageInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::MemoryUsage, "Estimates the memory usage of a key", ReqType::Read, true);

/// See official Redis documentation for `MEMORY USAGE`
/// https://redis.io/docs/latest/commands/memory-usage/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MemoryUsageInput {
    key: RedisKey,
    samples: Option<RedisJsonValue>,
}

impl Serialize for MemoryUsageInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.samples.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("MemoryUsageInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(samples) = &self.samples {
            state.serialize_field("samples", &samples)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    MemoryUsageInput,
    API_INFO,
    {key, samples}
);

impl RedisCommandInput for MemoryUsageInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(samples) = &self.samples {
            command.arg("SAMPLES").arg(samples);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("MEMORY USAGE requires at least 1 argument".to_string()));
        }

        let key = args[0].clone().try_into()?;
        let mut samples = None;

        if args.len() >= 3
            && let RedisJsonValue::String(s) = &args[1]
            && s.to_uppercase() == "SAMPLES"
        {
            samples = Some(args[2].clone());
        }

        Ok(Self { key, samples })
    }
}

/// Output for Redis MEMORY USAGE command
///
/// Returns the memory usage in bytes, or None if the key doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MemoryUsageOutput {
    /// Memory usage in bytes, or None if key doesn't exist
    bytes: Option<i64>,
}

impl MemoryUsageOutput {
    pub fn new(bytes: Option<i64>) -> Self {
        Self { bytes }
    }

    /// Get the memory usage in bytes
    pub fn bytes(&self) -> Option<i64> {
        self.bytes
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.bytes.is_some()
    }

    /// Get memory usage in kilobytes
    pub fn kilobytes(&self) -> Option<f64> {
        self.bytes.map(|b| b as f64 / 1024.0)
    }

    /// Decode the Redis protocol response into a MemoryUsageOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let usage = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => Some(i),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY USAGE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Some(data),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY USAGE response: {:?}", other)));
                }
            },
        };

        Ok(Self { bytes: usage })
    }
}

impl Serialize for MemoryUsageOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryUsageOutput", 1)?;
        state.serialize_field("bytes", &self.bytes)?;
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
            let input = MemoryUsageInput { key: RedisKey::String("mykey".into()), samples: None };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_with_samples() {
            let input = MemoryUsageInput {
                key: RedisKey::String("mykey".into()),
                samples: Some(RedisJsonValue::Integer(5)),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"SAMPLES"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = MemoryUsageOutput::decode(b":1024\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.bytes(), Some(1024));
            assert_eq!(output.kilobytes(), Some(1.0));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = MemoryUsageOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.bytes(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = MemoryUsageOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.bytes(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MemoryUsageOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = MemoryUsageInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.samples.is_none());
        }

        #[test]
        fn test_decode_input_with_samples() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("SAMPLES".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = MemoryUsageInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.samples, Some(RedisJsonValue::Integer(10)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = MemoryUsageInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = MemoryUsageInput { key: RedisKey::String("mykey".into()), samples: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind() {
            let input = MemoryUsageInput { key: RedisKey::String("mykey".into()), samples: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::MemoryUsage);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_usage_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MemoryUsageInput {
                                key: RedisKey::String("nonexistent_key".into()),
                                samples: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MemoryUsageOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                    assert_eq!(output.bytes(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_usage_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a key first
                    ctx.write(SetInput {
                        key: RedisKey::String("testkey".into()),
                        value: RedisJsonValue::String("testvalue".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(&MemoryUsageInput { key: RedisKey::String("testkey".into()), samples: None }.command())
                        .await
                        .expect("raw failed");

                    let output = MemoryUsageOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.bytes().unwrap() > 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_usage_with_samples() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a key
                    ctx.write(SetInput {
                        key: RedisKey::String("samplekey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &MemoryUsageInput {
                                key: RedisKey::String("samplekey".into()),
                                samples: Some(RedisJsonValue::Integer(0)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MemoryUsageOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_usage_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(&MemoryUsageInput { key: RedisKey::String("missing".into()), samples: None }.command())
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = MemoryUsageOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_usage_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(&MemoryUsageInput { key: RedisKey::String("missing".into()), samples: None }.command())
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = MemoryUsageOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());

            ctx.stop().await;
        }
    }
}
