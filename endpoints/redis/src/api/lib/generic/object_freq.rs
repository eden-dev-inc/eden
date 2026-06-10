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

const API_INFO: ApiInfo<RedisApi, ObjectFreqInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ObjectFreq,
    "Returns the logarithmic access frequency counter of a Redis object. Only works when maxmemory-policy is set to an LFU policy.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `OBJECT FREQ`
/// https://redis.io/docs/latest/commands/object-freq/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ObjectFreqInput {
    pub(crate) key: RedisKey,
}

impl Serialize for ObjectFreqInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ObjectFreqInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ObjectFreqInput, API_INFO, { key });

impl RedisCommandInput for ObjectFreqInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        // OBJECT FREQ is a subcommand: OBJECT FREQ <key>
        let mut command = crate::command::cmd("OBJECT");
        command.arg("FREQ");
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("OBJECT FREQ requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "OBJECT FREQ takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis OBJECT FREQ command
///
/// Returns the logarithmic access frequency counter of the object stored at the key.
/// Only available when maxmemory-policy is set to an LFU policy.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ObjectFreqOutput {
    /// The logarithmic access frequency counter (0-255)
    frequency: Option<i64>,
}

impl ObjectFreqOutput {
    pub fn new(frequency: Option<i64>) -> Self {
        Self { frequency }
    }
}

impl Serialize for ObjectFreqOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ObjectFreqOutput", 1)?;
        state.serialize_field("frequency", &self.frequency)?;
        state.end()
    }
}

impl ObjectFreqOutput {
    /// Get the frequency counter value
    pub fn frequency(&self) -> Option<i64> {
        self.frequency
    }

    /// Check if the key exists and has a frequency value
    pub fn exists(&self) -> bool {
        self.frequency.is_some()
    }

    /// Decode the Redis protocol response into an ObjectFreqOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let frequency = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => Some(i),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT FREQ response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected OBJECT FREQ response: {:?}", other)));
                }
            },
        };

        Ok(Self { frequency })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ObjectFreqInput { key: RedisKey::String("mykey".into()) };
            // OBJECT FREQ mykey => *3\r\n$6\r\nOBJECT\r\n$4\r\nFREQ\r\n$5\r\nmykey\r\n
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nOBJECT\r\n$4\r\nFREQ\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_integer() {
            // RESP2 integer: :42\r\n
            let output = ObjectFreqOutput::decode(b":42\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.frequency(), Some(42));
        }

        #[test]
        fn test_decode_zero_frequency() {
            let output = ObjectFreqOutput::decode(b":0\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.frequency(), Some(0));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = ObjectFreqOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.frequency(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ObjectFreqOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.frequency(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ObjectFreqOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_lfu_not_enabled_error() {
            // Error when LFU policy is not enabled
            let err =
                ObjectFreqOutput::decode(b"-ERR An LFU maxmemory policy is not selected, access frequency not tracked.\r\n").unwrap_err();
            assert!(err.to_string().contains("LFU"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_freq_nonexistent_key() {
            // OBJECT FREQ requires Redis 4.0+
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ObjectFreqInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    // Non-existent key returns null or error depending on Redis version
                    let output = ObjectFreqOutput::decode(&result);
                    // Either null response or error is acceptable for missing key
                    if let Ok(o) = output {
                        assert!(!o.exists());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_freq_requires_lfu_policy() {
            // This test verifies the command works but may error without LFU policy
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    // Set a key first
                    ctx.write(SetInput {
                        key: RedisKey::String("freqtest".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ObjectFreqInput { key: RedisKey::String("freqtest".into()) }.command()).await.expect("raw failed");

                    let output = ObjectFreqOutput::decode(&result);
                    // Without LFU policy, this will error; with LFU, returns integer
                    // Both are valid outcomes depending on server config
                    match output {
                        Ok(o) => {
                            // LFU enabled - should have a frequency value
                            assert!(o.exists());
                            assert!(o.frequency().unwrap() >= 0);
                        }
                        Err(e) => {
                            // LFU not enabled - should get specific error
                            assert!(e.to_string().contains("LFU") || e.to_string().contains("ERR"));
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_freq_command_format() {
            // Verify the command is properly formatted as OBJECT FREQ
            let input = ObjectFreqInput { key: RedisKey::String("testkey".into()) };
            let cmd = input.command();

            // Parse to verify structure
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("OBJECT"), "Command should contain OBJECT");
            assert!(cmd_str.contains("FREQ"), "Command should contain FREQ");
            assert!(cmd_str.starts_with("*3\r\n"), "Should be array of 3 elements");
        }
    }
}
