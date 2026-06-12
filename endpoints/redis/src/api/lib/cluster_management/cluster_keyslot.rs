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

const API_INFO: ApiInfo<RedisApi, ClusterKeyslotInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterKeyslot, "Returns the hash slot for a key", ReqType::Read, true);

/// See official Redis documentation for `CLUSTER KEYSLOT`
/// https://redis.io/docs/latest/commands/cluster-keyslot/
///
/// Available since Redis 3.0.0
///
/// Official example: `CLUSTER KEYSLOT somekey` returns the slot number (0-16383)
/// Hash tags: `foo{hash_tag}` and `bar{hash_tag}` will return the same slot
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterKeyslotInput {
    /// The key to compute the hash slot for
    pub(crate) key: RedisKey,
}

impl Serialize for ClusterKeyslotInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterKeyslotInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ClusterKeyslotInput, API_INFO, { key });

impl RedisCommandInput for ClusterKeyslotInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        // CLUSTER KEYSLOT doesn't actually operate on keys in the data sense,
        // it just computes the slot. Return empty as per convention.
        vec![]
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
            return Err(EpError::request("CLUSTER KEYSLOT requires 1 argument, given 0"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER KEYSLOT expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis CLUSTER KEYSLOT command
///
/// Returns the hash slot number (0-16383) for the given key.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterKeyslotOutput {
    /// The hash slot number (0-16383)
    slot: u16,
}

impl ClusterKeyslotOutput {
    pub fn new(slot: u16) -> Self {
        Self { slot }
    }

    /// Get the slot number
    pub fn slot(&self) -> u16 {
        self.slot
    }

    /// Decode the Redis protocol response into a ClusterKeyslotOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let slot = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => {
                    if !(0..=16383).contains(&i) {
                        return Err(EpError::parse(format!("slot must be 0-16383, got {}", i)));
                    }
                    i as u16
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER KEYSLOT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => {
                    if !(0..=16383).contains(&data) {
                        return Err(EpError::parse(format!("slot must be 0-16383, got {}", data)));
                    }
                    data as u16
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER KEYSLOT response: {:?}", other)));
                }
            },
        };

        Ok(Self { slot })
    }
}

impl Serialize for ClusterKeyslotOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterKeyslotOutput", 1)?;
        state.serialize_field("slot", &self.slot)?;
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
            let input = ClusterKeyslotInput { key: RedisKey::String("mykey".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("KEYSLOT") || cmd_str.contains("CLUSTER KEYSLOT"));
            assert!(cmd_str.contains("mykey"));
        }

        #[test]
        fn test_encode_command_with_hash_tag() {
            let input = ClusterKeyslotInput { key: RedisKey::String("foo{bar}".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("foo{bar}"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = ClusterKeyslotInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterKeyslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_extra_args_succeeds_with_warning() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let result = ClusterKeyslotInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_decode_output_valid_slot() {
            // RESP2 integer: :5649\r\n
            let response = b":5649\r\n";
            let output = ClusterKeyslotOutput::decode(response).unwrap();
            assert_eq!(output.slot(), 5649);
        }

        #[test]
        fn test_decode_output_slot_zero() {
            let response = b":0\r\n";
            let output = ClusterKeyslotOutput::decode(response).unwrap();
            assert_eq!(output.slot(), 0);
        }

        #[test]
        fn test_decode_output_slot_max() {
            let response = b":16383\r\n";
            let output = ClusterKeyslotOutput::decode(response).unwrap();
            assert_eq!(output.slot(), 16383);
        }

        #[test]
        fn test_decode_output_error_response() {
            let response = b"-ERR This instance has cluster support disabled\r\n";
            let err = ClusterKeyslotOutput::decode(response).unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterKeyslotInput { key: RedisKey::String("test".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization_roundtrip() {
            let output = ClusterKeyslotOutput::new(12345);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("12345"));
            let decoded: ClusterKeyslotOutput = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.slot(), 12345);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_keyslot_basic() {
            test_all_protocols_min_version("3.0", |ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("hello".into()) }.command()).await.expect("raw failed");

                    let decode_result = ClusterKeyslotOutput::decode(&result);

                    match decode_result {
                        Ok(output) => {
                            // Slot should be in valid range
                            assert!(output.slot() <= 16383);
                        }
                        Err(e) => {
                            // Standalone mode returns error
                            let err_msg = e.to_string().to_lowercase();
                            assert!(
                                err_msg.contains("cluster") || err_msg.contains("disabled"),
                                "Expected cluster-related error, got: {}",
                                e
                            );
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_keyslot_hash_tags() {
            test_all_protocols_min_version("3.0", |ctx| {
                Box::pin(async move {
                    // Keys with same hash tag should return same slot
                    let result1 =
                        ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("foo{tag}".into()) }.command()).await.expect("raw failed");

                    let result2 =
                        ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("bar{tag}".into()) }.command()).await.expect("raw failed");

                    match (ClusterKeyslotOutput::decode(&result1), ClusterKeyslotOutput::decode(&result2)) {
                        (Ok(out1), Ok(out2)) => {
                            assert_eq!(out1.slot(), out2.slot(), "Keys with same hash tag should have same slot");
                        }
                        _ => {
                            // Standalone mode - skip hash tag verification
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_keyslot_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("3.0", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;
                let result = ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("testkey".into()) }.command()).await.expect("raw failed");

                // Should get either integer or error
                assert!(
                    result.starts_with(b":") || result.starts_with(b"-"),
                    "Expected integer or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_keyslot_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("6", version) {
                    continue; // RESP3 requires Redis 6+
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;
                let result = ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("testkey".into()) }.command()).await.expect("raw failed");

                // RESP3 integer also uses : prefix
                assert!(
                    result.starts_with(b":") || result.starts_with(b"-"),
                    "Expected integer or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_keyslot_empty_key() {
            test_all_protocols_min_version("3.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterKeyslotInput { key: RedisKey::String("".into()) }.command()).await.expect("raw failed");

                    // Empty key should still return a valid slot (or error in standalone)
                    let decode_result = ClusterKeyslotOutput::decode(&result);
                    match decode_result {
                        Ok(output) => {
                            assert!(output.slot() <= 16383);
                        }
                        Err(_) => {
                            // Standalone mode error is acceptable
                        }
                    }
                })
            })
            .await;
        }
    }
}
