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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, LastsaveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lastsave,
    "Returns the Unix timestamp of the last successful save to disk",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LASTSAVE`
/// https://redis.io/docs/latest/commands/lastsave/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LastsaveInput {}

impl Serialize for LastsaveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LastsaveInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(LastsaveInput, API_INFO);

impl RedisCommandInput for LastsaveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "LASTSAVE expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis LASTSAVE command
///
/// Returns the Unix timestamp of the last successful save to disk.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LastsaveOutput {
    /// Unix timestamp of the last save
    timestamp: i64,
}

impl Serialize for LastsaveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LastsaveOutput", 1)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl LastsaveOutput {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// Get the Unix timestamp of the last save
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let timestamp = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("LASTSAVE must return integer, got: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("LASTSAVE must return number, got: {:?}", other)));
                }
            },
        };

        Ok(Self::new(timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = LastsaveInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$8\r\nLASTSAVE\r\n");
        }

        #[test]
        fn test_decode_output_resp2_integer() {
            let output = LastsaveOutput::decode(b":1700000000\r\n").unwrap();
            assert_eq!(output.timestamp(), 1700000000);
        }

        #[test]
        fn test_decode_output_zero() {
            // Zero timestamp indicates no save has occurred
            let output = LastsaveOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.timestamp(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = LastsaveOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = LastsaveInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_still_succeeds() {
            // Extra args are logged but don't cause failure
            let input = LastsaveInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LastsaveInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LastsaveInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Lastsave);
        }

        #[test]
        fn test_serialize_input() {
            let input = LastsaveInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("LASTSAVE") || json.contains("Lastsave"));
        }

        #[test]
        fn test_serialize_output() {
            let output = LastsaveOutput::new(1700000000);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"timestamp\":1700000000"));
        }

        #[test]
        fn test_req_type_is_read() {
            assert_eq!(API_INFO.request_type, ReqType::Read);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lastsave_returns_timestamp() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&LastsaveInput {}.command()).await.expect("raw failed");

                    let output = LastsaveOutput::decode(&result).expect("decode failed");

                    // Timestamp should be reasonable (after year 2000)
                    assert!(output.timestamp() >= 946684800, "timestamp should be after year 2000");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lastsave_timestamp_increases_after_bgsave() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Get initial timestamp
                    let result1 = ctx.raw(&LastsaveInput {}.command()).await.expect("raw failed");
                    let output1 = LastsaveOutput::decode(&result1).expect("decode failed");
                    let ts1 = output1.timestamp();

                    // Wait a moment and trigger BGSAVE
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    let _ = ctx.raw(b"*1\r\n$6\r\nBGSAVE\r\n").await;

                    // Wait for BGSAVE to complete
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                    // Get new timestamp
                    let result2 = ctx.raw(&LastsaveInput {}.command()).await.expect("raw failed");
                    let output2 = LastsaveOutput::decode(&result2).expect("decode failed");
                    let ts2 = output2.timestamp();

                    // Timestamp should be >= the original (may be same if save completed in same second)
                    assert!(ts2 >= ts1, "timestamp should not decrease after BGSAVE");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lastsave_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&LastsaveInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = LastsaveOutput::decode(&result).expect("decode failed");
            assert!(output.timestamp() > 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lastsave_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&LastsaveInput {}.command()).await.expect("raw failed");

            let output = LastsaveOutput::decode(&result).expect("decode failed");
            assert!(output.timestamp() > 0);

            ctx.stop().await;
        }
    }
}
