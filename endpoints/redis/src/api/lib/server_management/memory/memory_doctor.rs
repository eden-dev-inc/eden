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

const API_INFO: ApiInfo<RedisApi, MemoryDoctorInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::MemoryDoctor, "Outputs a memory problems report", ReqType::Read, true);

/// See official Redis documentation for `MEMORY DOCTOR`
/// https://redis.io/docs/latest/commands/memory-doctor/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MemoryDoctorInput {}

impl Serialize for MemoryDoctorInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryDoctorInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MemoryDoctorInput, API_INFO);

impl RedisCommandInput for MemoryDoctorInput {
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
                "MEMORY DOCTOR expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis MEMORY DOCTOR command
///
/// Returns a diagnostic report about memory issues.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MemoryDoctorOutput {
    /// The diagnostic report string
    report: String,
}

impl MemoryDoctorOutput {
    pub fn new(report: String) -> Self {
        Self { report }
    }

    /// Get the diagnostic report
    pub fn report(&self) -> &str {
        &self.report
    }

    /// Check if no problems were detected
    pub fn is_healthy(&self) -> bool {
        self.report.contains("no memory problems") || self.report.to_lowercase().contains("sam, i have no memory problems")
    }

    /// Decode the Redis protocol response into a MemoryDoctorOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let report = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY DOCTOR response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MEMORY DOCTOR response: {:?}", other)));
                }
            },
        };

        Ok(Self { report })
    }
}

impl Serialize for MemoryDoctorOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MemoryDoctorOutput", 1)?;
        state.serialize_field("report", &self.report)?;
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
            let input = MemoryDoctorInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nMEMORY\r\n$6\r\nDOCTOR\r\n");
        }

        #[test]
        fn test_decode_bulk_string_healthy() {
            // "Sam, I have no memory problems" is 30 characters
            let response = b"$30\r\nSam, I have no memory problems\r\n";
            let output = MemoryDoctorOutput::decode(response).unwrap();
            assert!(output.is_healthy());
            assert!(output.report().contains("no memory problems"));
        }

        #[test]
        fn test_decode_bulk_string_with_issues() {
            // "High fragmentation detected. Consider restarting." is 49 characters
            let response = b"$49\r\nHigh fragmentation detected. Consider restarting.\r\n";
            let output = MemoryDoctorOutput::decode(response).unwrap();
            assert!(!output.is_healthy());
            assert!(output.report().contains("fragmentation"));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MemoryDoctorOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = MemoryDoctorInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns() {
            let input = MemoryDoctorInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MemoryDoctorInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = MemoryDoctorInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::MemoryDoctor);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_doctor_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryDoctorInput {}.command()).await.expect("raw failed");

                    let output = MemoryDoctorOutput::decode(&result).expect("decode failed");
                    // Fresh Redis instance should be healthy
                    assert!(!output.report().is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_doctor_returns_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MemoryDoctorInput {}.command()).await.expect("raw failed");

                    let output = MemoryDoctorOutput::decode(&result).expect("decode failed");
                    // Should return some diagnostic text
                    assert!(!output.report().is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_doctor_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&MemoryDoctorInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = MemoryDoctorOutput::decode(&result).expect("decode failed");
            assert!(!output.report().is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_memory_doctor_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&MemoryDoctorInput {}.command()).await.expect("raw failed");

            let output = MemoryDoctorOutput::decode(&result).expect("decode failed");
            assert!(!output.report().is_empty());

            ctx.stop().await;
        }
    }
}
