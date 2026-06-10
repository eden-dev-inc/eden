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

const API_INFO: ApiInfo<RedisApi, LatencyDoctorInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::LatencyDoctor,
    "Returns a human-readable latency analysis report",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LATENCY DOCTOR`
/// https://redis.io/docs/latest/commands/latency-doctor/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyDoctorInput {}

impl Serialize for LatencyDoctorInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyDoctorInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(LatencyDoctorInput, API_INFO);

impl RedisCommandInput for LatencyDoctorInput {
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
                "LATENCY DOCTOR expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis LATENCY DOCTOR command
///
/// Returns a human-readable latency analysis report.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyDoctorOutput {
    /// The human-readable latency analysis report
    report: String,
}

impl LatencyDoctorOutput {
    pub fn new(report: String) -> Self {
        Self { report }
    }

    /// Get the latency analysis report
    pub fn report(&self) -> &str {
        &self.report
    }

    /// Check if the report indicates no latency issues
    pub fn has_no_issues(&self) -> bool {
        self.report.contains("I have a few latency reports to show you") || self.report.contains("Dave, no latency spike")
    }

    /// Decode the Redis protocol response into a LatencyDoctorOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let report = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected LATENCY DOCTOR response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected LATENCY DOCTOR response: {:?}", other)));
                }
            },
        };

        Ok(Self { report })
    }
}

impl Serialize for LatencyDoctorOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyDoctorOutput", 1)?;
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
            let input = LatencyDoctorInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nLATENCY\r\n$6\r\nDOCTOR\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let report = "Dave, no latency spike was observed";
            let resp = format!("${}\r\n{}\r\n", report.len(), report);
            let output = LatencyDoctorOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.report(), report);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyDoctorOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = LatencyDoctorInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyDoctorInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyDoctorInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyDoctor);
        }

        #[test]
        fn test_has_no_issues() {
            let output = LatencyDoctorOutput::new("Dave, no latency spike was observed during the lifetime".into());
            assert!(output.has_no_issues());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_doctor_basic() {
            // LATENCY DOCTOR requires Redis 2.8.13+
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&LatencyDoctorInput {}.command()).await.expect("raw failed");

                    let output = LatencyDoctorOutput::decode(&result).expect("decode failed");
                    // Should return a non-empty report
                    assert!(!output.report().is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_doctor_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&LatencyDoctorInput {}.command()).await.expect("raw failed");

            // RESP2 should return bulk string
            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = LatencyDoctorOutput::decode(&result).expect("decode failed");
            assert!(!output.report().is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_doctor_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&LatencyDoctorInput {}.command()).await.expect("raw failed");

            let output = LatencyDoctorOutput::decode(&result).expect("decode failed");
            assert!(!output.report().is_empty());

            ctx.stop().await;
        }
    }
}
