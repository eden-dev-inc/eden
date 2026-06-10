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

const API_INFO: ApiInfo<RedisApi, PfselftestInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pfselftest,
    "An internal command for testing HyperLogLog values",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PFSELFTEST`
/// https://redis.io/docs/latest/commands/pfselftest/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PfselftestInput {}

impl Serialize for PfselftestInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PfselftestInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(PfselftestInput, API_INFO);

impl RedisCommandInput for PfselftestInput {
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
                "PFSELFTEST expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis PFSELFTEST command
///
/// PFSELFTEST is an internal debugging command that runs a self-test of the
/// HyperLogLog implementation. It returns OK if the test passes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PfselftestOutput {
    /// Whether the self-test passed
    success: bool,
}

impl PfselftestOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if the self-test passed
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a PfselftestOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected PFSELFTEST response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected PFSELFTEST response: {:?}", other))),
            },
        }
    }
}

impl Serialize for PfselftestOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PfselftestOutput", 1)?;
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
        fn test_encode_command() {
            let input = PfselftestInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$10\r\nPFSELFTEST\r\n");
        }

        #[test]
        fn test_decode_output_ok() {
            let output = PfselftestOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PfselftestOutput::decode(b"-ERR self test failed\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = PfselftestInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_decode_input_with_args_warns_but_succeeds() {
            // PFSELFTEST should warn but still succeed if given args
            let args = vec![RedisJsonValue::String("unexpected".into())];
            let input = PfselftestInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PfselftestInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = PfselftestInput::default();
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfselftest_passes() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PfselftestInput {}.command()).await.expect("raw failed");

                    let output = PfselftestOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success(), "PFSELFTEST should pass");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfselftest_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PfselftestInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = PfselftestOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfselftest_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PfselftestInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = PfselftestOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfselftest_multiple_calls() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // PFSELFTEST should be idempotent
                    for _ in 0..3 {
                        let result = ctx.raw(&PfselftestInput {}.command()).await.expect("raw failed");

                        let output = PfselftestOutput::decode(&result).expect("decode failed");
                        assert!(output.is_success());
                    }
                })
            })
            .await;
        }
    }
}
