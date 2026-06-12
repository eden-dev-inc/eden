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

const API_INFO: ApiInfo<RedisApi, PubsubNumpatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::PubsubNumpat,
    "Returns a count of unique pattern subscriptions",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PUBSUB NUMPAT`
/// https://redis.io/docs/latest/commands/pubsub-numpat/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PubsubNumpatInput {}

impl Serialize for PubsubNumpatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubNumpatInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(PubsubNumpatInput, API_INFO);

impl RedisCommandInput for PubsubNumpatInput {
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
                "PUBSUB NUMPAT expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis PUBSUB NUMPAT command
///
/// Returns the number of unique pattern subscriptions.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PubsubNumpatOutput {
    /// Number of unique pattern subscriptions
    count: i64,
}

impl PubsubNumpatOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of pattern subscriptions
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a PubsubNumpatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB NUMPAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PUBSUB NUMPAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for PubsubNumpatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PubsubNumpatOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = PubsubNumpatInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$6\r\nNUMPAT\r\n");
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let _input = PubsubNumpatInput::decode(args).unwrap();
            // Successfully created with no args
        }

        #[test]
        fn test_decode_input_with_args_logs_warning() {
            // This should succeed but log a warning
            let args = vec![RedisJsonValue::String("extra".into())];
            let _input = PubsubNumpatInput::decode(args).unwrap();
        }

        #[test]
        fn test_decode_output_zero() {
            let output = PubsubNumpatOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_nonzero() {
            let output = PubsubNumpatOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.count(), 42);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PubsubNumpatOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = PubsubNumpatInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let _input = PubsubNumpatInput::default();
            // Should work with default
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numpat_no_patterns() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PubsubNumpatInput {}.command()).await.expect("raw failed");

                    let output = PubsubNumpatOutput::decode(&result).expect("decode failed");
                    // No pattern subscriptions by default
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numpat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&PubsubNumpatInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = PubsubNumpatOutput::decode(&result).expect("decode failed");
            assert!(output.count() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pubsub_numpat_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PubsubNumpatInput {}.command()).await.expect("raw failed");

            let output = PubsubNumpatOutput::decode(&result).expect("decode failed");
            assert!(output.count() >= 0);

            ctx.stop().await;
        }
    }
}
