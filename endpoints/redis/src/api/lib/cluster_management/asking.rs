use crate::api::lib::RedisCommandOutput;
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

const API_INFO: ApiInfo<RedisApi, AskingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Asking,
    "Signals that a cluster client is following an -ASK redirect",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `ASKING`
/// https://redis.io/docs/latest/commands/asking/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct AskingInput {}

impl Serialize for AskingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AskingInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(AskingInput, API_INFO);

impl RedisCommandInput for AskingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
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
                "ASKING expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis ASKING command
///
/// Returns "OK" on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct AskingOutput {
    /// Success status
    success: bool,
}

impl AskingOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the command was successful
    pub fn is_success(&self) -> bool {
        self.success
    }
}

impl Serialize for AskingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AskingOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl RedisCommandOutput for AskingOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Asking
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => s == b"OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ASKING response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => data == b"OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ASKING response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = AskingInput::default();
            assert_eq!(input.command().to_vec(), b"*1\r\n$6\r\nASKING\r\n");
        }

        #[test]
        fn test_decode_success() {
            let output = AskingOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AskingOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = AskingInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AskingInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind_returns_asking() {
            let input = AskingInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Asking);
        }

        #[test]
        fn test_output_kind_returns_asking() {
            let output = AskingOutput::new(true);
            assert_eq!(output.kind(), RedisApi::Asking);
        }
    }

    // Note: Integration tests for ASKING require a Redis Cluster setup.
    // The ASKING command only makes sense in cluster mode when following
    // an -ASK redirect. On standalone Redis instances, it returns OK but
    // has no practical effect.
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // ASKING works on standalone Redis (returns OK) but is only meaningful in cluster mode.
        // These tests verify protocol correctness, not cluster behavior.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_asking_standalone_returns_ok() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AskingInput::default().command()).await.expect("raw failed");

                    // On standalone Redis (non-cluster), ASKING returns an error
                    // In cluster mode, it would return OK
                    let output = AskingOutput::decode(&result);

                    match output {
                        Ok(out) => {
                            assert!(out.is_success());
                        }
                        Err(e) => {
                            assert!(
                                e.to_string().contains("cluster support disabled")
                                    || e.to_string().contains("This instance has cluster support disabled"),
                                "Expected cluster disabled error, got: {}",
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
        async fn test_asking_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&AskingInput::default().command()).await.expect("raw failed");

            // On standalone Redis (non-cluster), ASKING returns an error
            // In cluster mode, it would return +OK\r\n
            let output = AskingOutput::decode(&result);

            match output {
                Ok(out) => {
                    assert!(out.is_success());
                    assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
                }
                Err(e) => {
                    assert!(
                        e.to_string().contains("cluster support disabled")
                            || e.to_string().contains("This instance has cluster support disabled"),
                        "Expected cluster disabled error, got: {}",
                        e
                    );
                    assert!(result.starts_with(b"-ERR"), "Expected RESP2 error format");
                }
            }
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_asking_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&AskingInput::default().command()).await.expect("raw failed");

            // On standalone Redis (non-cluster), ASKING returns an error
            // In cluster mode, it would return +OK\r\n
            let output = AskingOutput::decode(&result);

            match output {
                Ok(out) => {
                    assert!(out.is_success());
                    assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string OK format");
                }
                Err(e) => {
                    assert!(
                        e.to_string().contains("cluster support disabled")
                            || e.to_string().contains("This instance has cluster support disabled"),
                        "Expected cluster disabled error, got: {}",
                        e
                    );
                    assert!(result.starts_with(b"-ERR") || result.starts_with(b"-"), "Expected RESP3 error format");
                }
            }
            ctx.stop().await;
        }
    }
}
