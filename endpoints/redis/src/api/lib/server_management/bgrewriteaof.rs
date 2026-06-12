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
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, BgrewriteaofInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bgrewriteaof,
    "Asynchronously rewrites the append-only file to disk",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `BGREWRITEAOF`
/// https://redis.io/docs/latest/commands/bgrewriteaof/
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BgrewriteaofInput {}

impl_redis_operation!(BgrewriteaofInput, API_INFO);

impl RedisCommandInput for BgrewriteaofInput {
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
                "BGREWRITEAOF expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        };

        Ok(Self::default())
    }
}

/// Output for Redis BGREWRITEAOF command
#[derive(Debug, Clone, PartialEq)]
pub struct BgrewriteaofOutput {
    message: String,
}

impl BgrewriteaofOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    /// Get the response message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Check if the rewrite was scheduled
    pub fn is_scheduled(&self) -> bool {
        self.message.contains("scheduled") || self.message.contains("started")
    }

    /// Decode the Redis protocol response
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BGREWRITEAOF response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BGREWRITEAOF response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::RedisCommandInput;
    use crate::protocol::decoder::{DecoderRespFrame, RedisCommandArgs};
    use endpoint_types::Operation;
    use redis_protocol::resp2::decode::decode as decode_resp2;
    use redis_protocol::resp3::decode::complete::decode as decode_resp3;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = BgrewriteaofInput::default();
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*1\r\n$12\r\nBGREWRITEAOF\r\n");
        }

        #[test]
        fn test_kind() {
            let input = BgrewriteaofInput::default();
            assert_eq!(Operation::kind(&input), RedisApi::Bgrewriteaof);
        }

        #[test]
        fn test_keys_empty() {
            let input = BgrewriteaofInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_no_args() {
            let args = vec![];
            let result = BgrewriteaofInput::decode(args).unwrap();
            assert_eq!(result, BgrewriteaofInput::default());
        }

        #[test]
        fn test_decode_with_extra_args_succeeds() {
            // Command should still succeed with extra args (just logs warning)
            let args = vec![RedisJsonValue::String("extra".to_string())];
            let result = BgrewriteaofInput::decode(args).unwrap();
            assert_eq!(result, BgrewriteaofInput::default());
        }

        #[test]
        fn test_decode_output_simple_string() {
            let output = BgrewriteaofOutput::decode(b"+Background append only file rewriting started\r\n").unwrap();
            assert!(output.message().contains("started"));
            assert!(output.is_scheduled());
        }

        #[test]
        fn test_decode_output_scheduled() {
            let output = BgrewriteaofOutput::decode(b"+Background append only file rewriting scheduled\r\n").unwrap();
            assert!(output.message().contains("scheduled"));
            assert!(output.is_scheduled());
        }

        #[test]
        fn test_decode_output_error() {
            let err = BgrewriteaofOutput::decode(b"-ERR Background append only file rewriting already in progress\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_raw_resp2() {
            let raw = b"*1\r\n$12\r\nBGREWRITEAOF\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Bgrewriteaof);
            assert_eq!(input.args().len(), 0);

            let decoded = BgrewriteaofInput::decode(input.args().to_vec()).unwrap();
            assert_eq!(decoded, BgrewriteaofInput::default());
        }

        #[test]
        fn test_decode_raw_resp2_lowercase() {
            let raw = b"*1\r\n$12\r\nbgrewriteaof\r\n";
            let (frame, _) = decode_resp2(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp2(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Bgrewriteaof);
            assert_eq!(input.args().len(), 0);
        }

        #[test]
        fn test_decode_raw_resp3() {
            let raw = b"*1\r\n$12\r\nBGREWRITEAOF\r\n";
            let (frame, _) = decode_resp3(raw).expect("failed to decode").unwrap();
            let decoder_frame = DecoderRespFrame::Resp3(frame);
            let input = RedisCommandArgs::try_from(decoder_frame).unwrap();

            assert_eq!(input.command(), &RedisApi::Bgrewriteaof);
            assert_eq!(input.args().len(), 0);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bgrewriteaof_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&BgrewriteaofInput::default().command()).await.expect("raw failed");

                    // Response could be success or error if already running
                    let output_result = BgrewriteaofOutput::decode(&result);
                    assert!(output_result.is_ok() || result.starts_with(b"-ERR"), "should return valid response or error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bgrewriteaof_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&BgrewriteaofInput::default().command()).await.expect("raw failed");

            // RESP2 simple string format starts with +
            assert!(result.starts_with(b"+") || result.starts_with(b"-"), "RESP2 should return simple string or error");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bgrewriteaof_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&BgrewriteaofInput::default().command()).await.expect("raw failed");

            // RESP3 also uses simple string format for this command
            assert!(result.starts_with(b"+") || result.starts_with(b"-"), "RESP3 should return simple string or error");
            ctx.stop().await;
        }
    }
}
