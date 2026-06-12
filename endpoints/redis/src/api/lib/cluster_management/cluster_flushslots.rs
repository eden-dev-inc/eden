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

const API_INFO: ApiInfo<RedisApi, ClusterFlushslotsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterFlushslots,
    "Deletes all slots information from a node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER FLUSHSLOTS`
/// https://redis.io/docs/latest/commands/cluster-flushslots/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterFlushslotsInput {}

impl Serialize for ClusterFlushslotsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterFlushslotsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterFlushslotsInput, API_INFO);

impl RedisCommandInput for ClusterFlushslotsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let command = crate::command::cmd(&API_INFO.api.to_string());
        command.get_packed_command()
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
                "CLUSTER FLUSHSLOTS expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER FLUSHSLOTS command
///
/// Returns OK on success when all slot information is deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterFlushslotsOutput {
    /// "OK" on success
    status: String,
}

impl ClusterFlushslotsOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }
}

impl Default for ClusterFlushslotsOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterFlushslotsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterFlushslotsOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterFlushslotsOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterFlushslots
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CLUSTER FLUSHSLOTS response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER FLUSHSLOTS response: {:?}", other))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterFlushslotsInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("FLUSHSLOTS"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterFlushslotsOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterFlushslotsOutput::decode(b"-ERR CLUSTER FLUSHSLOTS can't be called when there are keys\r\n").unwrap_err();
            assert!(err.to_string().contains("keys"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterFlushslotsInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_decode_input_extra_args_warns_but_succeeds() {
            // Extra args should log warning but still succeed
            let args = vec![RedisJsonValue::String("extra".into())];
            let input = ClusterFlushslotsInput::decode(args);
            assert!(input.is_ok());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterFlushslotsInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterFlushslotsInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterFlushslots);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_flushslots_on_non_cluster_returns_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterFlushslotsInput::default().command()).await.expect("raw failed");

                    // On a non-cluster Redis, this should return an error
                    let output = ClusterFlushslotsOutput::decode(&result);
                    assert!(output.is_err() || !output.unwrap().is_ok());
                })
            })
            .await;
        }
    }
}
