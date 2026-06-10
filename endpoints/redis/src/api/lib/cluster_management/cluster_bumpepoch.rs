use crate::api::lib::RedisCommandOutput;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BumpepochResult, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterBumpepochInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterBumpepoch,
    "Advances the cluster config epoch",
    ReqType::Write, // This modifies cluster state
    true,
);

/// See official Redis documentation for `CLUSTER BUMPEPOCH`
/// https://redis.io/docs/latest/commands/cluster-bumpepoch/
///
/// Note: This command only works on Redis nodes running in cluster mode.
/// On standalone instances, it returns an error.
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterBumpepochInput {}

impl Serialize for ClusterBumpepochInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterBumpepochInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterBumpepochInput, API_INFO);

impl RedisCommandInput for ClusterBumpepochInput {
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
                "CLUSTER BUMPEPOCH expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER BUMPEPOCH command
///
/// Returns "BUMPED" if the epoch was incremented, or "STILL" if it wasn't changed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterBumpepochOutput {
    /// The result of the bump operation
    result: BumpepochResult,
}

impl ClusterBumpepochOutput {
    pub fn new(result: BumpepochResult) -> Self {
        Self { result }
    }

    /// Get the result of the bump operation
    pub fn result(&self) -> &BumpepochResult {
        &self.result
    }

    /// Check if the epoch was bumped
    pub fn was_bumped(&self) -> bool {
        matches!(self.result, BumpepochResult::Bumped)
    }

    /// Check if the epoch remained the same
    pub fn is_still(&self) -> bool {
        matches!(self.result, BumpepochResult::Still)
    }
}

impl Serialize for ClusterBumpepochOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterBumpepochOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterBumpepochOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterBumpepoch
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let response = String::from_utf8_lossy(&s).to_uppercase();
                    match response.as_str() {
                        "BUMPED" => BumpepochResult::Bumped,
                        "STILL" => BumpepochResult::Still,
                        _ => {
                            return Err(EpError::parse(format!("unexpected CLUSTER BUMPEPOCH response: {}", response)));
                        }
                    }
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER BUMPEPOCH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let response = String::from_utf8_lossy(&data).to_uppercase();
                    match response.as_str() {
                        "BUMPED" => BumpepochResult::Bumped,
                        "STILL" => BumpepochResult::Still,
                        _ => {
                            return Err(EpError::parse(format!("unexpected CLUSTER BUMPEPOCH response: {}", response)));
                        }
                    }
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER BUMPEPOCH response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterBumpepochInput::default();
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n"));
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("BUMPEPOCH"));
        }

        #[test]
        fn test_decode_bumped() {
            let output = ClusterBumpepochOutput::decode(b"+BUMPED\r\n").unwrap();
            assert!(output.was_bumped());
            assert!(!output.is_still());
            assert_eq!(output.result(), &BumpepochResult::Bumped);
        }

        #[test]
        fn test_decode_still() {
            let output = ClusterBumpepochOutput::decode(b"+STILL\r\n").unwrap();
            assert!(!output.was_bumped());
            assert!(output.is_still());
            assert_eq!(output.result(), &BumpepochResult::Still);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterBumpepochOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_cluster_disabled_error() {
            let err = ClusterBumpepochOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterBumpepochInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterBumpepochInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind_returns_cluster_bumpepoch() {
            let input = ClusterBumpepochInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterBumpepoch);
        }

        #[test]
        fn test_output_kind_returns_cluster_bumpepoch() {
            let output = ClusterBumpepochOutput::new(BumpepochResult::Bumped);
            assert_eq!(output.kind(), RedisApi::ClusterBumpepoch);
        }
    }

    // Note: CLUSTER BUMPEPOCH requires Redis running in cluster mode.
    // Standalone Redis instances return:
    //   -ERR This instance has cluster support disabled
    //
    // Proper integration testing requires a Redis Cluster setup.
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_bumpepoch_standalone_returns_error() {
            // Verify that standalone Redis correctly rejects cluster commands
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterBumpepochInput::default().command()).await.expect("raw failed");

                    // Standalone Redis returns an error for cluster commands
                    let err = ClusterBumpepochOutput::decode(&result);
                    assert!(err.is_err(), "Expected error on standalone Redis");
                    assert!(err.unwrap_err().to_string().contains("cluster"), "Error should mention cluster");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_bumpepoch_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&ClusterBumpepochInput::default().command()).await.expect("raw failed");

            // Standalone returns error
            assert!(result.starts_with(b"-"), "Expected RESP2 error response");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_bumpepoch_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&ClusterBumpepochInput::default().command()).await.expect("raw failed");

            // Standalone returns error (RESP3 simple error also starts with -)
            assert!(result.starts_with(b"-"), "Expected RESP3 error response");
            ctx.stop().await;
        }

        // These tests require a Redis Cluster and are ignored by default
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "Requires Redis Cluster setup"]
        async fn test_cluster_bumpepoch_success() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterBumpepochInput::default().command()).await.expect("raw failed");

                    let output = ClusterBumpepochOutput::decode(&result).expect("decode failed");
                    // Either BUMPED or STILL is valid
                    assert!(output.was_bumped() || output.is_still());
                })
            })
            .await;
        }
    }
}
