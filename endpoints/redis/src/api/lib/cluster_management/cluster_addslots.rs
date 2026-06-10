use crate::api::lib::RedisCommandOutput;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterAddslotsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterAddslots, "Assigns new hash slots to a node", ReqType::Write, true);

/// See official Redis documentation for `CLUSTER ADDSLOTS`
/// https://redis.io/docs/latest/commands/cluster-addslots/
///
/// Note: This command only works on Redis nodes running in cluster mode.
/// On standalone instances, it returns an error.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterAddslotsInput {
    /// Slot numbers to assign to this node (0-16383)
    slots: Vec<RedisJsonValue>,
}

impl Serialize for ClusterAddslotsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterAddslotsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl_redis_operation!(ClusterAddslotsInput, API_INFO, { slots });

impl RedisCommandInput for ClusterAddslotsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for slot in &self.slots {
            command.arg(slot);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLUSTER ADDSLOTS requires at least 1 argument, given none"));
        }

        Ok(Self { slots: args })
    }
}

/// Output for Redis CLUSTER ADDSLOTS command
///
/// Returns "OK" on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterAddslotsOutput {
    /// Success status
    success: bool,
}

impl ClusterAddslotsOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the command was successful
    pub fn is_success(&self) -> bool {
        self.success
    }
}

impl Serialize for ClusterAddslotsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterAddslotsOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterAddslotsOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterAddslots
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
                    return Err(EpError::parse(format!("unexpected CLUSTER ADDSLOTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => data == b"OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER ADDSLOTS response: {:?}", other)));
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
        fn test_encode_command_single_slot() {
            let input = ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n"));
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("ADDSLOTS"));
        }

        #[test]
        fn test_encode_command_multiple_slots() {
            let input = ClusterAddslotsInput {
                slots: vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(2), RedisJsonValue::Integer(3)],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = ClusterAddslotsOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterAddslotsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_cluster_disabled_error() {
            let err = ClusterAddslotsOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)];
            let input = ClusterAddslotsInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterAddslotsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind_returns_cluster_addslots() {
            let input = ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterAddslots);
        }

        #[test]
        fn test_output_kind_returns_cluster_addslots() {
            let output = ClusterAddslotsOutput::new(true);
            assert_eq!(output.kind(), RedisApi::ClusterAddslots);
        }
    }

    // Note: CLUSTER ADDSLOTS requires Redis running in cluster mode.
    // Standalone Redis instances return:
    //   -ERR This instance has cluster support disabled
    //
    // Proper integration testing requires:
    // 1. A Redis Cluster setup (minimum 3 master nodes recommended)
    // 2. Slots that are not already assigned
    // 3. Running the command on the correct node
    //
    // These tests are marked #[ignore] and require manual cluster setup.
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslots_standalone_returns_error() {
            // Verify that standalone Redis correctly rejects cluster commands
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] }.command()).await.expect("raw failed");

                    // Standalone Redis returns an error for cluster commands
                    let err = ClusterAddslotsOutput::decode(&result);
                    assert!(err.is_err(), "Expected error on standalone Redis");
                    assert!(err.unwrap_err().to_string().contains("cluster"), "Error should mention cluster");
                })
            })
            .await;
        }

        // These tests require a Redis Cluster and are ignored by default
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "Requires Redis Cluster setup"]
        async fn test_cluster_addslots_success() {
            // This test requires:
            // 1. Redis running in cluster mode
            // 2. Slot 16000 not already assigned
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(16000)] }.command()).await.expect("raw failed");

                    let output = ClusterAddslotsOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "Requires Redis Cluster setup"]
        async fn test_cluster_addslots_multiple_slots() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterAddslotsInput {
                                slots: vec![
                                    RedisJsonValue::Integer(16001),
                                    RedisJsonValue::Integer(16002),
                                    RedisJsonValue::Integer(16003),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ClusterAddslotsOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslots_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] }.command()).await.expect("raw failed");

            // Standalone returns error
            assert!(result.starts_with(b"-"), "Expected RESP2 error response");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslots_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&ClusterAddslotsInput { slots: vec![RedisJsonValue::Integer(1)] }.command()).await.expect("raw failed");

            // Standalone returns error (RESP3 simple error also starts with -)
            assert!(result.starts_with(b"-"), "Expected RESP3 error response");
            ctx.stop().await;
        }
    }
}
