use crate::api::lib::RedisCommandOutput;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Slot, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterAddslotsrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterAddslotsrange,
    "Assigns new hash slot ranges to a node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER ADDSLOTSRANGE`
/// https://redis.io/docs/latest/commands/cluster-addslotsrange/
///
/// Note: This command was added in Redis 7.0 and only works on Redis nodes
/// running in cluster mode. On standalone instances, it returns an error.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, Default)]
#[builder(default)]
pub struct ClusterAddslotsrangeInput {
    /// Slot ranges to assign (each range has start and end inclusive)
    slots: Vec<Slot>,
}

impl Serialize for ClusterAddslotsrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterAddslotsrangeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl_redis_operation!(ClusterAddslotsrangeInput, API_INFO, { slots });

impl RedisCommandInput for ClusterAddslotsrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for slot in &self.slots {
            command.arg(&slot.start).arg(&slot.end);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLUSTER ADDSLOTSRANGE requires at least one start/end pair"));
        }

        if !args.len().is_multiple_of(2) {
            return Err(EpError::request("CLUSTER ADDSLOTSRANGE requires pairs of start/end values"));
        }

        let mut slots = Vec::new();
        for chunk in args.chunks(2) {
            slots.push(Slot { start: chunk[0].clone(), end: chunk[1].clone() });
        }

        Ok(Self { slots })
    }
}

/// Output for Redis CLUSTER ADDSLOTSRANGE command
///
/// Returns "OK" on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterAddslotsrangeOutput {
    /// Success status
    success: bool,
}

impl ClusterAddslotsrangeOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the command was successful
    pub fn is_success(&self) -> bool {
        self.success
    }
}

impl Serialize for ClusterAddslotsrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterAddslotsrangeOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterAddslotsrangeOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterAddslotsrange
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
                    return Err(EpError::parse(format!("unexpected CLUSTER ADDSLOTSRANGE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => data == b"OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER ADDSLOTSRANGE response: {:?}", other)));
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
        fn test_encode_command_single_range() {
            let input = ClusterAddslotsrangeInput {
                slots: vec![Slot {
                    start: RedisJsonValue::Integer(1),
                    end: RedisJsonValue::Integer(5),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("ADDSLOTSRANGE"));
        }

        #[test]
        fn test_encode_command_multiple_ranges() {
            let input = ClusterAddslotsrangeInput {
                slots: vec![
                    Slot {
                        start: RedisJsonValue::Integer(1),
                        end: RedisJsonValue::Integer(5),
                    },
                    Slot {
                        start: RedisJsonValue::Integer(10),
                        end: RedisJsonValue::Integer(15),
                    },
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = ClusterAddslotsrangeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterAddslotsrangeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_cluster_disabled_error() {
            let err = ClusterAddslotsrangeOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(5),
                RedisJsonValue::Integer(10),
                RedisJsonValue::Integer(15),
            ];
            let input = ClusterAddslotsrangeInput::decode(args).unwrap();
            assert_eq!(input.slots.len(), 2);
        }

        #[test]
        fn test_decode_input_odd_count_fails() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(5), RedisJsonValue::Integer(10)];
            let err = ClusterAddslotsrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("pairs of start/end values"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterAddslotsrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least one"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterAddslotsrangeInput {
                slots: vec![Slot {
                    start: RedisJsonValue::Integer(1),
                    end: RedisJsonValue::Integer(5),
                }],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind_returns_cluster_addslotsrange() {
            let input = ClusterAddslotsrangeInput {
                slots: vec![Slot {
                    start: RedisJsonValue::Integer(1),
                    end: RedisJsonValue::Integer(5),
                }],
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterAddslotsrange);
        }

        #[test]
        fn test_output_kind_returns_cluster_addslotsrange() {
            let output = ClusterAddslotsrangeOutput::new(true);
            assert_eq!(output.kind(), RedisApi::ClusterAddslotsrange);
        }
    }

    // Note: CLUSTER ADDSLOTSRANGE requires:
    // 1. Redis 7.0+ (command was added in 7.0)
    // 2. Redis running in cluster mode
    //
    // Standalone Redis instances return:
    //   -ERR This instance has cluster support disabled
    // Redis < 7.0 returns:
    //   -ERR unknown command 'CLUSTER ADDSLOTSRANGE'
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslotsrange_standalone_returns_error() {
            // Only test on Redis 7.0+ where the command exists
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterAddslotsrangeInput {
                                slots: vec![Slot {
                                    start: RedisJsonValue::Integer(1),
                                    end: RedisJsonValue::Integer(5),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Standalone Redis returns an error for cluster commands
                    let err = ClusterAddslotsrangeOutput::decode(&result);
                    assert!(err.is_err(), "Expected error on standalone Redis");
                    assert!(err.unwrap_err().to_string().contains("cluster"), "Error should mention cluster");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslotsrange_pre_v7_unknown_command() {
            // Test that Redis < 7.0 returns unknown command error
            for version in ["5", "6"] {
                for resp in [RespVersion::Resp2, RespVersion::Resp3] {
                    // RESP3 requires Redis 6+
                    if matches!(resp, RespVersion::Resp3) && version == "5" {
                        continue;
                    }

                    let mut ctx = setup(resp, Some(version)).await;
                    let result = ctx
                        .raw(
                            &ClusterAddslotsrangeInput {
                                slots: vec![Slot {
                                    start: RedisJsonValue::Integer(1),
                                    end: RedisJsonValue::Integer(5),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should get unknown command or cluster disabled error
                    assert!(result.starts_with(b"-"), "Expected error response");
                    ctx.stop().await;
                }
            }
        }

        // These tests require a Redis 7.0+ Cluster and are ignored by default
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "Requires Redis 7.0+ Cluster setup"]
        async fn test_cluster_addslotsrange_success() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterAddslotsrangeInput {
                                slots: vec![Slot {
                                    start: RedisJsonValue::Integer(16000),
                                    end: RedisJsonValue::Integer(16010),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ClusterAddslotsrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "Requires Redis 7.0+ Cluster setup"]
        async fn test_cluster_addslotsrange_multiple_ranges() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterAddslotsrangeInput {
                                slots: vec![
                                    Slot {
                                        start: RedisJsonValue::Integer(16011),
                                        end: RedisJsonValue::Integer(16020),
                                    },
                                    Slot {
                                        start: RedisJsonValue::Integer(16021),
                                        end: RedisJsonValue::Integer(16030),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ClusterAddslotsrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslotsrange_resp2_error_format() {
            // Only test on Redis 7.0+ where the command exists
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            let result = ctx
                .raw(
                    &ClusterAddslotsrangeInput {
                        slots: vec![Slot {
                            start: RedisJsonValue::Integer(1),
                            end: RedisJsonValue::Integer(5),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // Standalone returns error
            assert!(result.starts_with(b"-"), "Expected RESP2 error response");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_addslotsrange_resp3_error_format() {
            // Only test on Redis 7.0+ where the command exists
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            let result = ctx
                .raw(
                    &ClusterAddslotsrangeInput {
                        slots: vec![Slot {
                            start: RedisJsonValue::Integer(1),
                            end: RedisJsonValue::Integer(5),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // Standalone returns error
            assert!(result.starts_with(b"-"), "Expected RESP3 error response");
            ctx.stop().await;
        }
    }
}
