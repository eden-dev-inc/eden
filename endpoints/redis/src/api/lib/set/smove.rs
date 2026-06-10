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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SmoveInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Smove, "Moves a member from one set to another", ReqType::Write, true);

/// See official Redis documentation for `SMOVE`
/// https://redis.io/docs/latest/commands/smove/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SmoveInput {
    source: RedisKey,
    destination: RedisKey,
    member: RedisJsonValue,
}

impl Serialize for SmoveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SmoveInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

impl_redis_operation!(
    SmoveInput,
    API_INFO,
    {source, destination, member }
);

impl RedisCommandInput for SmoveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.source).arg(&self.destination).arg(&self.member);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("SMOVE requires 3 arguments, given {}", args.len())))?;
        } else if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "SMOVE expects 3 arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self {
            source: args[0].clone().try_into()?,
            destination: args[1].clone().try_into()?,
            member: args[2].clone(),
        })
    }
}

/// Output for Redis SMOVE command
///
/// Returns whether the member was moved (true) or not (false).
/// Returns false if the member doesn't exist in the source set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SmoveOutput {
    /// Whether the member was successfully moved
    moved: bool,
}

impl SmoveOutput {
    pub fn new(moved: bool) -> Self {
        Self { moved }
    }

    /// Check if the member was moved
    pub fn was_moved(&self) -> bool {
        self.moved
    }

    /// Decode the Redis protocol response into a SmoveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => Ok(Self { moved: i == 1 }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SMOVE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { moved: data == 1 }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SMOVE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for SmoveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SmoveOutput", 1)?;
        state.serialize_field("moved", &self.moved)?;
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
            let input = SmoveInput {
                source: RedisKey::String("src_set".into()),
                destination: RedisKey::String("dst_set".into()),
                member: RedisJsonValue::String("member1".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SMOVE"));
            assert!(cmd_str.contains("src_set"));
            assert!(cmd_str.contains("dst_set"));
            assert!(cmd_str.contains("member1"));
        }

        #[test]
        fn test_encode_command_with_numeric_member() {
            let input = SmoveInput {
                source: RedisKey::String("set1".into()),
                destination: RedisKey::String("set2".into()),
                member: RedisJsonValue::Integer(42),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nSMOVE\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("source_set".into()),
                RedisJsonValue::String("dest_set".into()),
                RedisJsonValue::String("member".into()),
            ];
            let input = SmoveInput::decode(args).unwrap();
            assert_eq!(input.source, RedisKey::String("source_set".into()));
            assert_eq!(input.destination, RedisKey::String("dest_set".into()));
            assert_eq!(input.member, RedisJsonValue::String("member".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("source".into()), RedisJsonValue::String("dest".into())];
            let err = SmoveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("source".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("member".into()),
                RedisJsonValue::String("extra".into()),
            ];
            // Should succeed but log warning
            let input = SmoveInput::decode(args).unwrap();
            assert_eq!(input.member, RedisJsonValue::String("member".into()));
        }

        #[test]
        fn test_decode_output_moved() {
            let output = SmoveOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_moved());
        }

        #[test]
        fn test_decode_output_not_moved() {
            let output = SmoveOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_moved());
        }

        #[test]
        fn test_decode_output_moved_resp3() {
            let output = SmoveOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_moved());
        }

        #[test]
        fn test_decode_output_not_moved_resp3() {
            let output = SmoveOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_moved());
        }

        #[test]
        fn test_decode_output_error() {
            let err = SmoveOutput::decode(b"-WRONGTYPE wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = SmoveInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                member: RedisJsonValue::String("member".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("src".into()));
            assert_eq!(keys[1], RedisKey::String("dst".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::set::sadd::SaddInput;
        use crate::api::lib::set::sismember::SismemberInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_success() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: create source set with members
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_src".into()),
                            members: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd failed");

                    // Move member 'b' from source to destination
                    let result = ctx
                        .raw(
                            &SmoveInput {
                                source: RedisKey::String("smove_src".into()),
                                destination: RedisKey::String("smove_dst".into()),
                                member: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmoveOutput::decode(&result).expect("decode failed");
                    assert!(output.was_moved());

                    // Verify 'b' is no longer in source
                    let src_check = ctx
                        .raw(
                            &SismemberInput {
                                key: RedisKey::String("smove_src".into()),
                                member: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("sismember failed");
                    assert_eq!(src_check.as_ref(), b":0\r\n");

                    // Verify 'b' is in destination
                    let dst_check = ctx
                        .raw(
                            &SismemberInput {
                                key: RedisKey::String("smove_dst".into()),
                                member: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("sismember failed");
                    assert_eq!(dst_check.as_ref(), b":1\r\n");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: create source set without the member we'll try to move
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_src2".into()),
                            members: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("c".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd failed");

                    // Try to move non-existent member
                    let result = ctx
                        .raw(
                            &SmoveInput {
                                source: RedisKey::String("smove_src2".into()),
                                destination: RedisKey::String("smove_dst2".into()),
                                member: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmoveOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_moved());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_nonexistent_source() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Try to move from non-existent source
                    let result = ctx
                        .raw(
                            &SmoveInput {
                                source: RedisKey::String("nonexistent_smove_src".into()),
                                destination: RedisKey::String("smove_dst3".into()),
                                member: RedisJsonValue::String("member".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmoveOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_moved());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_to_existing_destination() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: create both source and destination sets
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_src4".into()),
                            members: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd src failed");

                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_dst4".into()),
                            members: vec![RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd dst failed");

                    // Move member from source to existing destination
                    let result = ctx
                        .raw(
                            &SmoveInput {
                                source: RedisKey::String("smove_src4".into()),
                                destination: RedisKey::String("smove_dst4".into()),
                                member: RedisJsonValue::String("a".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmoveOutput::decode(&result).expect("decode failed");
                    assert!(output.was_moved());

                    // Verify 'a' is in destination
                    let dst_check = ctx
                        .raw(
                            &SismemberInput {
                                key: RedisKey::String("smove_dst4".into()),
                                member: RedisJsonValue::String("a".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("sismember failed");
                    assert_eq!(dst_check.as_ref(), b":1\r\n");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_member_already_in_destination() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: create sets where member exists in both
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_src5".into()),
                            members: vec![RedisJsonValue::String("shared".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd src failed");

                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("smove_dst5".into()),
                            members: vec![RedisJsonValue::String("shared".into()), RedisJsonValue::String("y".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd dst failed");

                    // Move member that's already in destination
                    let result = ctx
                        .raw(
                            &SmoveInput {
                                source: RedisKey::String("smove_src5".into()),
                                destination: RedisKey::String("smove_dst5".into()),
                                member: RedisJsonValue::String("shared".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SmoveOutput::decode(&result).expect("decode failed");
                    assert!(output.was_moved());

                    // Verify it's still in destination (and removed from source)
                    let src_check = ctx
                        .raw(
                            &SismemberInput {
                                key: RedisKey::String("smove_src5".into()),
                                member: RedisJsonValue::String("shared".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("sismember failed");
                    assert_eq!(src_check.as_ref(), b":0\r\n");
                })
            })
            .await;
        }
    }
}
