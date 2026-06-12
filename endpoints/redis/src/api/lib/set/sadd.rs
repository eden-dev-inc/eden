use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sadd,
    "Adds one or more members to a set. Creates the key if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SADD`
/// https://redis.io/docs/latest/commands/sadd/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SaddInput {
    pub(crate) key: RedisKey,
    pub(crate) members: Vec<RedisJsonValue>,
}

impl Serialize for SaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SaddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

impl_redis_operation!(
    SaddInput,
    API_INFO,
    {key, members}
);

impl RedisCommandInput for SaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.members);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SADD requires at least 2 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let members = args[1..].to_vec();

        Ok(Self { key, members })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = SaddInput {
                key: RedisKey::String("myset".into()),
                members: vec![RedisJsonValue::String("member1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SADD"));
            assert!(cmd_str.contains("myset"));
            assert!(cmd_str.contains("member1"));
        }

        #[test]
        fn test_encode_command_multiple_members() {
            let input = SaddInput {
                key: RedisKey::String("myset".into()),
                members: vec![
                    RedisJsonValue::String("a".into()),
                    RedisJsonValue::String("b".into()),
                    RedisJsonValue::String("c".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SADD"));
            assert!(cmd_str.contains("myset"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("myset".into()), RedisJsonValue::String("member".into())];
            let input = SaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert_eq!(input.members.len(), 1);
            assert_eq!(input.members[0], RedisJsonValue::String("member".into()));
        }

        #[test]
        fn test_decode_input_multiple_members() {
            let args = vec![
                RedisJsonValue::String("myset".into()),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let input = SaddInput::decode(args).unwrap();
            assert_eq!(input.members.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myset".into())];
            let err = SaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SaddInput {
                key: RedisKey::String("myset".into()),
                members: vec![RedisJsonValue::String("member".into())],
            };
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myset".into()));
        }

        #[test]
        fn test_decode_input_numeric_members() {
            let args = vec![
                RedisJsonValue::String("numset".into()),
                RedisJsonValue::Integer(42),
                RedisJsonValue::Float(3.01),
            ];
            let input = SaddInput::decode(args).unwrap();
            assert_eq!(input.members.len(), 2);
            assert_eq!(input.members[0], RedisJsonValue::Integer(42));
            assert_eq!(input.members[1], RedisJsonValue::Float(3.01));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args = vec![];
            let err = SaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_single_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsadd_single\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_single".into()),
                                members: vec![RedisJsonValue::String("member1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // SADD returns number of elements added
                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("1") || response.starts_with(":1"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_multiple_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nsadd_multi\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_multi".into()),
                                members: vec![
                                    RedisJsonValue::String("a".into()),
                                    RedisJsonValue::String("b".into()),
                                    RedisJsonValue::String("c".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("3") || response.starts_with(":3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_add_duplicates() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nsadd_dups\r\n").await.expect("del");

                    // Add initial members
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("sadd_dups".into()),
                            members: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd");

                    // Try to add the same members again
                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_dups".into()),
                                members: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should return 0 since no new members were added
                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("0") || response.starts_with(":0"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_partial_duplicates() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nsadd_partial\r\n").await.expect("del");

                    // Add initial member
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("sadd_partial".into()),
                            members: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd");

                    // Add one duplicate and one new member
                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_partial".into()),
                                members: vec![
                                    RedisJsonValue::String("a".into()), // duplicate
                                    RedisJsonValue::String("b".into()), // new
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should return 1 (only "b" was added)
                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("1") || response.starts_with(":1"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_mixed_types() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nsadd_mixed\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_mixed".into()),
                                members: vec![
                                    RedisJsonValue::String("string".into()),
                                    RedisJsonValue::Integer(42),
                                    RedisJsonValue::Float(3.25),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("3") || response.starts_with(":3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a string key first
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$10\r\nsadd_wrong\r\n$5\r\nvalue\r\n").await.expect("set");

                    // Try to use SADD on the string key
                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::String("sadd_wrong".into()),
                                members: vec![RedisJsonValue::String("member".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nsadd_r2\r\n").await.expect("del");

            let result = ctx
                .raw(
                    &SaddInput {
                        key: RedisKey::String("sadd_r2".into()),
                        members: vec![RedisJsonValue::String("member".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let response = String::from_utf8_lossy(&result);
            assert!(response.contains("1"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nsadd_r3\r\n").await.expect("del");

            let result = ctx
                .raw(
                    &SaddInput {
                        key: RedisKey::String("sadd_r3".into()),
                        members: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let response = String::from_utf8_lossy(&result);
            assert!(response.contains("2"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_numeric_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$5\r\n12345\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &SaddInput {
                                key: RedisKey::Integer(12345, "12345".to_string()),
                                members: vec![RedisJsonValue::String("element".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("1") || response.starts_with(":1"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sadd_large_batch() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nsadd_batch\r\n").await.expect("del");

                    let members: Vec<RedisJsonValue> = (1..=10).map(|i| RedisJsonValue::String(format!("item{}", i))).collect();

                    let result =
                        ctx.raw(&SaddInput { key: RedisKey::String("sadd_batch".into()), members }.command()).await.expect("raw failed");

                    let response = String::from_utf8_lossy(&result);
                    assert!(response.contains("10") || response.starts_with(":10"));
                })
            })
            .await;
        }
    }
}
