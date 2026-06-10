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

const API_INFO: ApiInfo<RedisApi, GeoposInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Geopos,
    "Returns the longitude and latitude of members from a geospatial index",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEOPOS`
/// https://redis.io/docs/latest/commands/geopos/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeoposInput {
    key: RedisKey,
    members: Vec<RedisJsonValue>,
}

impl Serialize for GeoposInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GeoposInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

impl_redis_operation!(
    GeoposInput,
    API_INFO,
    {key, members}
);

impl RedisCommandInput for GeoposInput {
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
            return Err(EpError::request(format!("GEOPOS requires at least 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            members: args[1..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_member() {
            let input = GeoposInput {
                key: RedisKey::String("mygeo".into()),
                members: vec![RedisJsonValue::from("Palermo")],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GEOPOS"));
            assert!(cmd_str.contains("mygeo"));
            assert!(cmd_str.contains("Palermo"));
        }

        #[test]
        fn test_encode_command_multiple_members() {
            let input = GeoposInput {
                key: RedisKey::String("mygeo".into()),
                members: vec![
                    RedisJsonValue::from("Palermo"),
                    RedisJsonValue::from("Catania"),
                    RedisJsonValue::from("Naples"),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("Palermo"));
            assert!(cmd_str.contains("Catania"));
            assert!(cmd_str.contains("Naples"));
        }

        #[test]
        fn test_decode_input_single_member() {
            let args = vec![RedisJsonValue::String("mygeo".into()), RedisJsonValue::from("Palermo")];
            let input = GeoposInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mygeo".into()));
            assert_eq!(input.members.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_members() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from("Palermo"),
                RedisJsonValue::from("Catania"),
                RedisJsonValue::from("Naples"),
            ];
            let input = GeoposInput::decode(args).unwrap();
            assert_eq!(input.members.len(), 3);
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mygeo".into())];
            let err = GeoposInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = GeoposInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GeoposInput {
                key: RedisKey::String("mygeo".into()),
                members: vec![RedisJsonValue::from("Palermo")],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mygeo".into()));
        }

        #[test]
        fn test_kind() {
            let input = GeoposInput {
                key: RedisKey::String("mygeo".into()),
                members: vec![RedisJsonValue::from("Palermo")],
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Geopos);
        }

        #[test]
        fn test_serialize_input() {
            let input = GeoposInput {
                key: RedisKey::String("mygeo".into()),
                members: vec![RedisJsonValue::from("Palermo")],
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("mygeo"));
            assert!(json.contains("Palermo"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{GeoaddInput, Position};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geopos_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: add locations
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geopos_test".into()),
                            options: None,
                            ch: None,
                            position: vec![Position {
                                longitude: RedisJsonValue::from(13.361389),
                                latitude: RedisJsonValue::from(38.115556),
                                member: RedisJsonValue::from("Palermo"),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("setup failed");

                    let result = ctx
                        .raw(
                            &GeoposInput {
                                key: RedisKey::String("geopos_test".into()),
                                members: vec![RedisJsonValue::from("Palermo")],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geopos_multiple_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geopos_multi".into()),
                            options: None,
                            ch: None,
                            position: vec![
                                Position {
                                    longitude: RedisJsonValue::from(13.361389),
                                    latitude: RedisJsonValue::from(38.115556),
                                    member: RedisJsonValue::from("Palermo"),
                                },
                                Position {
                                    longitude: RedisJsonValue::from(15.087269),
                                    latitude: RedisJsonValue::from(37.502669),
                                    member: RedisJsonValue::from("Catania"),
                                },
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("setup failed");

                    let result = ctx
                        .raw(
                            &GeoposInput {
                                key: RedisKey::String("geopos_multi".into()),
                                members: vec![RedisJsonValue::from("Palermo"), RedisJsonValue::from("Catania")],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geopos_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geopos_missing".into()),
                            options: None,
                            ch: None,
                            position: vec![Position {
                                longitude: RedisJsonValue::from(13.361389),
                                latitude: RedisJsonValue::from(38.115556),
                                member: RedisJsonValue::from("Palermo"),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("setup failed");

                    let result = ctx
                        .raw(
                            &GeoposInput {
                                key: RedisKey::String("geopos_missing".into()),
                                members: vec![RedisJsonValue::from("Palermo"), RedisJsonValue::from("NonExistent")],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Redis returns array with null for nonexistent members
                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geopos_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GeoposInput {
                                key: RedisKey::String("nonexistent_key".into()),
                                members: vec![RedisJsonValue::from("member")],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Redis returns array of nulls for nonexistent key
                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geopos_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geopos_pipe".into()),
                            options: None,
                            ch: None,
                            position: vec![
                                Position {
                                    longitude: RedisJsonValue::from(13.361389),
                                    latitude: RedisJsonValue::from(38.115556),
                                    member: RedisJsonValue::from("Palermo"),
                                },
                                Position {
                                    longitude: RedisJsonValue::from(15.087269),
                                    latitude: RedisJsonValue::from(37.502669),
                                    member: RedisJsonValue::from("Catania"),
                                },
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("setup failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &GeoposInput {
                            key: RedisKey::String("geopos_pipe".into()),
                            members: vec![RedisJsonValue::from("Palermo")],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GeoposInput {
                            key: RedisKey::String("geopos_pipe".into()),
                            members: vec![RedisJsonValue::from("Catania")],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    assert!(!result.is_empty());
                })
            })
            .await;
        }
    }
}
