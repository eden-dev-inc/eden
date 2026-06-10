use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Unit, key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, GeodistInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Geodist,
    "Returns the distance between two members of a geospatial index",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEODIST`
/// https://redis.io/docs/latest/commands/geodist/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeodistInput {
    key: RedisKey,
    member1: RedisJsonValue,
    member2: RedisJsonValue,
    unit: Option<Unit>,
}

impl Serialize for GeodistInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, member1, member2
        if self.unit.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeodistInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member1", &self.member1)?;
        state.serialize_field("member2", &self.member2)?;

        if let Some(unit) = &self.unit {
            state.serialize_field("unit", unit)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    GeodistInput,
    API_INFO,
    {key, member1, member2, unit}
);

impl RedisCommandInput for GeodistInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.member1).arg(&self.member2);

        if let Some(unit) = &self.unit {
            match unit {
                Unit::M => {
                    command.arg("M");
                }
                Unit::KM => {
                    command.arg("KM");
                }
                Unit::FT => {
                    command.arg("FT");
                }
                Unit::MI => {
                    command.arg("MI");
                }
            };
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("GEODIST requires at least 3 arguments, given {}", args.len())));
        } else if args.len() > 4 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GEODIST expects at most 4 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let unit = if args.len() >= 4 {
            match &args[3] {
                RedisJsonValue::String(s) => Some(match s.as_str() {
                    "M" => Unit::M,
                    "KM" => Unit::KM,
                    "FT" => Unit::FT,
                    "MI" => Unit::MI,
                    _ => return Err(EpError::request("Invalid unit for GEODIST")),
                }),
                _ => return Err(EpError::parse("GEODIST unit must be a string")),
            }
        } else {
            None
        };

        Ok(Self {
            key: args[0].clone().try_into()?,
            member1: args[1].clone(),
            member2: args[2].clone(),
            unit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = GeodistInput {
                key: RedisKey::String("mygeo".into()),
                member1: RedisJsonValue::from("Palermo"),
                member2: RedisJsonValue::from("Catania"),
                unit: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GEODIST"));
            assert!(cmd_str.contains("mygeo"));
            assert!(cmd_str.contains("Palermo"));
            assert!(cmd_str.contains("Catania"));
        }

        #[test]
        fn test_encode_command_with_unit() {
            let input = GeodistInput {
                key: RedisKey::String("mygeo".into()),
                member1: RedisJsonValue::from("Palermo"),
                member2: RedisJsonValue::from("Catania"),
                unit: Some(Unit::KM),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("KM"));
        }

        #[test]
        fn test_encode_command_all_units() {
            for (unit, expected) in [(Unit::M, "M"), (Unit::KM, "KM"), (Unit::FT, "FT"), (Unit::MI, "MI")] {
                let input = GeodistInput {
                    key: RedisKey::String("mygeo".into()),
                    member1: RedisJsonValue::from("m1"),
                    member2: RedisJsonValue::from("m2"),
                    unit: Some(unit),
                };
                let cmd = input.command();
                let cmd_str = String::from_utf8_lossy(&cmd);
                assert!(cmd_str.contains(expected));
            }
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from("Palermo"),
                RedisJsonValue::from("Catania"),
            ];
            let input = GeodistInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mygeo".into()));
            assert!(input.unit.is_none());
        }

        #[test]
        fn test_decode_input_with_unit() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from("Palermo"),
                RedisJsonValue::from("Catania"),
                RedisJsonValue::String("KM".into()),
            ];
            let input = GeodistInput::decode(args).unwrap();
            assert!(matches!(input.unit, Some(Unit::KM)));
        }

        #[test]
        fn test_decode_input_all_units() {
            for (unit_str, expected) in [("M", Unit::M), ("KM", Unit::KM), ("FT", Unit::FT), ("MI", Unit::MI)] {
                let args = vec![
                    RedisJsonValue::String("mygeo".into()),
                    RedisJsonValue::from("m1"),
                    RedisJsonValue::from("m2"),
                    RedisJsonValue::String(unit_str.into()),
                ];
                let input = GeodistInput::decode(args).unwrap();
                assert!(matches!(input.unit, Some(u) if format!("{:?}", u) == format!("{:?}", expected)));
            }
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mygeo".into()), RedisJsonValue::from("Palermo")];
            let err = GeodistInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_unit() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from("Palermo"),
                RedisJsonValue::from("Catania"),
                RedisJsonValue::String("INVALID".into()),
            ];
            let err = GeodistInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Invalid unit"));
        }

        #[test]
        fn test_decode_input_non_string_unit() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from("Palermo"),
                RedisJsonValue::from("Catania"),
                RedisJsonValue::Integer(123),
            ];
            let err = GeodistInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("unit must be a string"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GeodistInput {
                key: RedisKey::String("mygeo".into()),
                member1: RedisJsonValue::from("Palermo"),
                member2: RedisJsonValue::from("Catania"),
                unit: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mygeo".into()));
        }

        #[test]
        fn test_kind() {
            let input = GeodistInput {
                key: RedisKey::String("mygeo".into()),
                member1: RedisJsonValue::from("Palermo"),
                member2: RedisJsonValue::from("Catania"),
                unit: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Geodist);
        }

        #[test]
        fn test_serialize_input() {
            let input = GeodistInput {
                key: RedisKey::String("mygeo".into()),
                member1: RedisJsonValue::from("Palermo"),
                member2: RedisJsonValue::from("Catania"),
                unit: Some(Unit::KM),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("mygeo"));
            assert!(json.contains("Palermo"));
            assert!(json.contains("Catania"));
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
        async fn test_geodist_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: add two locations
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geodist_test".into()),
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
                            &GeodistInput {
                                key: RedisKey::String("geodist_test".into()),
                                member1: RedisJsonValue::from("Palermo"),
                                member2: RedisJsonValue::from("Catania"),
                                unit: None,
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
        async fn test_geodist_with_unit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geodist_km".into()),
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
                            &GeodistInput {
                                key: RedisKey::String("geodist_km".into()),
                                member1: RedisJsonValue::from("Palermo"),
                                member2: RedisJsonValue::from("Catania"),
                                unit: Some(Unit::KM),
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
        async fn test_geodist_nonexistent_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("geodist_missing".into()),
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
                            &GeodistInput {
                                key: RedisKey::String("geodist_missing".into()),
                                member1: RedisJsonValue::from("Palermo"),
                                member2: RedisJsonValue::from("NonExistent"),
                                unit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Redis returns null for nonexistent members
                    assert!(!result.is_empty());
                })
            })
            .await;
        }
    }
}
