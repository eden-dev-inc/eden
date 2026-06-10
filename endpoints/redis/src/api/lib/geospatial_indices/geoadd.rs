use crate::api::lib::geospatial_indices::{Options, Position};
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

const API_INFO: ApiInfo<RedisApi, GeoaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Geoadd,
    "Adds one or more members to a geospatial index. The key is created if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `GEOADD`
/// https://redis.io/docs/latest/commands/geoadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeoaddInput {
    pub(crate) key: RedisKey,
    pub(crate) options: Option<Options>,
    pub(crate) ch: Option<bool>,
    pub(crate) position: Vec<Position>,
}

impl Serialize for GeoaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, position
        if self.options.is_some() {
            fields += 1;
        }
        if self.ch.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeoaddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("position", &self.position)?;

        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        if let Some(ch) = &self.ch {
            state.serialize_field("ch", ch)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    GeoaddInput,
    API_INFO,
    {key, options, ch, position}
);

impl RedisCommandInput for GeoaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(options) = &self.options {
            match options {
                Options::NX => {
                    command.arg("NX");
                }
                Options::XX => {
                    command.arg("XX");
                }
            };
        }

        if let Some(ch) = self.ch
            && ch
        {
            command.arg("CH");
        }

        for position in &self.position {
            command.arg(&position.longitude);
            command.arg(&position.latitude);
            command.arg(&position.member);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!(
                "GEOADD requires at least 4 arguments (key + lon/lat/member), given {}",
                args.len()
            )));
        }

        let mut options = None;
        let mut ch = None;
        let mut arg_idx = 1;

        // Parse optional flags
        while arg_idx < args.len() {
            match &args[arg_idx] {
                RedisJsonValue::String(s) => match s.as_str() {
                    "NX" => {
                        options = Some(Options::NX);
                        arg_idx += 1;
                    }
                    "XX" => {
                        options = Some(Options::XX);
                        arg_idx += 1;
                    }
                    "CH" => {
                        ch = Some(true);
                        arg_idx += 1;
                    }
                    _ => break, // Start of position data
                },
                _ => break, // Start of position data
            }
        }

        // Parse positions (longitude, latitude, member) triplets
        if !(args.len() - arg_idx).is_multiple_of(3) {
            return Err(EpError::request("GEOADD position data must be in triplets (lon, lat, member)"));
        }

        let mut position = Vec::new();
        while arg_idx + 2 < args.len() {
            position.push(Position {
                longitude: args[arg_idx].clone(),
                latitude: args[arg_idx + 1].clone(),
                member: args[arg_idx + 2].clone(),
            });
            arg_idx += 3;
        }

        if position.is_empty() {
            return Err(EpError::request("GEOADD requires at least one position"));
        }

        Ok(Self { key: args[0].clone().try_into()?, options, ch, position })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: None,
                ch: None,
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GEOADD"));
            assert!(cmd_str.contains("mygeo"));
            assert!(cmd_str.contains("Palermo"));
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: Some(Options::NX),
                ch: None,
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NX"));
            assert!(!cmd_str.contains("XX"));
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: Some(Options::XX),
                ch: None,
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("XX"));
            assert!(!cmd_str.contains("NX"));
            assert!(!cmd_str.contains("CH"));
        }

        #[test]
        fn test_encode_command_with_ch() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: None,
                ch: Some(true),
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CH"));
        }

        #[test]
        fn test_encode_command_multiple_positions() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
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
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("Palermo"));
            assert!(cmd_str.contains("Catania"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
            ];
            let input = GeoaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mygeo".into()));
            assert_eq!(input.position.len(), 1);
            assert!(input.options.is_none());
            assert!(input.ch.is_none());
        }

        #[test]
        fn test_decode_input_with_nx() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::String("NX".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
            ];
            let input = GeoaddInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(Options::NX)));
        }

        #[test]
        fn test_decode_input_with_xx() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::String("XX".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
            ];
            let input = GeoaddInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(Options::XX)));
        }

        #[test]
        fn test_decode_input_with_ch() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::String("CH".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
            ];
            let input = GeoaddInput::decode(args).unwrap();
            assert_eq!(input.ch, Some(true));
        }

        #[test]
        fn test_decode_input_multiple_positions() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
                RedisJsonValue::from(15.087269),
                RedisJsonValue::from(37.502669),
                RedisJsonValue::String("Catania".into()),
            ];
            let input = GeoaddInput::decode(args).unwrap();
            assert_eq!(input.position.len(), 2);
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
            ];
            let err = GeoaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_triplet() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(13.361389),
                RedisJsonValue::from(38.115556),
                RedisJsonValue::String("Palermo".into()),
                RedisJsonValue::from(15.087269),
            ];
            let err = GeoaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("must be in triplets"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: None,
                ch: None,
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mygeo".into()));
        }

        #[test]
        fn test_kind() {
            let input = GeoaddInput {
                key: RedisKey::String("mygeo".into()),
                options: None,
                ch: None,
                position: vec![Position {
                    longitude: RedisJsonValue::from(13.361389),
                    latitude: RedisJsonValue::from(38.115556),
                    member: RedisJsonValue::from("Palermo"),
                }],
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Geoadd);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geoadd_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GeoaddInput {
                                key: RedisKey::String("locations".into()),
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
                        .expect("raw failed");

                    // GEOADD returns number of elements added
                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geoadd_multiple() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GeoaddInput {
                                key: RedisKey::String("cities".into()),
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
                        .expect("raw failed");

                    assert!(!result.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_geoadd_with_nx() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Add initially
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("nx_test".into()),
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
                    .expect("raw failed");

                    // Try to add same member with NX (should not update)
                    let result = ctx
                        .raw(
                            &GeoaddInput {
                                key: RedisKey::String("nx_test".into()),
                                options: Some(Options::NX),
                                ch: None,
                                position: vec![Position {
                                    longitude: RedisJsonValue::from(14.0),
                                    latitude: RedisJsonValue::from(39.0),
                                    member: RedisJsonValue::from("Palermo"),
                                }],
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
    }
}
