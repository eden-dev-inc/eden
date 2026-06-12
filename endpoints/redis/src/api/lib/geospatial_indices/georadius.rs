use crate::api::lib::geospatial_indices::{Count, Sort, Store, Unit};
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

const API_INFO: ApiInfo<RedisApi, GeoradiusInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Georadius,
    "Queries a geospatial index for members within a distance from a coordinate, optionally stores the result",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEORADIUS`
/// https://redis.io/docs/latest/commands/georadius/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeoradiusInput {
    key: RedisKey,
    longitude: RedisJsonValue,
    latitude: RedisJsonValue,
    radius: RedisJsonValue,
    unit: Unit,
    with_coord: Option<bool>,
    with_dist: Option<bool>,
    with_hash: Option<bool>,
    count: Option<Count>,
    sort: Option<Sort>,
    store: Option<Store>,
}

impl Serialize for GeoradiusInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 6; // type, key, longitude, latitude, radius, unit
        if self.with_coord.is_some() {
            fields += 1;
        }
        if self.with_dist.is_some() {
            fields += 1;
        }
        if self.with_hash.is_some() {
            fields += 1;
        }
        if let Some(count) = &self.count {
            fields += 1;
            if count.any.is_some() {
                fields += 1;
            }
        }
        if self.sort.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeoradiusInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("longitude", &self.longitude)?;
        state.serialize_field("latitude", &self.latitude)?;
        state.serialize_field("radius", &self.radius)?;
        state.serialize_field("unit", &self.unit)?;

        if let Some(with_coord) = &self.with_coord {
            state.serialize_field("with_coord", with_coord)?;
        }
        if let Some(with_dist) = &self.with_dist {
            state.serialize_field("with_dist", with_dist)?;
        }
        if let Some(with_hash) = &self.with_hash {
            state.serialize_field("with_hash", with_hash)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", &count)?;
            if let Some(any) = &count.any {
                state.serialize_field("any", any)?;
            }
        }
        if let Some(sort) = &self.sort {
            state.serialize_field("sort", sort)?;
        }
        if let Some(store) = &self.store {
            match store {
                Store::STORE(store) => {
                    state.serialize_field("store", store)?;
                }
                Store::STOREDIST(store) => {
                    state.serialize_field("storedist", store)?;
                }
            }
        }
        state.end()
    }
}

impl_redis_operation!(
    GeoradiusInput,
    API_INFO,
    {key, longitude, latitude, radius, unit, with_coord, with_dist, with_hash, count, sort, store}
);

impl RedisCommandInput for GeoradiusInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.longitude).arg(&self.latitude).arg(&self.radius);

        match &self.unit {
            Unit::M => command.arg("M"),
            Unit::KM => command.arg("KM"),
            Unit::FT => command.arg("FT"),
            Unit::MI => command.arg("MI"),
        };

        if let Some(with_coord) = self.with_coord
            && with_coord
        {
            command.arg("WITHCOORD");
        }

        if let Some(with_dist) = self.with_dist
            && with_dist
        {
            command.arg("WITHDIST");
        }

        if let Some(with_hash) = self.with_hash
            && with_hash
        {
            command.arg("WITHHASH");
        }

        if let Some(count) = &self.count {
            count.cmd(&mut command);
        }

        if let Some(sort) = &self.sort {
            match sort {
                Sort::ASC => command.arg("ASC"),
                Sort::DESC => command.arg("DESC"),
            };
        }

        if let Some(store) = &self.store {
            match store {
                Store::STORE(k) => {
                    command.arg("STORE").arg(k);
                }
                Store::STOREDIST(k) => {
                    command.arg("STOREDIST").arg(k);
                }
            }
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request(format!("GEORADIUS requires at least 5 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let longitude = args[1].clone();
        let latitude = args[2].clone();
        let radius = args[3].clone();

        // Parse unit
        let unit = if let RedisJsonValue::String(s) = &args[4] {
            match s.to_uppercase().as_str() {
                "M" => Unit::M,
                "KM" => Unit::KM,
                "FT" => Unit::FT,
                "MI" => Unit::MI,
                _ => Unit::M, // default
            }
        } else {
            Unit::M
        };

        let mut with_coord = None;
        let mut with_dist = None;
        let mut with_hash = None;
        let mut count = None;
        let mut sort = None;
        let mut store = None;
        let mut i = 5;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "WITHCOORD" => {
                        with_coord = Some(true);
                        i += 1;
                    }
                    "WITHDIST" => {
                        with_dist = Some(true);
                        i += 1;
                    }
                    "WITHHASH" => {
                        with_hash = Some(true);
                        i += 1;
                    }
                    "COUNT" => {
                        if i + 1 < args.len() {
                            let count_value = args[i + 1].clone();
                            let mut any = None;
                            i += 2;

                            // Check for ANY
                            if i < args.len()
                                && let RedisJsonValue::String(s) = &args[i]
                                && s.to_uppercase() == "ANY"
                            {
                                any = Some(true);
                                i += 1;
                            }

                            count = Some(Count { count: count_value, any });
                        } else {
                            i += 1;
                        }
                    }
                    "ASC" => {
                        sort = Some(Sort::ASC);
                        i += 1;
                    }
                    "DESC" => {
                        sort = Some(Sort::DESC);
                        i += 1;
                    }
                    "STORE" => {
                        if i + 1 < args.len() {
                            store = Some(Store::STORE(args[i + 1].clone()));
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "STOREDIST" => {
                        if i + 1 < args.len() {
                            store = Some(Store::STOREDIST(args[i + 1].clone()));
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        Ok(Self {
            key,
            longitude,
            latitude,
            radius,
            unit,
            with_coord,
            with_dist,
            with_hash,
            count,
            sort,
            store,
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
            let input = GeoradiusInput {
                key: RedisKey::String("mygeo".into()),
                longitude: RedisJsonValue::from(15.0),
                latitude: RedisJsonValue::from(37.0),
                radius: RedisJsonValue::from(200.0),
                unit: Unit::KM,
                with_coord: None,
                with_dist: None,
                with_hash: None,
                count: None,
                sort: None,
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GEORADIUS"));
            assert!(cmd_str.contains("mygeo"));
            assert!(cmd_str.contains("KM"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = GeoradiusInput {
                key: RedisKey::String("mygeo".into()),
                longitude: RedisJsonValue::from(15.0),
                latitude: RedisJsonValue::from(37.0),
                radius: RedisJsonValue::from(200.0),
                unit: Unit::M,
                with_coord: Some(true),
                with_dist: Some(true),
                with_hash: Some(true),
                count: Some(Count { count: RedisJsonValue::from(10), any: Some(true) }),
                sort: Some(Sort::ASC),
                store: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHCOORD"));
            assert!(cmd_str.contains("WITHDIST"));
            assert!(cmd_str.contains("WITHHASH"));
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("ANY"));
            assert!(cmd_str.contains("ASC"));
        }

        #[test]
        fn test_encode_command_with_store() {
            let input = GeoradiusInput {
                key: RedisKey::String("mygeo".into()),
                longitude: RedisJsonValue::from(15.0),
                latitude: RedisJsonValue::from(37.0),
                radius: RedisJsonValue::from(200.0),
                unit: Unit::KM,
                with_coord: None,
                with_dist: None,
                with_hash: None,
                count: None,
                sort: None,
                store: Some(Store::STORE(RedisJsonValue::from("result"))),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("STORE"));
            assert!(cmd_str.contains("result"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(15.0),
                RedisJsonValue::from(37.0),
                RedisJsonValue::from(200.0),
                RedisJsonValue::String("KM".into()),
            ];
            let input = GeoradiusInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mygeo".into()));
            assert!(matches!(input.unit, Unit::KM));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mygeo".into()),
                RedisJsonValue::from(15.0),
                RedisJsonValue::from(37.0),
                RedisJsonValue::from(200.0),
                RedisJsonValue::String("M".into()),
                RedisJsonValue::String("WITHCOORD".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::from(5),
                RedisJsonValue::String("ASC".into()),
            ];
            let input = GeoradiusInput::decode(args).unwrap();
            assert_eq!(input.with_coord, Some(true));
            assert!(input.count.is_some());
            assert!(matches!(input.sort, Some(Sort::ASC)));
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mygeo".into()), RedisJsonValue::from(15.0)];
            let err = GeoradiusInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 5 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GeoradiusInput {
                key: RedisKey::String("mygeo".into()),
                longitude: RedisJsonValue::from(15.0),
                latitude: RedisJsonValue::from(37.0),
                radius: RedisJsonValue::from(200.0),
                unit: Unit::M,
                with_coord: None,
                with_dist: None,
                with_hash: None,
                count: None,
                sort: None,
                store: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mygeo".into()));
        }

        #[test]
        fn test_kind() {
            let input = GeoradiusInput {
                key: RedisKey::String("mygeo".into()),
                longitude: RedisJsonValue::from(15.0),
                latitude: RedisJsonValue::from(37.0),
                radius: RedisJsonValue::from(200.0),
                unit: Unit::M,
                with_coord: None,
                with_dist: None,
                with_hash: None,
                count: None,
                sort: None,
                store: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Georadius);
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
        async fn test_georadius_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup
                    ctx.raw(
                        &GeoaddInput {
                            key: RedisKey::String("georadius_test".into()),
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
                            &GeoradiusInput {
                                key: RedisKey::String("georadius_test".into()),
                                longitude: RedisJsonValue::from(15.0),
                                latitude: RedisJsonValue::from(37.0),
                                radius: RedisJsonValue::from(200.0),
                                unit: Unit::KM,
                                with_coord: None,
                                with_dist: None,
                                with_hash: None,
                                count: None,
                                sort: None,
                                store: None,
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
