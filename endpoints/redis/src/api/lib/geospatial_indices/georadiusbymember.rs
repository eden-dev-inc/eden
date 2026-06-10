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

const API_INFO: ApiInfo<RedisApi, GeoradiusbymemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Georadiusbymember,
    "Queries a geospatial index of members within a distance from a member, optionally stores the result",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEORADIUSBYMEMBER`
/// https://redis.io/docs/latest/commands/georadiusbymember/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeoradiusbymemberInput {
    key: RedisKey,
    member: RedisJsonValue,
    radius: RedisJsonValue,
    unit: Unit,
    with_cord: Option<bool>,
    with_dist: Option<bool>,
    with_hash: Option<bool>,
    count: Option<Count>,
    sort: Option<Sort>,
    store: Option<Store>,
}

impl Serialize for GeoradiusbymemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5; // type, key,  member, radius, unit
        if self.with_cord.is_some() {
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

        let mut state = serializer.serialize_struct("GeoradiusbymemberInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("member", &self.member)?;
        state.serialize_field("radius", &self.radius)?;
        state.serialize_field("unit", &self.unit)?;

        if let Some(with_cord) = &self.with_cord {
            state.serialize_field("with_cord", with_cord)?;
        }
        if let Some(with_dist) = &self.with_dist {
            state.serialize_field("with_dist", with_dist)?;
        }
        if let Some(with_hash) = &self.with_hash {
            state.serialize_field("with_hash", with_hash)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", &count.count)?;
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
    GeoradiusbymemberInput,
    API_INFO,
    {key, member, radius, unit, with_cord, with_dist, with_hash, count, sort, store}
);

impl RedisCommandInput for GeoradiusbymemberInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.member).arg(&self.radius);

        match &self.unit {
            Unit::M => command.arg("M"),
            Unit::KM => command.arg("KM"),
            Unit::FT => command.arg("FT"),
            Unit::MI => command.arg("MI"),
        };

        if let Some(with_cord) = self.with_cord
            && with_cord
        {
            command.arg("WITHCORD");
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
        if args.len() < 4 {
            return Err(EpError::request(format!("GEORADIUSBYMEMBER requires at least 4 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let member = args[1].clone();
        let radius = args[2].clone();

        // Parse unit
        let unit = if let RedisJsonValue::String(s) = &args[3] {
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

        let mut with_cord = None;
        let mut with_dist = None;
        let mut with_hash = None;
        let mut count = None;
        let mut sort = None;
        let mut store = None;
        let mut i = 4;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "WITHCOORD" => {
                        with_cord = Some(true);
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
            member,
            radius,
            unit,
            with_cord,
            with_dist,
            with_hash,
            count,
            sort,
            store,
        })
    }
}
