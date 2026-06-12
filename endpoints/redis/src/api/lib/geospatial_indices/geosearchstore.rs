use crate::api::lib::geospatial_indices::{Bx, By, Count, From, Pos, Radius, Sort, Unit};
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

const API_INFO: ApiInfo<RedisApi, GeosearchstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Geosearchstore,
    "Queries a geospatial index for members inside an area of a box or a circle, optionally stores the result",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEOSEARCHSTORE`
/// https://redis.io/docs/latest/commands/geosearchstore/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeosearchstoreInput {
    destination: RedisKey,
    source: RedisKey,
    from: From,
    by: By,
    sort: Option<Sort>,
    count: Option<Count>,
    store_dist: Option<bool>,
}

impl Serialize for GeosearchstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 6; // type, destination, source, from, by, radius
        if self.sort.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.store_dist.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeosearchstoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("source", &self.source)?;
        match &self.from {
            From::FROMLONLOAT(from) => state.serialize_field("fromlonloat", &from)?,
            From::FROMMEMBER(from) => state.serialize_field("frommember", &from)?,
        }
        match &self.by {
            By::BYBOX(by) => state.serialize_field("bybox", &by)?,
            By::BYRADIUS(by) => state.serialize_field("byradius", &by)?,
        }

        if let Some(sort) = &self.sort {
            state.serialize_field("sort", sort)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(store_dist) = &self.store_dist {
            state.serialize_field("store_dist", store_dist)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    GeosearchstoreInput,
    API_INFO,
    {destination, source, from, by, sort, count, store_dist}
);

impl RedisCommandInput for GeosearchstoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.destination.clone(), self.source.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination).arg(&self.source);

        match &self.from {
            From::FROMMEMBER(m) => command.arg("FROMMEMBER").arg(m),
            From::FROMLONLOAT(pos) => command.arg("FROMLONLOAT").arg(&pos.lon).arg(&pos.lat),
        };

        match &self.by {
            By::BYRADIUS(radius) => {
                command.arg("BYRADIUS").arg(&radius.radius);
                match radius.unit {
                    Unit::M => command.arg("M"),
                    Unit::KM => command.arg("KM"),
                    Unit::FT => command.arg("FT"),
                    Unit::MI => command.arg("MI"),
                };
            }
            By::BYBOX(bx) => {
                command.arg("BYBOX").arg(&bx.width).arg(&bx.height);
                match bx.unit {
                    Unit::M => command.arg("M"),
                    Unit::KM => command.arg("KM"),
                    Unit::FT => command.arg("FT"),
                    Unit::MI => command.arg("MI"),
                };
            }
        }

        if let Some(sort) = &self.sort {
            match sort {
                Sort::ASC => command.arg("ASC"),
                Sort::DESC => command.arg("DESC"),
            };
        }

        if let Some(count) = &self.count {
            count.cmd(&mut command);
        }

        if let Some(store_dist) = &self.store_dist
            && *store_dist
        {
            command.arg("STOREDIST");
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 6 {
            return Err(EpError::request("GEOSEARCHSTORE requires at least 6 arguments"));
        }

        let destination = args[0].clone().try_into()?;
        let source = args[1].clone().try_into()?;

        let mut i = 2;
        let mut from = None;
        let mut by = None;
        let mut sort = None;
        let mut count = None;
        let mut store_dist = None;

        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "FROMMEMBER" if i + 1 < args.len() => {
                        from = Some(From::FROMMEMBER(args[i + 1].clone()));
                        i += 2;
                    }
                    "FROMLONLAT" if i + 2 < args.len() => {
                        from = Some(From::FROMLONLOAT(Pos { lon: args[i + 1].clone(), lat: args[i + 2].clone() }));
                        i += 3;
                    }
                    "BYRADIUS" if i + 2 < args.len() => {
                        let radius_val = args[i + 1].clone();
                        let unit = Unit::try_from(args[i + 2].clone())?;
                        by = Some(By::BYRADIUS(Radius { radius: radius_val, unit }));
                        i += 3;
                    }
                    "BYBOX" if i + 3 < args.len() => {
                        let width = args[i + 1].clone();
                        let height = args[i + 2].clone();
                        let unit = Unit::try_from(args[i + 3].clone())?;
                        by = Some(By::BYBOX(Bx { width, height, unit }));
                        i += 4;
                    }
                    "ASC" => {
                        sort = Some(Sort::ASC);
                        i += 1;
                    }
                    "DESC" => {
                        sort = Some(Sort::DESC);
                        i += 1;
                    }
                    "COUNT" if i + 1 < args.len() => {
                        let count_val = args[i + 1].clone();
                        let mut any = None;
                        if i + 2 < args.len()
                            && let RedisJsonValue::String(s) = &args[i + 2]
                            && s.to_uppercase() == "ANY"
                        {
                            any = Some(true);
                            i += 1;
                        }
                        count = Some(Count { count: count_val, any });
                        i += 2;
                    }
                    "STOREDIST" => {
                        store_dist = Some(true);
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        let from = from.ok_or_else(|| EpError::parse("FROM clause is required"))?;
        let by = by.ok_or_else(|| EpError::parse("BY clause is required"))?;

        Ok(Self { destination, source, from, by, sort, count, store_dist })
    }
}
