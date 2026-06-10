use crate::api::lib::geospatial_indices::{By, Count, From, Pos, Sort, Unit};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Bx, Radius, key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, GeosearchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Geosearch,
    "Queries a geospatial index for members inside an area of a box or a circle",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEOSEARCH`
/// https://redis.io/docs/latest/commands/geosearch/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeosearchInput {
    key: RedisKey,
    from: From,
    by: By,
    radius: RedisJsonValue,
    sort: Option<Sort>,
    count: Option<Count>,
    with_coord: Option<bool>,
    with_dist: Option<bool>,
    with_hash: Option<bool>,
}

impl Serialize for GeosearchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5; // type, key, from, by, radius
        if self.sort.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.with_coord.is_some() {
            fields += 1;
        }
        if self.with_dist.is_some() {
            fields += 1;
        }
        if self.with_hash.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeosearchInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("from", &self.from)?;
        state.serialize_field("by", &self.by)?;
        state.serialize_field("radius", &self.radius)?;

        if let Some(sort) = &self.sort {
            state.serialize_field("sort", sort)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(with_coord) = &self.with_coord {
            state.serialize_field("with_coord", with_coord)?;
        }
        if let Some(with_dist) = &self.with_dist {
            state.serialize_field("with_dist", with_dist)?;
        }
        if let Some(with_hash) = &self.with_hash {
            state.serialize_field("with_hash", with_hash)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    GeosearchInput,
   API_INFO,
    {key, from, by, radius, sort, count, with_coord, with_dist, with_hash}
);

impl RedisCommandInput for GeosearchInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        match &self.from {
            From::FROMMEMBER(m) => command.arg("FROMMEMBER").arg(m),
            From::FROMLONLOAT(pos) => command.arg("FROMLONLOAT").arg(&pos.lon).arg(&pos.lat),
        };

        match &self.by {
            By::BYRADIUS(radius) => {
                radius.cmd(&mut command);
            }
            By::BYBOX(bx) => {
                bx.cmd(&mut command);
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

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request("GEOSEARCH requires at least 5 arguments"));
        }

        let key = args[0].clone().try_into()?;
        let mut i = 1;
        let mut from = None;
        let mut by = None;
        let mut sort = None;
        let mut count = None;
        let mut with_coord = None;
        let mut with_dist = None;
        let mut with_hash = None;

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
                        let any = if i + 2 < args.len() {
                            if let RedisJsonValue::String(s) = &args[i + 2] {
                                if s.to_uppercase() == "ANY" {
                                    i += 1;
                                    Some(true)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        count = Some(Count { count: count_val, any });
                        i += 2;
                    }
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
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        let from = from.ok_or_else(|| EpError::parse("FROM clause is required"))?;
        let by = by.ok_or_else(|| EpError::parse("BY clause is required"))?;

        Ok(Self {
            key,
            from,
            by,
            radius: RedisJsonValue::String("".to_string()), // This field seems unused
            sort,
            count,
            with_coord,
            with_dist,
            with_hash,
        })
    }
}
