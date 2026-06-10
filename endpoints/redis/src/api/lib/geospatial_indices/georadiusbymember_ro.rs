use crate::api::lib::geospatial_indices::{Count, Sort, Unit};
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

const API_INFO: ApiInfo<RedisApi, GeoradiusbymemberRoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::GeoradiusbymemberRo,
    "Returns members from a geospatial index that are within a distance from a member",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GEORADIUSBYMEMBER_RO`
/// https://redis.io/docs/latest/commands/georadiusbymember_ro/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GeoradiusbymemberRoInput {
    key: RedisKey,
    member: RedisJsonValue,
    radius: RedisJsonValue,
    unit: Unit,
    with_cord: Option<bool>,
    with_dist: Option<bool>,
    with_hash: Option<bool>,
    count: Option<Count>,
    sort: Option<Sort>,
}

impl Serialize for GeoradiusbymemberRoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5; // type, key, member, radius, unit
        if self.with_cord.is_some() {
            fields += 1;
        }
        if self.with_dist.is_some() {
            fields += 1;
        }
        if self.with_hash.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.sort.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GeoradiusbymemberRoInput", fields)?;
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
            state.serialize_field("count", count)?;
        }
        if let Some(sort) = &self.sort {
            state.serialize_field("sort", sort)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    GeoradiusbymemberRoInput,
    API_INFO,
    {key, member, radius, unit, with_cord, with_dist, with_hash, count, sort}
);

impl RedisCommandInput for GeoradiusbymemberRoInput {
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

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request("GEORADIUSBYMEMBER_RO requires at least 4 arguments"));
        }

        let unit = match &args[3] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "M" => Unit::M,
                "KM" => Unit::KM,
                "FT" => Unit::FT,
                "MI" => Unit::MI,
                _ => return Err(EpError::parse("Invalid unit")),
            },
            _ => return Err(EpError::parse("Unit must be string")),
        };

        let mut with_cord = None;
        let mut with_dist = None;
        let mut with_hash = None;
        let mut count = None;
        let mut sort = None;

        let mut i = 4;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
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
                    "ASC" => {
                        sort = Some(Sort::ASC);
                        i += 1;
                    }
                    "DESC" => {
                        sort = Some(Sort::DESC);
                        i += 1;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            member: args[1].clone(),
            radius: args[2].clone(),
            unit,
            with_cord,
            with_dist,
            with_hash,
            count,
            sort,
        })
    }
}
