#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod geoadd;
mod geodist;
mod geohash;
mod geopos;
mod georadius;
mod georadius_ro;
mod georadiusbymember;
mod georadiusbymember_ro;
mod geosearch;
mod geosearchstore;

pub use geoadd::*;
pub use geodist::*;
pub use geohash::*;
pub use geopos::*;
pub use georadius::*;
pub use georadius_ro::*;
pub use georadiusbymember::*;
pub use georadiusbymember_ro::*;
pub use geosearch::*;
pub use geosearchstore::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub(crate) struct Position {
    pub(crate) longitude: RedisJsonValue,
    pub(crate) latitude: RedisJsonValue,
    pub(crate) member: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default, Clone, ToSchema, JsonSchema)]
pub(crate) enum Options {
    #[default]
    NX,
    XX,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default, Clone, ToSchema, JsonSchema)]
enum Unit {
    #[default]
    M,
    KM,
    FT,
    MI,
}

impl TryFrom<RedisJsonValue> for Unit {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, EpError> {
        match value {
            RedisJsonValue::String(string) => match string.as_str() {
                "M" => Ok(Unit::M),
                "KM" => Ok(Unit::KM),
                "FT" => Ok(Unit::FT),
                "MI" => Ok(Unit::MI),
                _ => Err(EpError::parse(format!("Unexpected unit provided: {string}"))),
            },
            _ => Err(EpError::parse("Expected unit to be a string")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
enum Store {
    STORE(RedisJsonValue),
    STOREDIST(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Count {
    count: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    any: Option<bool>,
}

impl Count {
    fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("COUNT").arg(&self.count);

        if let Some(any) = &self.any
            && *any
        {
            command.arg("ANY");
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Default, Clone, ToSchema, JsonSchema)]
enum Sort {
    #[default]
    ASC,
    DESC,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
enum By {
    BYRADIUS(Radius),
    BYBOX(Bx),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
#[allow(private_interfaces)]
pub struct Bx {
    pub(crate) width: RedisJsonValue,
    pub(crate) height: RedisJsonValue,
    pub(crate) unit: Unit,
}

impl Bx {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("BYBOX").arg(&self.width).arg(&self.height);

        match &self.unit {
            Unit::FT => command.arg("FT"),
            Unit::KM => command.arg("KM"),
            Unit::M => command.arg("M"),
            Unit::MI => command.arg("MI"),
        };
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
#[allow(private_interfaces)]
pub struct Radius {
    pub(crate) radius: RedisJsonValue,
    pub(crate) unit: Unit,
}

impl Radius {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("BYRADIUS").arg(&self.radius);
        match self.unit {
            Unit::M => command.arg("M"),
            Unit::KM => command.arg("KM"),
            Unit::FT => command.arg("FT"),
            Unit::MI => command.arg("MI"),
        };
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
enum From {
    FROMMEMBER(RedisJsonValue),
    FROMLONLOAT(Pos),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
struct Pos {
    lon: RedisJsonValue,
    lat: RedisJsonValue,
}
