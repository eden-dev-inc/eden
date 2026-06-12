use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod bf_add;
mod bf_card;
mod bf_exists;
mod bf_info;
mod bf_insert;
mod bf_loadchunk;
mod bf_madd;
mod bf_mexists;
mod bf_reserve;
mod bf_scandump;

pub use bf_add::*;
pub use bf_card::*;
pub use bf_exists::*;
pub use bf_info::*;
pub use bf_insert::*;
pub use bf_loadchunk::*;
pub use bf_madd::*;
pub use bf_mexists::*;
pub use bf_reserve::*;
pub use bf_scandump::*;
use error::EpError;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema, PartialEq)]
pub enum BfInfoArg {
    #[default]
    CAPACITY,
    SIZE,
    FILTERS,
    ITEMS,
    EXPANSION,
}

impl TryFrom<RedisJsonValue> for BfInfoArg {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "CAPACITY" => Ok(BfInfoArg::CAPACITY),
                "SIZE" => Ok(BfInfoArg::SIZE),
                "FILTERS" => Ok(BfInfoArg::FILTERS),
                "ITEMS" => Ok(BfInfoArg::ITEMS),
                "EXPANSION" => Ok(BfInfoArg::EXPANSION),
                _ => Err(EpError::parse(format!("Unknown BF.INFO argument: {}", s))),
            },
            _ => Err(EpError::parse("BF.INFO argument must be a string")),
        }
    }
}
