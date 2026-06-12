use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod bitcount;
mod bitfield;
mod bitfield_ro;
mod bitop;
mod bitpos;
mod getbit;
mod setbit;

pub use bitcount::*;
pub use bitfield::*;
pub use bitfield_ro::*;
pub use bitop::*;
pub use bitpos::*;
pub use getbit::*;
pub use setbit::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct BitcountRange {
    pub start: RedisJsonValue,
    pub end: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<BitMode>,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum BitMode {
    #[default]
    BYTE,
    BIT,
}

/// Individual BITFIELD operation
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum BitfieldOp {
    Get {
        encoding: RedisJsonValue,
        offset: RedisJsonValue,
    },
    Set {
        encoding: RedisJsonValue,
        offset: RedisJsonValue,
        value: RedisJsonValue,
    },
    Incrby {
        encoding: RedisJsonValue,
        offset: RedisJsonValue,
        increment: RedisJsonValue,
    },
    Overflow(OverflowBehavior),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum OverflowBehavior {
    #[default]
    WRAP,
    SAT,
    FAIL,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct BitfieldRoGet {
    pub encoding: RedisJsonValue,
    pub offset: RedisJsonValue,
}

/// Bitwise operation type for BITOP command
#[derive(Debug, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum BitopOperation {
    #[default]
    AND,
    OR,
    XOR,
    NOT,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct BitposRange {
    pub start: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<BitMode>,
}
