use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod blmove;
mod blmpop;
mod blpop;
mod brpop;
mod brpoplpush;
mod lindex;
mod linsert;
mod llen;
mod lmove;
mod lmpop;
mod lpop;
mod lpos;
mod lpush;
mod lpushx;
mod lrange;
mod lrem;
mod lset;
mod ltrim;
mod rpop;
mod rpoplpush;
pub(crate) mod rpush;
mod rpushx;

pub use blmove::*;
pub use blmpop::*;
pub use blpop::*;
pub use brpop::*;
pub use brpoplpush::*;
use error::EpError;
pub use lindex::*;
pub use linsert::*;
pub use llen::*;
pub use lmove::*;
pub use lmpop::*;
pub use lpop::*;
pub use lpos::*;
pub use lpush::*;
pub use lpushx::*;
pub use lrange::*;
pub use lrem::*;
pub use lset::*;
pub use ltrim::*;
pub use rpop::*;
pub use rpoplpush::*;
pub use rpush::*;
pub use rpushx::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Copy, Default, ToSchema, JsonSchema, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Left,
    Right,
}

impl TryFrom<RedisJsonValue> for Direction {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        if let RedisJsonValue::String(string) = value {
            match string.to_uppercase().as_str() {
                "LEFT" => Ok(Self::Left),
                "RIGHT" => Ok(Self::Right),
                _ => Err(EpError::parse(format!("Direction must be LEFT or RIGHT, got {}", string))),
            }
        } else {
            Err(EpError::parse("Expected string for direction"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Copy, Default, ToSchema, JsonSchema)]
pub enum Traverse {
    #[default]
    Before,
    After,
}

impl TryFrom<RedisJsonValue> for Traverse {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        if let RedisJsonValue::String(string) = value {
            match string.to_lowercase().as_str() {
                "before" => Ok(Self::Before),
                "after" => Ok(Self::After),
                _ => Err(EpError::parse(format!("Failed to parse traverse from {}, expected BEFORE or AFTER", string))),
            }
        } else {
            Err(EpError::parse("Expected string for traverse direction"))
        }
    }
}
