use crate::api::{key::RedisKey, value::RedisJsonValue};
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod json_arrappend;
mod json_arrindex;
mod json_arrinsert;
mod json_arrlen;
mod json_arrpop;
mod json_arrtrim;
mod json_clear;
mod json_debug;
mod json_debug_memory;
mod json_del;
mod json_forget;
mod json_get;
mod json_merge;
mod json_mget;
mod json_mset;
mod json_numincrby;
mod json_nummultby;
mod json_objkeys;
mod json_objlen;
mod json_resp;
mod json_set;
mod json_strappend;
mod json_strlen;
mod json_toggle;
mod json_type;

pub use json_arrappend::*;
pub use json_arrindex::*;
pub use json_arrinsert::*;
pub use json_arrlen::*;
pub use json_arrpop::*;
pub use json_arrtrim::*;
pub use json_clear::*;
pub use json_debug::*;
pub use json_debug_memory::*;
pub use json_del::*;
pub use json_forget::*;
pub use json_get::*;
pub use json_merge::*;
pub use json_mget::*;
pub use json_mset::*;
pub use json_numincrby::*;
pub use json_nummultby::*;
pub use json_objkeys::*;
pub use json_objlen::*;
pub use json_resp::*;
pub use json_set::*;
pub use json_strappend::*;
pub use json_strlen::*;
pub use json_toggle::*;
pub use json_type::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Range {
    pub start: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<RedisJsonValue>,
}

impl Range {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.start);
        if let Some(stop) = &self.stop {
            command.arg(stop);
        }
    }
}

#[derive(Debug, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct PathWithIndex {
    pub path: RedisJsonValue,
    pub index: Option<RedisJsonValue>,
}

impl PathWithIndex {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.path);
        if let Some(index) = &self.index {
            command.arg(index);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonMsetEntry {
    pub(crate) key: RedisKey,
    pub(crate) path: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl JsonMsetEntry {
    pub fn new(key: RedisKey, path: RedisJsonValue, value: RedisJsonValue) -> Self {
        Self { key, path, value }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Options {
    #[default]
    NX,
    XX,
}

impl TryFrom<RedisJsonValue> for Options {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(string) => match string.to_uppercase().as_str() {
                "NX" => Ok(Self::NX),
                "XX" => Ok(Self::XX),
                _ => Err(EpError::parse(format!("Invalid option: {}. Must be NX or XX", string))),
            },
            _ => Err(EpError::parse("Expected string for option")),
        }
    }
}

/// Output for Redis JSON.SET command
///
/// Returns OK if the value was set, or Nil if the operation was aborted
/// due to NX/XX conditions not being met.
#[derive(Debug, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum JsonSetResult {
    Ok,
    Nil,
}
