use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod module_list;
mod module_load;
mod module_loadex;
mod module_unload;

pub use module_list::*;
pub use module_load::*;
pub use module_loadex::*;
pub use module_unload::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Config {
    pub(crate) name: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}
