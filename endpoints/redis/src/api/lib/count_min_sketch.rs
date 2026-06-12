use crate::api::value::RedisJsonValue;
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod cms_incrby;
mod cms_info;
mod cms_initbydim;
mod cms_initbyprob;
mod cms_merge;
mod cms_query;

pub use cms_incrby::*;
pub use cms_info::*;
pub use cms_initbydim::*;
pub use cms_initbyprob::*;
pub use cms_merge::*;
pub use cms_query::*;

/// An item/increment pair for CMS.INCRBY
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Incrby {
    /// The item to increment
    pub item: RedisJsonValue,
    /// The amount to increment by
    pub increment: RedisJsonValue,
}
