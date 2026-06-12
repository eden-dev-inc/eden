use schemars::JsonSchema;
use serde::Deserialize;
use std::fmt::Debug;
use utoipa::ToSchema;

mod pfadd;
mod pfcount;
mod pfdebug;
mod pfmerge;
mod pfselftest;

pub use pfadd::*;
pub use pfcount::*;
pub use pfdebug::*;
pub use pfmerge::*;
pub use pfselftest::*;

/// The possible response types from PFDEBUG
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum PfdebugValue {
    /// String response (from DECODE or ENCODING subcommands)
    String(String),
    /// Integer response (from TODENSE subcommand)
    Integer(i64),
    /// Array response (register dump)
    Array(Vec<i64>),
}
