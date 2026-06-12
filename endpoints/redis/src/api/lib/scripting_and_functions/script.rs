#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod script_debug;
mod script_exists;
mod script_flush;
mod script_kill;
pub(crate) mod script_load;

pub use script_debug::*;
pub use script_exists::*;
pub use script_flush::*;
pub use script_kill::*;
pub use script_load::*;

/// Debug mode for SCRIPT DEBUG command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum DebugMode {
    /// Enable non-blocking asynchronous debugging of Lua scripts (changes are rolled back)
    YES,
    /// Enable blocking synchronous debugging of Lua scripts (changes are retained)
    SYNC,
    /// Disable debugging (default)
    #[default]
    NO,
}

/// Flush mode for SCRIPT FLUSH command
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "UPPERCASE")]
pub enum FlushMode {
    /// Default behavior (determined by lazyfree-lazy-user-flush config).
    /// Does not send ASYNC/SYNC argument, compatible with Redis < 6.2.
    #[default]
    Default,
    /// Flush asynchronously (Redis 6.2+)
    ASYNC,
    /// Flush synchronously (Redis 6.2+)
    SYNC,
}
