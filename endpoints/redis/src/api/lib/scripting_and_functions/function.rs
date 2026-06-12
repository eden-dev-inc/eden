use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod function_delete;
mod function_dump;
mod function_flush;
mod function_kill;
mod function_list;
mod function_load;
mod function_restore;
mod function_stats;

pub use function_delete::*;
pub use function_dump::*;
pub use function_flush::*;
pub use function_kill::*;
pub use function_list::*;
pub use function_load::*;
pub use function_restore::*;
pub use function_stats::*;

/// Flush mode for FUNCTION FLUSH command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum FlushMode {
    /// Synchronous flush (default) - blocks until complete
    #[default]
    SYNC,
    /// Asynchronous flush - returns immediately, flush happens in background
    ASYNC,
}

/// Information about a function within a library
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct FunctionInfo {
    pub name: String,
    pub description: Option<String>,
    pub flags: Vec<String>,
}

/// Information about a library
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct LibraryInfo {
    pub library_name: String,
    pub engine: String,
    pub functions: Vec<FunctionInfo>,
    pub library_code: Option<String>,
}
