use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

pub mod acl;
mod bgrewriteaof;
mod bgsave;
mod command;
mod config;
mod dbsize;
mod failover;
mod flushall;
mod flushdb;
mod info;
mod lastsave;
mod latency;
mod lolwut;
mod memory;
mod module;
mod monitor;
mod psync;
mod replconf;
mod replicaof;
mod replication_common;
mod restore_asking;
mod role;
mod save;
mod shutdown;
#[allow(deprecated)]
mod slaveof;
mod slowlog;
mod swapdb;
mod sync;
mod time;

pub use acl::*;
pub use bgrewriteaof::*;
pub use bgsave::*;
pub use command::*;
pub use config::*;
pub use dbsize::*;
pub use failover::*;
pub use flushall::*;
pub use flushdb::*;
pub use info::*;
pub use lastsave::*;
pub use latency::*;
pub use lolwut::*;
pub use memory::*;
pub use module::*;
pub use monitor::*;
pub use psync::*;
pub use replconf::*;
pub use replicaof::*;
pub use restore_asking::*;
pub use role::*;
pub use save::*;
pub use shutdown::*;
#[allow(deprecated)]
pub use slaveof::*;
pub use slowlog::*;
pub use swapdb::*;
pub use sync::*;
pub use time::*;

/// Flush mode for FLUSHALL command
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Mode {
    /// Flush synchronously (blocking)
    #[default]
    SYNC,
    /// Flush asynchronously (non-blocking)
    ASYNC,
}
