use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

pub(crate) mod copy;
pub(crate) mod del;
pub(crate) mod dump;
pub(crate) mod exists;
pub(crate) mod expire;
pub(crate) mod expireat;
pub(crate) mod expiretime;
pub(crate) mod keys;
pub(crate) mod migrate;
pub(crate) mod r#move;
pub(crate) mod object_encoding;
pub(crate) mod object_freq;
pub(crate) mod object_idletime;
pub(crate) mod object_refcount;
pub(crate) mod persist;
pub(crate) mod pexpire;
pub(crate) mod pexpireat;
pub(crate) mod pexpiretime;
pub(crate) mod pttl;
pub(crate) mod randomkey;
pub(crate) mod rename;
pub(crate) mod renamenx;
pub(crate) mod restore;
pub(crate) mod scan;
pub(crate) mod sort;
pub(crate) mod sort_common;
pub(crate) mod sort_ro;
pub(crate) mod touch;
pub(crate) mod ttl;
pub(crate) mod typ;
pub(crate) mod unlink;
pub(crate) mod wait;
pub(crate) mod waitaof;

pub use copy::*;
pub use del::*;
pub use dump::*;
pub use exists::*;
pub use expire::*;
pub use expireat::*;
pub use expiretime::*;
pub use keys::*;
pub use migrate::*;
pub use r#move::*;
pub use object_encoding::*;
pub use object_freq::*;
pub use object_idletime::*;
pub use object_refcount::*;
pub use persist::*;
pub use pexpire::*;
pub use pexpireat::*;
pub use pexpiretime::*;
pub use pttl::*;
pub use randomkey::*;
pub use rename::*;
pub use renamenx::*;
pub use restore::*;
pub use scan::*;
pub use sort::*;
pub use sort_ro::*;
pub use touch::*;
pub use ttl::*;
pub use typ::*;
pub use unlink::*;
pub use wait::*;
pub use waitaof::*;

/// Expiration options for EXPIRE command (requires Redis 7.0+)
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum ExpireOption {
    /// Only set expiry if key has no expiry
    NX,
    /// Only set expiry if key already has an expiry
    XX,
    /// Only set expiry if new expiry is greater than current
    GT,
    /// Only set expiry if new expiry is less than current
    LT,
}

impl std::fmt::Display for ExpireOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpireOption::NX => write!(f, "NX"),
            ExpireOption::XX => write!(f, "XX"),
            ExpireOption::GT => write!(f, "GT"),
            ExpireOption::LT => write!(f, "LT"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
#[serde(tag = "type")]
pub enum Auth {
    #[serde(rename = "AUTH")]
    Auth { password: String },
    #[serde(rename = "AUTH2")]
    Auth2 { username: String, password: String },
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, PartialEq, Eq)]
pub enum MigrateStatus {
    Ok,
    NoKey,
}

/// Result type for PTTL command representing the three possible states
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum PttlResult {
    /// Key does not exist (Redis returns -2)
    KeyNotFound,
    /// Key exists but has no associated TTL (Redis returns -1)
    NoExpire,
    /// Remaining TTL in milliseconds
    Milliseconds(i64),
}

/// Result of a RESTORE operation using Redis error naming conventions
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema, PartialEq)]
pub enum RestoreResult {
    /// Successful restoration - Redis returns "OK"
    Ok,
    /// BUSYKEY: Target key name already exists (without REPLACE flag)
    BusyKey(String),
    /// Invalid TTL value
    InvalidTtl(String),
    /// Bad data format / invalid RDB dump payload
    BadDataFormat(String),
    /// Other Redis error
    Err(String),
}

/// TTL result variants:
/// - `KeyDoesNotExist`: key does not exist (Redis returns -2)
/// - `NoExpiration`: key exists but has no associated expire (Redis returns -1)
/// - `Seconds(i64)`: TTL in seconds (>= 0)
#[derive(Debug, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum Ttl {
    KeyDoesNotExist,
    NoExpiration,
    Seconds(i64),
}
