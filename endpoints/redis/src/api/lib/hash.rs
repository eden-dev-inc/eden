use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

pub mod hdel;
pub mod hexists;
pub mod hexpire;
pub mod hexpireat;
pub mod hexpiretime;
pub mod hget;
pub mod hgetall;
pub mod hgetdel;
pub mod hgetex;
pub mod hincrby;
pub mod hincrbyfloat;
pub mod hkeys;
pub mod hlen;
pub mod hmget;
pub mod hmset;
pub mod hpersist;
pub mod hpexpire;
pub mod hpexpireat;
pub mod hpexpiretime;
pub mod hpttl;
pub mod hrandfield;
pub mod hscan;
pub mod hset;
pub mod hsetex;
pub mod hsetnx;
pub mod hstrlen;
pub mod httl;
pub mod hvals;

pub use hdel::*;
pub use hexists::*;
pub use hexpire::*;
pub use hexpireat::*;
pub use hexpiretime::*;
pub use hget::*;
pub use hgetall::*;
pub use hgetdel::*;
pub use hgetex::*;
pub use hincrby::*;
pub use hincrbyfloat::*;
pub use hkeys::*;
pub use hlen::*;
pub use hmget::*;
pub use hmset::*;
pub use hpersist::*;
pub use hpexpire::*;
pub use hpexpireat::*;
pub use hpexpiretime::*;
pub use hpttl::*;
pub use hrandfield::*;
pub use hscan::*;
pub use hset::*;
pub use hsetex::*;
pub use hsetnx::*;
pub use hstrlen::*;
pub use httl::*;
pub use hvals::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub enum Options {
    NX,
    XX,
    GT,
    LT,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub enum ExpireOptions {
    EX(RedisJsonValue),
    PX(RedisJsonValue),
    EXAT(RedisJsonValue),
    PXAT(RedisJsonValue),
    PERSIST,
}

/// Expire result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldExpireResult {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Condition not met (e.g., NX but field already has expiry) (0)
    ConditionNotMet,
    /// Expiration was successfully set (1)
    ExpirationSet,
    /// Expiration was successfully deleted (2)
    ExpirationDeleted,
}

/// Expire time result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldExpiretime {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Field exists but has no TTL (-1)
    NoExpire,
    /// Expiration time as Unix timestamp in seconds
    Timestamp(i64),
}

/// Value result for a single hash field from HGETDEL
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldValue {
    /// Field does not exist (nil)
    NotFound,
    /// Value that was retrieved and deleted
    Value(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema, PartialEq)]
pub struct Field {
    pub(crate) field: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Field {
    pub fn new(field: RedisJsonValue, value: RedisJsonValue) -> Self {
        Self { field, value }
    }

    pub fn field(&self) -> &RedisJsonValue {
        &self.field
    }

    pub fn value(&self) -> &RedisJsonValue {
        &self.value
    }
}

/// Persist result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldPersistResult {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Field exists but had no expiration (-1)
    NoExpire,
    /// Expiration was successfully removed (1)
    Persisted,
}

/// Expire result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldExpireAtResult {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Condition not met (e.g., NX but field already has expiry) (0)
    ConditionNotMet,
    /// Expiration was successfully set (1)
    ExpirationSet,
    /// Expiration was successfully deleted (2)
    ExpirationDeleted,
}

/// Expire time result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldExpireTime {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Field exists but has no expiration (-1)
    NoExpire,
    /// Unix timestamp in milliseconds when the field will expire
    UnixTimeMillis(i64),
}

#[derive(Debug, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Count {
    pub(crate) count: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) with_values: Option<bool>,
}

impl Count {
    pub fn new(count: impl Into<RedisJsonValue>) -> Self {
        Self { count: count.into(), with_values: None }
    }
    pub fn with_values(mut self) -> Self {
        self.with_values = Some(true);
        self
    }
}

/// Field existence condition for HSETEX
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum FieldCondition {
    /// Only set fields that don't exist
    FNX,
    /// Only set fields that already exist
    FXX,
}

/// Expiration options for HSETEX
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Expiration {
    /// Seconds TTL
    EX(RedisJsonValue),
    /// Milliseconds TTL
    PX(RedisJsonValue),
    /// Unix timestamp in seconds
    EXAT(RedisJsonValue),
    /// Unix timestamp in milliseconds
    PXAT(RedisJsonValue),
    /// Retain existing TTL
    KEEPTTL,
}
