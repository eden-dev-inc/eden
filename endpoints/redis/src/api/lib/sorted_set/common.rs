//! Shared types for sorted set commands.
//!
//! This module provides common types used across multiple sorted set commands
//! like ZUNION, ZUNIONSTORE, ZINTER, ZINTERSTORE, etc.

#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::value::RedisJsonValue;
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Aggregate function for combining scores in sorted set operations.
///
/// Used by ZUNION, ZUNIONSTORE, ZINTER, ZINTERSTORE, etc.
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema,
)]
pub enum Aggregate {
    /// Sum the scores (default)
    #[default]
    SUM,
    /// Take the minimum score
    MIN,
    /// Take the maximum score
    MAX,
}

impl Aggregate {
    /// Convert to Redis command argument
    pub fn as_str(&self) -> &'static str {
        match self {
            Aggregate::SUM => "SUM",
            Aggregate::MIN => "MIN",
            Aggregate::MAX => "MAX",
        }
    }

    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SUM" => Some(Aggregate::SUM),
            "MIN" => Some(Aggregate::MIN),
            "MAX" => Some(Aggregate::MAX),
            _ => None,
        }
    }
}

/// Limit clause for sorted set range commands.
///
/// Used by ZRANGEBYLEX, ZREVRANGEBYLEX, ZRANGEBYSCORE, ZREVRANGEBYSCORE, etc.
#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, Builder, ToSchema, JsonSchema,
)]
pub struct Limit {
    pub offset: RedisJsonValue,
    pub count: RedisJsonValue,
}

impl Limit {
    pub fn new(offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        Self { offset: offset.into(), count: count.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_default() {
        assert_eq!(Aggregate::default(), Aggregate::SUM);
    }

    #[test]
    fn test_aggregate_as_str() {
        assert_eq!(Aggregate::SUM.as_str(), "SUM");
        assert_eq!(Aggregate::MIN.as_str(), "MIN");
        assert_eq!(Aggregate::MAX.as_str(), "MAX");
    }

    #[test]
    fn test_aggregate_from_str() {
        assert_eq!(Aggregate::from_str("sum"), Some(Aggregate::SUM));
        assert_eq!(Aggregate::from_str("MIN"), Some(Aggregate::MIN));
        assert_eq!(Aggregate::from_str("Max"), Some(Aggregate::MAX));
        assert_eq!(Aggregate::from_str("invalid"), None);
    }

    #[test]
    fn test_limit_new() {
        let limit = Limit::new(0, 10);
        assert_eq!(limit.offset, RedisJsonValue::Integer(0));
        assert_eq!(limit.count, RedisJsonValue::Integer(10));
    }
}
