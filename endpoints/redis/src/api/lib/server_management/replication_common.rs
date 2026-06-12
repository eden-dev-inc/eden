//! Common types shared between REPLICAOF and SLAVEOF commands.
//!
//! These commands are functionally identical (SLAVEOF is deprecated in favor of REPLICAOF).

use crate::api::value::RedisJsonValue;
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Target specification for replication commands.
///
/// Either specifies a master server to replicate from, or NO ONE to stop replication.
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum ReplicationTarget {
    /// Replicate from a specific host:port
    HostPort(ReplicationAddr),
    /// Stop replication and become a master
    #[default]
    NoOne,
}

impl ReplicationTarget {
    pub fn host_port(host: impl Into<RedisJsonValue>, port: impl Into<RedisJsonValue>) -> Self {
        Self::HostPort(ReplicationAddr { host: host.into(), port: port.into() })
    }

    pub fn no_one() -> Self {
        Self::NoOne
    }

    pub fn is_no_one(&self) -> bool {
        matches!(self, Self::NoOne)
    }

    pub fn addr(&self) -> Option<&ReplicationAddr> {
        match self {
            Self::HostPort(addr) => Some(addr),
            Self::NoOne => None,
        }
    }
}

/// Address of a Redis master server.
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct ReplicationAddr {
    pub host: RedisJsonValue,
    pub port: RedisJsonValue,
}

impl ReplicationAddr {
    pub fn new(host: impl Into<RedisJsonValue>, port: impl Into<RedisJsonValue>) -> Self {
        Self { host: host.into(), port: port.into() }
    }

    pub fn host(&self) -> &RedisJsonValue {
        &self.host
    }

    pub fn port(&self) -> &RedisJsonValue {
        &self.port
    }
}

/// Parse replication command arguments into a ReplicationTarget.
///
/// Expects exactly 2 arguments: either (host, port) or ("NO", "ONE").
pub fn parse_replication_args(args: Vec<RedisJsonValue>, command_name: &str) -> Result<ReplicationTarget, error::EpError> {
    if args.len() != 2 {
        return Err(error::EpError::parse(format!("{} requires 2 arguments, given {}", command_name, args.len())));
    }

    // Check for "NO ONE"
    if let (RedisJsonValue::String(s1), RedisJsonValue::String(s2)) = (&args[0], &args[1])
        && s1.to_uppercase() == "NO"
        && s2.to_uppercase() == "ONE"
    {
        return Ok(ReplicationTarget::NoOne);
    }

    // Otherwise treat as host and port
    Ok(ReplicationTarget::HostPort(ReplicationAddr { host: args[0].clone(), port: args[1].clone() }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_target_host_port() {
        let target = ReplicationTarget::host_port(RedisJsonValue::String("localhost".into()), RedisJsonValue::Integer(6379));
        assert!(!target.is_no_one());
        assert!(target.addr().is_some());
    }

    #[test]
    fn test_replication_target_no_one() {
        let target = ReplicationTarget::no_one();
        assert!(target.is_no_one());
        assert!(target.addr().is_none());
    }

    #[test]
    fn test_parse_no_one() {
        let args = vec![RedisJsonValue::String("NO".into()), RedisJsonValue::String("ONE".into())];
        let target = parse_replication_args(args, "TEST").unwrap();
        assert!(target.is_no_one());
    }

    #[test]
    fn test_parse_no_one_lowercase() {
        let args = vec![RedisJsonValue::String("no".into()), RedisJsonValue::String("one".into())];
        let target = parse_replication_args(args, "TEST").unwrap();
        assert!(target.is_no_one());
    }

    #[test]
    fn test_parse_host_port() {
        let args = vec![RedisJsonValue::String("192.168.1.1".into()), RedisJsonValue::Integer(6380)];
        let target = parse_replication_args(args, "TEST").unwrap();
        assert!(!target.is_no_one());
        let addr = target.addr().unwrap();
        assert_eq!(addr.host(), &RedisJsonValue::String("192.168.1.1".into()));
        assert_eq!(addr.port(), &RedisJsonValue::Integer(6380));
    }

    #[test]
    fn test_parse_wrong_arg_count() {
        let args = vec![RedisJsonValue::String("localhost".into())];
        let err = parse_replication_args(args, "TEST").unwrap_err();
        assert!(err.to_string().contains("requires 2 arguments"));
    }

    #[test]
    fn test_parse_three_args() {
        let args = vec![
            RedisJsonValue::String("a".into()),
            RedisJsonValue::String("b".into()),
            RedisJsonValue::String("c".into()),
        ];
        let err = parse_replication_args(args, "TEST").unwrap_err();
        assert!(err.to_string().contains("requires 2 arguments"));
    }
}
