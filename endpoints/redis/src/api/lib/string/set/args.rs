#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::value::RedisJsonValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Rule for SET command conditional execution
/// - NX: Only set the key if it does not already exist
/// - XX: Only set the key if it already exists
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum Rule {
    NX,
    XX,
}

/// Expiration options for SET command
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Options {
    /// Set expiration in seconds
    EX(EX),
    /// Set expiration in milliseconds
    PX(PX),
    /// Set expiration at Unix timestamp (seconds)
    EXAT(EXAT),
    /// Set expiration at Unix timestamp (milliseconds)
    PXAT(PXAT),
    /// Retain the existing TTL
    KEEPTTL,
}

/// Expiration time in seconds
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct EX {
    pub(crate) seconds: RedisJsonValue,
}

/// Expiration time in milliseconds
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct PX {
    pub(crate) milliseconds: RedisJsonValue,
}

/// Expiration at Unix timestamp in seconds
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct EXAT {
    pub(crate) unix_time_seconds: RedisJsonValue,
}

/// Expiration at Unix timestamp in milliseconds
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct PXAT {
    pub(crate) unix_time_milliseconds: RedisJsonValue,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_serialization() {
        let nx = Rule::NX;
        let xx = Rule::XX;

        let nx_json = serde_json::to_string(&nx).unwrap();
        let xx_json = serde_json::to_string(&xx).unwrap();

        assert_eq!(nx_json, "\"NX\"");
        assert_eq!(xx_json, "\"XX\"");
    }

    #[test]
    fn test_rule_deserialization() {
        let nx: Rule = serde_json::from_str("\"NX\"").unwrap();
        let xx: Rule = serde_json::from_str("\"XX\"").unwrap();

        assert_eq!(nx, Rule::NX);
        assert_eq!(xx, Rule::XX);
    }

    #[test]
    fn test_options_ex_serialization() {
        let opt = Options::EX(EX { seconds: RedisJsonValue::Integer(60) });
        let json = serde_json::to_string(&opt).unwrap();
        assert!(json.contains("EX"));
        assert!(json.contains("60"));
    }

    #[test]
    fn test_options_keepttl_serialization() {
        let opt = Options::KEEPTTL;
        let json = serde_json::to_string(&opt).unwrap();
        assert!(json.contains("KEEPTTL"));
    }
}
