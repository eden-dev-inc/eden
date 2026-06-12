use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Role-Based Access Control (RBAC) errors (0x09XX error codes).
///
/// Covers permission validation, permission-bit parsing, and authorization failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RbacError {
    /// Access control rule not found (0x01).
    RuleNotFound,

    /// Invalid permission bits specified (0x02).
    InvalidPermissions,

    /// Connection to RBAC cache failed (0x03).
    ConnectionFailure,

    /// Referenced entity not found in RBAC system (0x04).
    EntityNotFound,

    /// Unauthorized access attempt (0x05).
    Unauthorized,

    /// Custom RBAC error message (0xFF).
    Custom(String),
}

impl RbacError {
    /// Returns the specific error code (0x01-0xFF) for this RBAC error.
    pub fn error_code(&self) -> u8 {
        match self {
            RbacError::RuleNotFound => 0x01,
            RbacError::InvalidPermissions => 0x02,
            RbacError::ConnectionFailure => 0x03,
            RbacError::EntityNotFound => 0x04,
            RbacError::Unauthorized => 0x05,
            RbacError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for RbacError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            RbacError::RuleNotFound => "Access control rule not found. Please verify the entity and subject exist",
            RbacError::InvalidPermissions => "Invalid permission bits specified",
            RbacError::ConnectionFailure => "Connection timeout to RBAC cache. Please check network connectivity",
            RbacError::EntityNotFound => "Referenced entity not found in access control system",
            RbacError::Unauthorized => "Unauthorized",
            RbacError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
