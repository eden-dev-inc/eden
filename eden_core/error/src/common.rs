use serde::{Deserialize, Serialize};
use std::{error, fmt, result};

/// Result type alias for common cross-cutting operations.
pub type ResultCommon<T> = result::Result<T, CommonError>;

/// Cross-cutting errors not specific to endpoint operations.
#[derive(Clone, Debug, PartialEq)]
pub enum CommonError {
    /// Configuration loading or validation error.
    Config(String),
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommonError::Config(s) => write!(f, "configuration error: {s}"),
        }
    }
}

impl error::Error for CommonError {}

/// Eden entity types for error categorization.
///
/// Used by [`EpError::database_query_error`](crate::EpError::database_query_error)
/// to determine the appropriate entity-specific error variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityType {
    /// Api entity (user-defined API schema/configuration).
    Api,
    /// Eden node entity.
    EdenNode,
    /// Endpoint entity.
    Endpoint,
    /// Endpoint group entity (logical grouping of same-kind endpoints).
    EndpointGroup,
    /// Interlay entity.
    Interlay,
    /// Migration entity.
    Migration,
    /// Organization entity.
    Organization,
    /// Robot entity (machine account).
    Robot,
    /// Template entity.
    Template,
    /// User entity.
    User,
    /// Pipeline entity (real-time CDC data sync).
    Pipeline,
    /// Snapshot entity (standalone data movement).
    Snapshot,
    /// Workflow entity.
    Workflow,
}

/// RBAC error type categorization.
///
/// Used by [`EpError::rbac_operation_error`](crate::EpError::rbac_operation_error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RbacErrorType {
    /// Connection to RBAC cache failed.
    ConnectionFailure,
    /// Invalid permission bits specified.
    InvalidPermissions,
    /// Access control rule not found.
    RuleNotFound,
}
