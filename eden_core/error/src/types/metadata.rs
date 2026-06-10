use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MetadataError {
    InvalidFormat,                                                             // 0x01
    MissingRequired,                                                           // 0x02
    CorruptedData,                                                             // 0x03
    VersionMismatch,                                                           // 0x04
    AccessDenied,                                                              // 0x05
    FailedToDowncastRouter,                                                    // 0x06 - "failed to downcast router" (10x)
    QueryTimeout(String),                                                      // 0x07 - for all "Query timeout for ..." (100+ occurrences)
    TimeoutBucket(String),                                                     // 0x08
    RouterMissing { kind: String }, // 0x09 - metadata router missing for the requested endpoint kind
    RouterTypeMismatch { kind: String, expected: String }, // 0x0A - metadata router could not be downcast to the expected type
    EndpointMissing { kind: String, endpoint: String }, // 0x0B - endpoint not registered within the metadata router
    ConnectionUnavailable { kind: String, endpoint: String, details: String }, // 0x0C - failed to acquire a connection for the endpoint
    PackageNotFound { package: String }, // 0x0D - requested metadata package not available for the endpoint
    CapabilityUnavailable { capability: String }, // 0x0E - endpoint lacks a required capability
    Custom(String),                 // 0xFF - For backward compatibility with string errors
}

impl MetadataError {
    pub fn error_code(&self) -> u8 {
        match self {
            MetadataError::InvalidFormat => 0x01,
            MetadataError::MissingRequired => 0x02,
            MetadataError::CorruptedData => 0x03,
            MetadataError::VersionMismatch => 0x04,
            MetadataError::AccessDenied => 0x05,
            MetadataError::FailedToDowncastRouter => 0x06,
            MetadataError::QueryTimeout(_) => 0x07,
            MetadataError::TimeoutBucket(_) => 0x08,
            MetadataError::RouterMissing { .. } => 0x09,
            MetadataError::RouterTypeMismatch { .. } => 0x0A,
            MetadataError::EndpointMissing { .. } => 0x0B,
            MetadataError::ConnectionUnavailable { .. } => 0x0C,
            MetadataError::PackageNotFound { .. } => 0x0D,
            MetadataError::CapabilityUnavailable { .. } => 0x0E,
            MetadataError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message: String = match self {
            MetadataError::InvalidFormat => "Metadata format is invalid or unsupported".to_string(),
            MetadataError::MissingRequired => "Required metadata fields are missing".to_string(),
            MetadataError::CorruptedData => "Metadata is corrupted or unreadable".to_string(),
            MetadataError::VersionMismatch => "Metadata version mismatch detected".to_string(),
            MetadataError::AccessDenied => "Access denied to metadata resource".to_string(),
            MetadataError::FailedToDowncastRouter => "Failed to downcast router".to_string(),
            MetadataError::QueryTimeout(timeout_obj) => {
                format!("Query timeout for {}", timeout_obj)
            }
            MetadataError::TimeoutBucket(bucket) => format!("Timeout processing bucket {bucket}"),
            MetadataError::RouterMissing { kind } => {
                format!("Metadata router not registered for endpoint kind {kind}")
            }
            MetadataError::RouterTypeMismatch { kind, expected } => {
                format!("Metadata router type mismatch for endpoint kind {kind}; expected {expected}")
            }
            MetadataError::EndpointMissing { kind, endpoint } => {
                format!("Metadata endpoint {endpoint} missing from router for endpoint kind {kind}")
            }
            MetadataError::ConnectionUnavailable { kind, endpoint, details } => {
                format!("Metadata connection unavailable for endpoint {endpoint} (kind {kind}): {details}")
            }
            MetadataError::PackageNotFound { package } => {
                format!("Metadata package '{package}' not available for the requested endpoint")
            }
            MetadataError::CapabilityUnavailable { capability } => {
                format!("Endpoint lacks required capability: {capability}")
            }
            MetadataError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
