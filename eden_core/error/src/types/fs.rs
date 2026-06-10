use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum FsError {
    FileNotFound,           // 0x01
    PermissionDenied,       // 0x02
    DiskFull,               // 0x03
    InvalidPath,            // 0x04
    IoError,                // 0x05
    OrganizationIdEmpty,    // 0x06 - "Organization ID cannot be empty" (1x)
    InfoMustBeJsonObject,   // 0x07 - "Info must be a valid JSON object" (1x)
    NodeMustHaveEndpoint,   // 0x08 - "Eden Node must have at least one endpoint" (1x)
    NodeIdEmpty,            // 0x09 - "Eden Node ID cannot be empty" (1x)
    DuplicateEndpointUuids, // 0x0A - "Duplicate endpoint UUIDs are not allowed" (1x)
    Custom(String),         // 0xFF - For backward compatibility with string errors
}

impl FsError {
    pub fn error_code(&self) -> u8 {
        match self {
            FsError::FileNotFound => 0x01,
            FsError::PermissionDenied => 0x02,
            FsError::DiskFull => 0x03,
            FsError::InvalidPath => 0x04,
            FsError::IoError => 0x05,
            FsError::OrganizationIdEmpty => 0x06,
            FsError::InfoMustBeJsonObject => 0x07,
            FsError::NodeMustHaveEndpoint => 0x08,
            FsError::NodeIdEmpty => 0x09,
            FsError::DuplicateEndpointUuids => 0x0A,
            FsError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            FsError::FileNotFound => "File or directory not found",
            FsError::PermissionDenied => "Permission denied. Please check file/directory permissions",
            FsError::DiskFull => "Disk is full. Please free up space and try again",
            FsError::InvalidPath => "File path is invalid or contains illegal characters",
            FsError::IoError => "Input/output error occurred during file operation",
            FsError::OrganizationIdEmpty => "Organization ID cannot be empty",
            FsError::InfoMustBeJsonObject => "Info must be a valid JSON object",
            FsError::NodeMustHaveEndpoint => "Eden Node must have at least one endpoint",
            FsError::NodeIdEmpty => "Eden Node ID cannot be empty",
            FsError::DuplicateEndpointUuids => "Duplicate endpoint UUIDs are not allowed",
            FsError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
