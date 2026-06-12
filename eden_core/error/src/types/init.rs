use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum InitError {
    ConfigurationMissing, // 0x01
    DatabaseInitFailed,   // 0x02
    ServiceStartupFailed, // 0x03
    DependencyMissing,    // 0x04
    PermissionDenied,     // 0x05
    Custom(String),       // 0xFF - For backward compatibility with string errors
}

impl InitError {
    pub fn error_code(&self) -> u8 {
        match self {
            InitError::ConfigurationMissing => 0x01,
            InitError::DatabaseInitFailed => 0x02,
            InitError::ServiceStartupFailed => 0x03,
            InitError::DependencyMissing => 0x04,
            InitError::PermissionDenied => 0x05,
            InitError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            InitError::ConfigurationMissing => "Required configuration is missing. Please check your settings",
            InitError::DatabaseInitFailed => "Database initialization failed. Please check database connection",
            InitError::ServiceStartupFailed => "Service failed to start properly. Please check logs",
            InitError::DependencyMissing => "Required dependency is missing or unavailable",
            InitError::PermissionDenied => "Permission denied during initialization. Please check file/directory permissions",
            InitError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
