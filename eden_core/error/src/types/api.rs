use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// API-level errors (0x01XX error codes).
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ApiError {
    InvalidRequest,     // 0x01
    RateLimitExceeded,  // 0x02
    ServiceUnavailable, // 0x03
    InvalidInput,       // 0x04
    InternalError,      // 0x05
    Custom(String),     // 0xFF - For backward compatibility with string errors
}

impl ApiError {
    /// Returns the specific error code (0x01-0xFF) for this api error.
    pub fn error_code(&self) -> u8 {
        match self {
            ApiError::InvalidRequest => 0x01,
            ApiError::RateLimitExceeded => 0x02,
            ApiError::ServiceUnavailable => 0x03,
            ApiError::InvalidInput => 0x04,
            ApiError::InternalError => 0x05,
            ApiError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            ApiError::InvalidRequest => "Invalid request format or parameters",
            ApiError::RateLimitExceeded => "Rate limit exceeded. Please slow down your requests",
            ApiError::ServiceUnavailable => "Service temporarily unavailable. Please try again later",
            ApiError::InvalidInput => "Invalid input provided. Please check your data",
            ApiError::InternalError => "Internal API error occurred",
            ApiError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
