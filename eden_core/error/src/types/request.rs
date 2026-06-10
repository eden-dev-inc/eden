use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// HTTP request validation errors (0x04XX error codes).
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RequestError {
    InvalidFormat,             // 0x01
    MissingParameters,         // 0x02
    InvalidParameters,         // 0x03
    PayloadTooLarge,           // 0x04
    UnsupportedMethod,         // 0x05
    FailedToUnwrapResponse,    // 0x06 - "failed to unwrap response" (3x)
    FailedToEncodeRequest,     // 0x07 - "failed to encode request" (2x)
    ErrorOutputNotImplemented, // 0x08 - "error output not yet implementd" (2x)
    RequestTimeout,            // 0x09
    ReadOnly,                  // 0x0A - Write operation requested on a read-only endpoint
    Custom(String),            // 0xFF - For backward compatibility with string errors
}

impl RequestError {
    /// Returns the specific error code (0x01-0xFF) for this request error.
    pub fn error_code(&self) -> u8 {
        match self {
            RequestError::InvalidFormat => 0x01,
            RequestError::MissingParameters => 0x02,
            RequestError::InvalidParameters => 0x03,
            RequestError::PayloadTooLarge => 0x04,
            RequestError::UnsupportedMethod => 0x05,
            RequestError::FailedToUnwrapResponse => 0x06,
            RequestError::FailedToEncodeRequest => 0x07,
            RequestError::ErrorOutputNotImplemented => 0x08,
            RequestError::RequestTimeout => 0x09,
            RequestError::ReadOnly => 0x0A,
            RequestError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            RequestError::InvalidFormat => "Request format is invalid. Please check your request structure",
            RequestError::MissingParameters => "Required parameters are missing from the request",
            RequestError::InvalidParameters => "One or more parameters are invalid",
            RequestError::PayloadTooLarge => "Request payload is too large. Please reduce the size",
            RequestError::UnsupportedMethod => "HTTP method is not supported for this endpoint",
            RequestError::FailedToUnwrapResponse => "Failed to unwrap response",
            RequestError::FailedToEncodeRequest => "Failed to encode request",
            RequestError::ErrorOutputNotImplemented => "Error output not yet implemented",
            RequestError::RequestTimeout => "Request timed out",
            RequestError::ReadOnly => "Write operation requested on a read-only endpoint",
            RequestError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
