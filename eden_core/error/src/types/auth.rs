use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Authentication errors (0x08XX error codes).
///
/// Covers JWT token validation, credential verification, API key validation,
/// and session management failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum AuthError {
    /// Invalid username or password (0x01).
    InvalidCredentials,

    /// Invalid or expired API key (0x02).
    InvalidApiKey,

    /// JWT token has expired (0x03).
    TokenExpired,

    /// JWT token is malformed or invalid (0x04).
    TokenMalformed,

    /// User lacks required permissions (0x05).
    InsufficientPermissions,

    /// Session has expired, re-authentication required (0x06).
    SessionExpired,

    /// Custom authentication error message (0xFF).
    Custom(String),
}

impl AuthError {
    /// Returns the specific error code (0x01-0xFF) for this authentication error.
    pub fn error_code(&self) -> u8 {
        match self {
            AuthError::InvalidCredentials => 0x01,
            AuthError::InvalidApiKey => 0x02,
            AuthError::TokenExpired => 0x03,
            AuthError::TokenMalformed => 0x04,
            AuthError::InsufficientPermissions => 0x05,
            AuthError::SessionExpired => 0x06,
            AuthError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            AuthError::InvalidCredentials => "Invalid username or password. Please verify your credentials",
            AuthError::InvalidApiKey => "Invalid API key. Please verify your API key is correct and has not expired",
            AuthError::TokenExpired => "Authentication token has expired. Please log in again",
            AuthError::TokenMalformed => "Authentication token is invalid",
            AuthError::InsufficientPermissions => "You do not have sufficient permissions for this operation",
            AuthError::SessionExpired => "Your session has expired. Please log in again",
            AuthError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
