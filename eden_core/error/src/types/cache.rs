use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Redis cache operation errors (0x07XX error codes).
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum CacheError {
    KeyNotFound,      // 0x01
    ConnectionLost,   // 0x02
    MemoryExhausted,  // 0x03
    ExpirationFailed, // 0x04
    InvalidKey,       // 0x05
    NoKeyProvided,    // 0x06 - "no key provided" (4x)
    Custom(String),   // 0xFF - For backward compatibility with string errors
}

impl CacheError {
    /// Returns the specific error code (0x01-0xFF) for this cache error.
    pub fn error_code(&self) -> u8 {
        match self {
            CacheError::KeyNotFound => 0x01,
            CacheError::ConnectionLost => 0x02,
            CacheError::MemoryExhausted => 0x03,
            CacheError::ExpirationFailed => 0x04,
            CacheError::InvalidKey => 0x05,
            CacheError::NoKeyProvided => 0x06,
            CacheError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            CacheError::KeyNotFound => "Requested key was not found in cache",
            CacheError::ConnectionLost => "Lost connection to cache server",
            CacheError::MemoryExhausted => "Cache memory limit exceeded",
            CacheError::ExpirationFailed => "Failed to set or update cache expiration",
            CacheError::InvalidKey => "Cache key format is invalid",
            CacheError::NoKeyProvided => "No key provided",
            CacheError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
