use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum TimeoutError {
    RequestTimeout,    // 0x01
    ConnectionTimeout, // 0x02
    ReadTimeout,       // 0x03
    WriteTimeout,      // 0x04
    ProcessTimeout,    // 0x05
    Custom(String),    // 0xFF - For backward compatibility with string errors
}

impl TimeoutError {
    pub fn error_code(&self) -> u8 {
        match self {
            TimeoutError::RequestTimeout => 0x01,
            TimeoutError::ConnectionTimeout => 0x02,
            TimeoutError::ReadTimeout => 0x03,
            TimeoutError::WriteTimeout => 0x04,
            TimeoutError::ProcessTimeout => 0x05,
            TimeoutError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            TimeoutError::RequestTimeout => "Request timeout exceeded. Please try again",
            TimeoutError::ConnectionTimeout => "Connection timeout reached",
            TimeoutError::ReadTimeout => "Read operation timed out",
            TimeoutError::WriteTimeout => "Write operation timed out",
            TimeoutError::ProcessTimeout => "Process execution timed out",
            TimeoutError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
