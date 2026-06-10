use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ConnectError {
    ConnectionRefused,       // 0x01
    NetworkUnreachable,      // 0x02
    TimeoutReached,          // 0x03
    SslHandshakeFailed,      // 0x04
    ProtocolMismatch,        // 0x05
    ConnectionNotFound,      // 0x06 - "could not find connection"
    FailedToDowncastConfig,  // 0x07 - "failed to downcast config"
    FailedToDowncastRouter,  // 0x08 - "failed to downcast router"
    FailedToDowncastRequest, // 0x09 - "failed to downcast request"
    CouldNotGetConnection,   // 0x0A - "could not get connection"
    CouldNotGetEndpoint,     // 0x0B - "could not get endpoint"
    SyncConnectionNotExist,  // 0x0C - "sync connection does not exist"
    IncorrectPoolFormat,     // 0x0D - "incorrect pool format: sync"
    InvalidHeaderName,       // 0x0E - "Invalid header name"
    InvalidHeaderValue,      // 0x0F - "Invalid header value"
    Custom(String),          // 0xFF - For backward compatibility with string errors
}

impl ConnectError {
    pub fn error_code(&self) -> u8 {
        match self {
            ConnectError::ConnectionRefused => 0x01,
            ConnectError::NetworkUnreachable => 0x02,
            ConnectError::TimeoutReached => 0x03,
            ConnectError::SslHandshakeFailed => 0x04,
            ConnectError::ProtocolMismatch => 0x05,
            ConnectError::ConnectionNotFound => 0x06,
            ConnectError::FailedToDowncastConfig => 0x07,
            ConnectError::FailedToDowncastRouter => 0x08,
            ConnectError::FailedToDowncastRequest => 0x09,
            ConnectError::CouldNotGetConnection => 0x0A,
            ConnectError::CouldNotGetEndpoint => 0x0B,
            ConnectError::SyncConnectionNotExist => 0x0C,
            ConnectError::IncorrectPoolFormat => 0x0D,
            ConnectError::InvalidHeaderName => 0x0E,
            ConnectError::InvalidHeaderValue => 0x0F,
            ConnectError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            ConnectError::ConnectionRefused => "Connection was refused by the target host",
            ConnectError::NetworkUnreachable => "Network is unreachable. Please check your connectivity",
            ConnectError::TimeoutReached => "Connection timeout reached",
            ConnectError::SslHandshakeFailed => "SSL/TLS handshake failed. Please check certificates",
            ConnectError::ProtocolMismatch => "Protocol version mismatch detected",
            ConnectError::ConnectionNotFound => "Could not find connection",
            ConnectError::FailedToDowncastConfig => "Failed to downcast config",
            ConnectError::FailedToDowncastRouter => "Failed to downcast router",
            ConnectError::FailedToDowncastRequest => "Failed to downcast request",
            ConnectError::CouldNotGetConnection => "Could not get connection",
            ConnectError::CouldNotGetEndpoint => "Could not get endpoint",
            ConnectError::SyncConnectionNotExist => "Sync connection does not exist",
            ConnectError::IncorrectPoolFormat => "Incorrect pool format: sync",
            ConnectError::InvalidHeaderName => "Invalid header name",
            ConnectError::InvalidHeaderValue => "Invalid header value",
            ConnectError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
