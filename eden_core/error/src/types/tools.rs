use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ToolsError {
    ConnectionFailed,          // 0x01
    ProtocolError,             // 0x02
    InvalidMessage,            // 0x03
    TimeoutError,              // 0x04
    AuthenticationError,       // 0x05
    UnknownTool,               // 0x06 - "Unknown tool: {}" (5x)
    ErrorReadingResult,        // 0x07 - "Error reading result: {}" (4x)
    ErrorPreparingRequestBody, // 0x08 - "Error preparing request body: {}" (4x)
    ErrorInRequestToRelay,     // 0x09 - "Error in request to relay: {}" (4x)
    InvalidToolArguments,      // 0x0A - "Invalid tool arguments: {}" (3x)
    Custom(String),            // 0xFF - For backward compatibility with string errors
}

impl ToolsError {
    pub fn error_code(&self) -> u8 {
        match self {
            ToolsError::ConnectionFailed => 0x01,
            ToolsError::ProtocolError => 0x02,
            ToolsError::InvalidMessage => 0x03,
            ToolsError::TimeoutError => 0x04,
            ToolsError::AuthenticationError => 0x05,
            ToolsError::UnknownTool => 0x06,
            ToolsError::ErrorReadingResult => 0x07,
            ToolsError::ErrorPreparingRequestBody => 0x08,
            ToolsError::ErrorInRequestToRelay => 0x09,
            ToolsError::InvalidToolArguments => 0x0A,
            ToolsError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for ToolsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            ToolsError::ConnectionFailed => "Failed to establish tools connection",
            ToolsError::ProtocolError => "Tools protocol error encountered",
            ToolsError::InvalidMessage => "Invalid tools message format",
            ToolsError::TimeoutError => "Tools operation timed out",
            ToolsError::AuthenticationError => "Tools authentication failed",
            ToolsError::UnknownTool => "Unknown tool",
            ToolsError::ErrorReadingResult => "Error reading result",
            ToolsError::ErrorPreparingRequestBody => "Error preparing request body",
            ToolsError::ErrorInRequestToRelay => "Error in request to relay",
            ToolsError::InvalidToolArguments => "Invalid tool arguments",
            ToolsError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
