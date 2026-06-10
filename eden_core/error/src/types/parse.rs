use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ParseError {
    InvalidSyntax,            // 0x01
    UnexpectedToken,          // 0x02
    InvalidEncoding,          // 0x03
    IncompleteData,           // 0x04
    FormatNotSupported,       // 0x05
    FailedToParseMetadata,    // 0x06 - "failed to parse metadata" (4x)
    FailedToDowncastMetadata, // 0x07 - "failed to downcast metadata" (4x)
    FailedToDowncastInput,    // 0x08 - "failed to downcast input" (4x)
    InvalidDatabaseUsername,  // 0x09 - "invalid database username" (2x)
    FailedToAddRbacKey,       // 0x0A - "failed to add RbacKey to Rbac Entity" (2x)
    Custom(String),           // For custom error messages
}

impl ParseError {
    pub fn error_code(&self) -> u8 {
        match self {
            ParseError::InvalidSyntax => 0x01,
            ParseError::UnexpectedToken => 0x02,
            ParseError::InvalidEncoding => 0x03,
            ParseError::IncompleteData => 0x04,
            ParseError::FormatNotSupported => 0x05,
            ParseError::FailedToParseMetadata => 0x06,
            ParseError::FailedToDowncastMetadata => 0x07,
            ParseError::FailedToDowncastInput => 0x08,
            ParseError::InvalidDatabaseUsername => 0x09,
            ParseError::FailedToAddRbacKey => 0x0A,
            ParseError::Custom(_) => 0xFF, // Generic code for custom errors
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            ParseError::InvalidSyntax => "Invalid syntax encountered during parsing",
            ParseError::UnexpectedToken => "Unexpected token found in input",
            ParseError::InvalidEncoding => "Invalid character encoding detected",
            ParseError::IncompleteData => "Input data is incomplete or truncated",
            ParseError::FormatNotSupported => "Data format is not supported",
            ParseError::FailedToParseMetadata => "Failed to parse metadata",
            ParseError::FailedToDowncastMetadata => "Failed to downcast metadata",
            ParseError::FailedToDowncastInput => "Failed to downcast input",
            ParseError::InvalidDatabaseUsername => "Invalid database username",
            ParseError::FailedToAddRbacKey => "Failed to add RbacKey to Rbac Entity",
            ParseError::Custom(msg) => msg,
        };
        write!(f, "{}", message)
    }
}
