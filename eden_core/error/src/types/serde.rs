use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum SerdeError {
    SerializationFailed,   // 0x01
    DeserializationFailed, // 0x02
    InvalidFormat,         // 0x03
    MissingField,          // 0x04
    TypeMismatch,          // 0x05
    InvalidRequest,        // 0x06 - "invalid request" (11x)
    FailedToParseRow,      // 0x07 - "failed to parse row" (2x)
    ExpectedJsonObject,    // 0x08 - "Expected a JSON object" (2x)
    BorshNotImplemented,   // 0x09 - "borsh serialize not implemented for Client" (2x)
    ModelNotString,        // 0x0A - "passed model is not a string" (2x)
    MaxTokensNotNumber,    // 0x0B - "passed max tokens is not a number" (2x)
    UrlNotString,          // 0x0C - "passed URL is not a string" (2x)
    ApiKeyNotString,       // 0x0D - "passed API key is not a string" (2x)
    Custom(String),        // 0xFF - For backward compatibility with string errors
}

impl SerdeError {
    pub fn error_code(&self) -> u8 {
        match self {
            SerdeError::SerializationFailed => 0x01,
            SerdeError::DeserializationFailed => 0x02,
            SerdeError::InvalidFormat => 0x03,
            SerdeError::MissingField => 0x04,
            SerdeError::TypeMismatch => 0x05,
            SerdeError::InvalidRequest => 0x06,
            SerdeError::FailedToParseRow => 0x07,
            SerdeError::ExpectedJsonObject => 0x08,
            SerdeError::BorshNotImplemented => 0x09,
            SerdeError::ModelNotString => 0x0A,
            SerdeError::MaxTokensNotNumber => 0x0B,
            SerdeError::UrlNotString => 0x0C,
            SerdeError::ApiKeyNotString => 0x0D,
            SerdeError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for SerdeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            SerdeError::SerializationFailed => "Failed to serialize data to required format",
            SerdeError::DeserializationFailed => "Failed to deserialize data from source format",
            SerdeError::InvalidFormat => "Data format is invalid or unsupported",
            SerdeError::MissingField => "Required field is missing from data structure",
            SerdeError::TypeMismatch => "Data type mismatch encountered during processing",
            SerdeError::InvalidRequest => "Invalid request",
            SerdeError::FailedToParseRow => "Failed to parse row",
            SerdeError::ExpectedJsonObject => "Expected a JSON object",
            SerdeError::BorshNotImplemented => "Borsh serialize not implemented for Client",
            SerdeError::ModelNotString => "Passed model is not a string",
            SerdeError::MaxTokensNotNumber => "Passed max tokens is not a number",
            SerdeError::UrlNotString => "Passed URL is not a string",
            SerdeError::ApiKeyNotString => "Passed API key is not a string",
            SerdeError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
