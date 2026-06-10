use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DataError {
    CorruptedData,    // 0x01
    InvalidFormat,    // 0x02
    MissingRequired,  // 0x03
    ValidationFailed, // 0x04
    ConversionFailed, // 0x05
    Custom(String),   // 0xFF - For backward compatibility with string errors
}

impl DataError {
    pub fn error_code(&self) -> u8 {
        match self {
            DataError::CorruptedData => 0x01,
            DataError::InvalidFormat => 0x02,
            DataError::MissingRequired => 0x03,
            DataError::ValidationFailed => 0x04,
            DataError::ConversionFailed => 0x05,
            DataError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for DataError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            DataError::CorruptedData => "Data is corrupted or unreadable",
            DataError::InvalidFormat => "Data format is invalid or unsupported",
            DataError::MissingRequired => "Required data fields are missing",
            DataError::ValidationFailed => "Data validation failed. Please check your input",
            DataError::ConversionFailed => "Failed to convert data to required format",
            DataError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
