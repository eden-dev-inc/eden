use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum InterlayError {
    InterlayError(String), // 0x01
}

impl InterlayError {
    pub fn error_code(&self) -> u8 {
        match self {
            Self::InterlayError(_) => 0x01,
        }
    }
}

impl fmt::Display for InterlayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InterlayError(err) => write!(f, "Interlay error {err}"),
        }
    }
}
