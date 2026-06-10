use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum IoError {
    Write(String),   // 0x01
    Read(String),    // 0x02
    Flush(String),   // 0x03
    Closed(String),  // 0x04
    Connect(String), // 0x05
}

impl IoError {
    pub fn error_code(&self) -> u8 {
        match self {
            Self::Write(_) => 0x01,
            Self::Read(_) => 0x02,
            Self::Flush(_) => 0x03,
            Self::Closed(_) => 0x04,
            Self::Connect(_) => 0x05,
        }
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Write(err) => write!(f, "IO write error: {err}"),
            Self::Read(err) => write!(f, "IO read error: {err}"),
            Self::Flush(err) => write!(f, "IO flush error: {err}"),
            Self::Closed(err) => write!(f, "IO closed: {err}"),
            Self::Connect(err) => write!(f, "IO connect: {err}"),
        }
    }
}
