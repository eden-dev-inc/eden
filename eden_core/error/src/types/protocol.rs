use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ProtocolError {
    NotImplemented,                 // 0x01
    RESP2(String),                  // 0x02
    RESP3(String),                  // 0x03
    NoResponses,                    // 0x04
    MissingResponses(usize, usize), // 0x05
}

impl ProtocolError {
    pub fn error_code(&self) -> u8 {
        match self {
            Self::NotImplemented => 0x01,
            Self::RESP2(_) => 0x02,
            Self::RESP3(_) => 0x03,
            Self::NoResponses => 0x04,
            Self::MissingResponses(_, _) => 0x05,
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProtocolError::NotImplemented => write!(f, "Protocol error: not implemented"),
            ProtocolError::RESP2(err) => write!(f, "Protocol error: failed to decode RESP2: {err}"),
            ProtocolError::RESP3(err) => write!(f, "Protocol error: failed to decode RESP3: {err}"),
            ProtocolError::NoResponses => write!(f, "No pending responses to read"),
            ProtocolError::MissingResponses(count, pending) => write!(f, "Requested {count} responses but only {} pending", pending,),
        }
    }
}
