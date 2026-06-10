use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Default, Copy)]
/// Nonce value for transaction replay protection.
pub struct Nonce(u64);

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
/// Response containing current nonce value.
pub struct NonceResponse {
    pub nonce: Nonce,
    pub capacity: usize,
    pub timeout_ms: u64,
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct NonceCommands {
    pub nonce: Nonce,
    pub commands: Vec<String>,
}

impl Deref for Nonce {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Nonce {
    /// nonce from value
    pub fn from(n: u64) -> Self {
        Nonce(n)
    }

    /// adds one to existing nonce
    pub fn mut_next(&mut self) {
        self.0 += 1;
    }

    /// returns nonce + 1
    pub fn new_next(&self) -> Self {
        Nonce(self.0 + 1)
    }

    /// returns vector of bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    /// returns a Nonce if it can be parsed from &[u8]
    pub fn from_slice(value: &[u8]) -> Option<Self> {
        std::str::from_utf8(value).ok().and_then(|s| s.parse::<u64>().ok()).map(Self::from)
    }
}

impl fmt::Display for Nonce {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
