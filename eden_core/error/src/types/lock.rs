use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum LockError {
    AlreadyLocked,     // 0x01
    TimeoutReached,    // 0x02
    DeadlockDetected,  // 0x03
    InvalidLockState,  // 0x04
    OwnershipMismatch, // 0x05
    Custom(String),    // 0xFF - For backward compatibility with string errors
}

impl LockError {
    pub fn error_code(&self) -> u8 {
        match self {
            LockError::AlreadyLocked => 0x01,
            LockError::TimeoutReached => 0x02,
            LockError::DeadlockDetected => 0x03,
            LockError::InvalidLockState => 0x04,
            LockError::OwnershipMismatch => 0x05,
            LockError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            LockError::AlreadyLocked => "Resource is already locked by another process",
            LockError::TimeoutReached => "Lock acquisition timeout reached",
            LockError::DeadlockDetected => "Deadlock detected in lock acquisition",
            LockError::InvalidLockState => "Lock is in an invalid state",
            LockError::OwnershipMismatch => "Lock ownership mismatch. Cannot perform operation",
            LockError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
