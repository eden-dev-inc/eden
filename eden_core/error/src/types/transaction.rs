use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum TransactionError {
    BeginFailed,                   // 0x01
    CommitFailed,                  // 0x02
    RollbackFailed,                // 0x03
    DeadlockDetected,              // 0x04
    TimeoutExceeded,               // 0x05
    ChannelFailure,                // 0x06 - "channel failure"
    Rollback,                      // 0x07 - "Rollback"
    FailedToDowncast,              // 0x08 - "failed to downcast transaction"
    NotImplemented,                // 0x09 - "Not Implemented"
    TransactionsNotImplemented,    // 0x0A - "transactions are not implemented" (1x)
    PrepareCannotRunInTransaction, // 0x0B - "prepare cannot run in a transaction" (1x)
    FailedToCollectApprovals,      // 0x0C - "failed to collect valid approvals" (1x)
    NothingReceived,               // 0x0D - "Nothing received" (1x)
    Custom(String),                // 0xFF - For backward compatibility with string errors
}

impl TransactionError {
    pub fn error_code(&self) -> u8 {
        match self {
            TransactionError::BeginFailed => 0x01,
            TransactionError::CommitFailed => 0x02,
            TransactionError::RollbackFailed => 0x03,
            TransactionError::DeadlockDetected => 0x04,
            TransactionError::TimeoutExceeded => 0x05,
            TransactionError::ChannelFailure => 0x06,
            TransactionError::Rollback => 0x07,
            TransactionError::FailedToDowncast => 0x08,
            TransactionError::NotImplemented => 0x09,
            TransactionError::TransactionsNotImplemented => 0x0A,
            TransactionError::PrepareCannotRunInTransaction => 0x0B,
            TransactionError::FailedToCollectApprovals => 0x0C,
            TransactionError::NothingReceived => 0x0D,
            TransactionError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            TransactionError::BeginFailed => "Failed to begin database transaction",
            TransactionError::CommitFailed => "Failed to commit database transaction",
            TransactionError::RollbackFailed => "Failed to rollback database transaction",
            TransactionError::DeadlockDetected => "Database deadlock detected. Transaction was aborted",
            TransactionError::TimeoutExceeded => "Transaction timeout exceeded",
            TransactionError::ChannelFailure => "Channel failure",
            TransactionError::Rollback => "Rollback",
            TransactionError::FailedToDowncast => "Failed to downcast transaction",
            TransactionError::NotImplemented => "Not implemented",
            TransactionError::TransactionsNotImplemented => "Transactions are not implemented",
            TransactionError::PrepareCannotRunInTransaction => "Prepare cannot run in a transaction",
            TransactionError::FailedToCollectApprovals => "Failed to collect valid approvals",
            TransactionError::NothingReceived => "Nothing received",
            TransactionError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
