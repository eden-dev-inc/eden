use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type TransactionHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTransactionHealthSummary {
    pub transaction_health: TransactionHealthStatus,
    pub locking_health: TransactionHealthStatus,
    pub rollback_health: TransactionHealthStatus,
    pub undo_health: TransactionHealthStatus,
}
