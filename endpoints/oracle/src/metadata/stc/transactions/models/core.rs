use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleTransactionDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTransactionInfo {
    pub active_transactions: u64,
    pub long_running_transactions: u64,
    pub very_long_transactions: u64,
    pub commits_last_hour: u64,
    pub rollbacks_last_hour: u64,
    pub user_commits: u64,
    pub user_rollbacks: u64,
    pub active_sessions: u64,
    pub sessions_waiting_locks: u64,
    pub blocking_sessions: u64,
    pub deadlocks_detected: u64,
    pub avg_transaction_duration: f64,
    pub max_transaction_duration: f64,
    pub rollback_ratio: f64,
    pub distributed_transactions: u64,
    pub prepared_transactions: u64,
    pub undo_transactions: u64,
    pub undo_blocks_used: u64,
    pub max_undo_retention: u64,
    pub current_undo_retention: u64,
    pub undo_segments: u64,
    pub active_undo_segments: u64,
    pub lock_timeouts: u64,
    pub transaction_health_score: f64,
    pub detailed_metrics: Option<OracleTransactionDetailedMetrics>,
}
