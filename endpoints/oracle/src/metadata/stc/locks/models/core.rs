use borsh::{BorshDeserialize, BorshSerialize};
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};

use super::{OracleBlockingChain, OracleContentionHotspot, OracleDeadlockInfo, OracleLockConflict, OracleSessionLockInfo};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub enum ContentionSeverity {
    #[default]
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLockInfo {
    pub total_active_locks: u64,
    pub blocking_locks: u64,
    pub blocked_sessions: u64,
    pub waiting_sessions: u64,
    pub total_deadlocks: u64,
    pub avg_lock_wait_time: f64,
    pub max_lock_wait_time: f64,
    pub total_lock_wait_time: f64,
    pub row_level_locks: u64,
    pub table_level_locks: u64,
    pub ddl_locks: u64,
    pub system_locks: u64,
    pub library_cache_locks: u64,
    pub dictionary_cache_locks: u64,
    pub other_locks: u64,
    pub null_locks: u64,
    pub row_share_locks: u64,
    pub row_exclusive_locks: u64,
    pub share_locks: u64,
    pub share_row_exclusive_locks: u64,
    pub exclusive_locks: u64,
    pub blocking_chains: Vec<OracleBlockingChain>,
    pub lock_conflicts: Vec<OracleLockConflict>,
    pub recent_deadlocks: Vec<OracleDeadlockInfo>,
    pub contended_objects: Vec<OracleContentionHotspot>,
    pub high_wait_sessions: Vec<OracleSessionLockInfo>,
    pub lock_efficiency_ratio: f64,
    pub blocked_session_percentage: f64,
    pub contention_severity: ContentionSeverity,
    pub performance_impact_score: f64,
    pub lock_waits_last_hour: u64,
    pub deadlocks_last_hour: u64,
    pub avg_blocking_time_last_hour: f64,
    pub collection_timestamp: DateTimeWrapper,
}
