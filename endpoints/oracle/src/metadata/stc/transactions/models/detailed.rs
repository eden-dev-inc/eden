use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTransactionDetailedMetrics {
    pub problem_transactions: Vec<OracleTransactionDetails>,
    pub lock_analysis: Option<Vec<OracleLockDetails>>,
    pub session_analysis: Option<Vec<OracleSessionDetails>>,
    pub undo_analysis: Option<Vec<OracleUndoDetails>>,
    pub deadlock_analysis: Option<Vec<OracleDeadlockDetails>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTransactionDetails {
    pub sid: u64,
    pub serial: u64,
    pub username: String,
    pub program: String,
    pub machine: String,
    pub start_time: String,
    pub duration_seconds: u64,
    pub status: String,
    pub sql_id: Option<String>,
    pub sql_text: Option<String>,
    pub undo_blocks: u64,
    pub undo_records: u64,
    pub transaction_type: String,
    pub lock_wait: String,
    pub blocking_session: Option<u64>,
    pub issue_severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLockDetails {
    pub holding_sid: u64,
    pub waiting_sid: u64,
    pub lock_type: String,
    pub mode_held: String,
    pub mode_requested: String,
    pub object_name: String,
    pub object_type: String,
    pub wait_time_seconds: u64,
    pub blocking_sql_id: Option<String>,
    pub waiting_sql_id: Option<String>,
    pub request_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionDetails {
    pub sid: u64,
    pub serial: u64,
    pub username: String,
    pub status: String,
    pub program: String,
    pub machine: String,
    pub logon_time: String,
    pub last_call_et: u64,
    pub sql_id: Option<String>,
    pub blocking_session: Option<u64>,
    pub wait_class: Option<String>,
    pub wait_event: Option<String>,
    pub wait_time_seconds: u64,
    pub session_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleUndoDetails {
    pub segment_name: String,
    pub segment_id: u64,
    pub status: String,
    pub tablespace_name: String,
    pub size_bytes: u64,
    pub blocks_used: u64,
    pub blocks_total: u64,
    pub usage_percent: f64,
    pub active_transactions: u64,
    pub optimal_size: u64,
    pub shrinks: u64,
    pub extends: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDeadlockDetails {
    pub detection_time: String,
    pub session1_sid: u64,
    pub session2_sid: u64,
    pub object_name: String,
    pub deadlock_type: String,
    pub resolution: String,
    pub sql_id1: Option<String>,
    pub sql_id2: Option<String>,
}
