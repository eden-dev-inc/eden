use borsh::{BorshDeserialize, BorshSerialize};
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionInfo {
    pub sid: u32,
    pub serial_number: u32,
    pub username: String,
    pub schema_name: String,
    pub os_user: Option<String>,
    pub machine: Option<String>,
    pub program: Option<String>,
    pub current_sql: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleBlockingChain {
    pub blocked_session: OracleSessionInfo,
    pub blocking_session: OracleSessionInfo,
    pub wait_time_centiseconds: u64,
    pub seconds_in_wait: u64,
    pub wait_event: Option<String>,
    pub object_name: Option<String>,
    pub object_type: Option<String>,
    pub lock_type: Option<String>,
    pub lock_mode_held: u32,
    pub lock_mode_requested: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLockConflict {
    pub waiting_sid: u32,
    pub holding_sid: u32,
    pub lock_type: String,
    pub lock_id1: u64,
    pub lock_id2: u64,
    pub mode_held: u32,
    pub mode_requested: u32,
    pub blocking_mode: u32,
    pub object_owner: Option<String>,
    pub object_name: Option<String>,
    pub object_type: Option<String>,
    pub seconds_in_wait: u64,
    pub wait_event: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDeadlockInfo {
    pub deadlock_timestamp: DateTimeWrapper,
    pub involved_sessions: Vec<u32>,
    pub deadlock_resource: Option<String>,
    pub resolution: Option<String>,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleContentionHotspot {
    pub owner: String,
    pub object_name: String,
    pub object_type: String,
    pub total_lock_count: u64,
    pub waiting_lock_count: u64,
    pub avg_wait_seconds: f64,
    pub max_wait_seconds: f64,
    pub unique_sessions: u64,
    pub contention_score: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionLockInfo {
    pub session_info: OracleSessionInfo,
    pub seconds_in_wait: u64,
    pub wait_event: Option<String>,
    pub p1_text: Option<String>,
    pub p1: Option<u64>,
    pub p2_text: Option<String>,
    pub p2: Option<u64>,
    pub blocking_session: Option<u32>,
    pub row_wait_obj: Option<u32>,
    pub row_wait_file: Option<u32>,
    pub row_wait_block: Option<u32>,
    pub row_wait_row: Option<u32>,
}
