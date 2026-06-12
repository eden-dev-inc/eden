use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SessionStatistics {
    /// Total sessions
    pub total_sessions: u32,
    /// Active sessions
    pub active_sessions: u32,
    /// Inactive sessions
    pub inactive_sessions: u32,
    /// Blocked sessions
    pub blocked_sessions: u32,
    /// Waiting sessions
    pub waiting_sessions: u32,
    /// Sessions by status
    pub sessions_by_status: HashMap<String, u32>,
    /// Sessions by wait class
    pub sessions_by_wait_class: HashMap<String, u32>,
    /// Long running sessions
    pub long_running_sessions: Vec<LongRunningSession>,
    /// Blocking sessions
    pub blocking_sessions: Vec<BlockingSession>,
}

/// Long running session information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LongRunningSession {
    /// Session ID
    pub sid: u32,
    /// Serial number
    pub serial: u32,
    /// Username
    pub username: String,
    /// Program
    pub program: String,
    /// SQL ID
    pub sql_id: Option<String>,
    /// Status
    pub status: String,
    /// Logon time
    pub logon_time: DateTimeWrapper,
    /// Runtime (seconds)
    pub runtime_seconds: u64,
    /// CPU time
    pub cpu_time: u64,
    /// Wait class
    pub wait_class: Option<String>,
    /// Wait event
    pub wait_event: Option<String>,
}

/// Blocking session information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct BlockingSession {
    /// Blocking session ID
    pub blocking_sid: u32,
    /// Blocked session ID
    pub blocked_sid: u32,
    /// Blocking username
    pub blocking_username: String,
    /// Blocked username
    pub blocked_username: String,
    /// Lock type
    pub lock_type: String,
    /// Lock mode
    pub lock_mode: String,
    /// Object name
    pub object_name: Option<String>,
    /// Block time (seconds)
    pub block_time_seconds: u64,
    /// Blocking SQL ID
    pub blocking_sql_id: Option<String>,
    /// Blocked SQL ID
    pub blocked_sql_id: Option<String>,
}
