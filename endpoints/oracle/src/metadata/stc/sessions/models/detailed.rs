use borsh::{BorshDeserialize, BorshSerialize};
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionDetailedMetrics {
    pub resource_intensive_sessions: Vec<OracleResourceSession>,
    pub long_running_sessions: Option<Vec<OracleLongSession>>,
    pub blocked_sessions: Option<Vec<OracleBlockedSessionDetails>>,
    pub failed_login_attempts: Option<Vec<OracleFailedLogin>>,
    pub user_session_stats: Option<Vec<OracleUserSessionStats>>,
    pub program_session_stats: Option<Vec<OracleProgramSessionStats>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleResourceSession {
    pub sid: i32,
    pub serial_number: i32,
    pub username: String,
    pub program: Option<String>,
    pub machine: Option<String>,
    pub os_user: Option<String>,
    pub status: String,
    pub session_duration: f64,
    pub pga_used_mem: Option<u64>,
    pub pga_alloc_mem: Option<u64>,
    pub pga_freeable_mem: Option<u64>,
    pub temp_space_used: Option<u64>,
    pub sql_id: Option<String>,
    pub event: Option<String>,
    pub wait_class: Option<String>,
    pub seconds_in_wait: Option<i32>,
    pub blocking_session: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLongSession {
    pub sid: i32,
    pub serial_number: i32,
    pub username: String,
    pub program: Option<String>,
    pub machine: Option<String>,
    pub os_user: Option<String>,
    pub status: String,
    pub logon_time: DateTimeWrapper,
    pub session_duration: f64,
    pub last_call_et: i32,
    pub sql_id: Option<String>,
    pub sql_text: Option<String>,
    pub event: Option<String>,
    pub wait_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleBlockedSessionDetails {
    pub blocked_sid: i32,
    pub blocked_serial: i32,
    pub blocked_username: String,
    pub blocked_program: Option<String>,
    pub blocking_sid: i32,
    pub blocking_serial: i32,
    pub blocking_username: String,
    pub blocking_program: Option<String>,
    pub wait_event: Option<String>,
    pub seconds_in_wait: Option<i32>,
    pub blocked_sql_id: Option<String>,
    pub blocking_sql_id: Option<String>,
    pub blocked_sql_text: Option<String>,
    pub blocking_sql_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleFailedLogin {
    pub username: String,
    pub terminal: Option<String>,
    pub timestamp: DateTimeWrapper,
    pub return_code: i32,
    pub client_id: Option<String>,
    pub attempt_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleUserSessionStats {
    pub username: String,
    pub session_count: u64,
    pub active_count: u64,
    pub inactive_count: u64,
    pub avg_duration: f64,
    pub max_duration: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleProgramSessionStats {
    pub program: String,
    pub session_count: u64,
    pub active_count: u64,
    pub unique_users: u64,
    pub unique_machines: u64,
}
