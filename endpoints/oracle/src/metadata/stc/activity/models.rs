use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
/// Oracle database activity: session counts, longest-running SQL/transaction
/// and parallel execution stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleActivityInfo {
    /// Total number of active user sessions
    pub active_sessions: u64,
    /// Number of inactive sessions
    pub inactive_sessions: u64,
    /// Number of sessions killed but not cleaned up
    pub killed_sessions: u64,
    /// Total number of sessions (including background processes)
    pub total_sessions: u64,
    /// Maximum allowed sessions from v$parameter
    pub max_sessions: u64,
    /// Percentage of session limit being used (0.0 to 100.0)
    pub session_utilization_pct: f64,
    /// Duration of the longest running SQL in seconds
    pub longest_sql_duration: f64,
    /// Duration of the longest running transaction in seconds
    pub longest_transaction_duration: f64,
    /// Average SQL execution time across all active statements
    pub avg_active_sql_duration: f64,
    /// Number of sessions currently waiting for locks
    pub waiting_sessions_count: u64,
    /// Number of sessions that are actively blocking others
    pub blocking_sessions_count: u64,
    /// Current number of active parallel execution servers
    pub parallel_servers_active: u64,
    /// Maximum parallel execution servers configured
    pub parallel_servers_max: u64,
    /// Current PGA memory usage in bytes
    pub current_pga_used: u64,
    /// Maximum PGA memory limit in bytes
    pub pga_aggregate_limit: u64,
    /// Current SGA size in bytes
    pub sga_size: u64,
    /// Number of processes currently connected
    pub process_count: u64,
    /// Maximum processes allowed
    pub process_limit: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<OracleDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDetailedMetrics {
    /// Long-running SQL statements (only collected when longest_sql_duration > threshold)
    pub long_running_sql: Vec<OracleActiveSql>,
    /// Blocking relationships (only collected when blocking_sessions_count > 0)
    pub blocked_sessions: Vec<OracleBlockedSession>,
    /// Session breakdown by schema (collected less frequently)
    pub sessions_by_schema: Option<Vec<OracleSessionsBySchema>>,
    /// Top wait events (collected when performance issues detected)
    pub top_wait_events: Option<Vec<OracleWaitEvent>>,
}

/// A long-running SQL statement with session and resource details.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleActiveSql {
    /// Session identifier
    pub sid: i32,
    /// Session serial number
    pub serial_number: i32,
    /// Username executing the SQL
    pub username: String,
    /// Schema name where the SQL is executing
    pub schema_name: String,
    /// SQL statement text (truncated for safety)
    pub sql_text: String,
    /// Duration the SQL has been running (seconds)
    pub duration: f64,
    /// Current status of the session
    pub status: String,
    /// Program name from connection
    pub program: Option<String>,
    /// Machine name from connection
    pub machine: Option<String>,
    /// Operating system user
    pub os_user: Option<String>,
    /// Time when the SQL execution started
    pub sql_exec_start: DateTimeWrapper,
    /// Elapsed time since last call (seconds)
    pub last_call_et: i32,
    /// Session ID blocking this session (if any)
    pub blocking_session: Option<i32>,
    /// Current wait event (if any)
    pub event: Option<String>,
    /// Wait class of current event
    pub wait_class: Option<String>,
    /// SQL identifier
    pub sql_id: Option<String>,
    /// SQL child number
    pub sql_child_number: Option<i32>,
}

/// Information about blocking session relationships
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleBlockedSession {
    /// Session ID of the blocked session
    pub blocked_sid: i32,
    /// Serial number of the blocked session
    pub blocked_serial: i32,
    /// Username of the blocked session
    pub blocked_username: String,
    /// Session ID of the blocking session
    pub blocking_sid: i32,
    /// Serial number of the blocking session
    pub blocking_serial: i32,
    /// Username of the blocking session
    pub blocking_username: String,
    /// SQL statement that is being blocked (truncated)
    pub blocked_sql_text: String,
    /// SQL statement that is causing the block (truncated)
    pub blocking_sql_text: String,
    /// Type of lock causing the block
    pub lock_type: Option<String>,
    /// Lock mode currently held
    pub mode_held: Option<String>,
    /// Lock mode being requested
    pub mode_requested: Option<String>,
    /// Duration the session has been blocked (seconds)
    pub blocked_duration: f64,
    /// Schema where the blocking is occurring
    pub schema_name: String,
    /// Object name involved in the lock
    pub object_name: Option<String>,
    /// Object type involved in the lock
    pub object_type: Option<String>,
    /// Current wait event of the blocked session
    pub wait_event: Option<String>,
    /// Seconds waiting for current event
    pub seconds_in_wait: Option<i32>,
}

/// Session statistics grouped by schema
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionsBySchema {
    /// Schema name
    pub schema_name: String,
    /// Total sessions for this schema
    pub total_sessions: u64,
    /// Active sessions for this schema
    pub active_sessions: u64,
    /// Inactive sessions for this schema
    pub inactive_sessions: u64,
    /// Sessions killed but not cleaned up
    pub killed_sessions: u64,
}

/// Oracle wait event information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEvent {
    /// Name of the wait event
    pub event: String,
    /// Wait class category
    pub wait_class: String,
    /// Total number of waits for this event
    pub total_waits: u64,
    /// Total number of timeouts
    pub total_timeouts: u64,
    /// Total time waited (centiseconds)
    pub time_waited: f64,
    /// Average wait time (centiseconds)
    pub average_wait: f64,
    /// Time waited in foreground (centiseconds)
    pub time_waited_fg: f64,
    /// Percentage of total database wait time
    pub pct_of_total_time: f64,
}

/// Overall health summary for Oracle database activity
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleHealthSummary {
    /// Session utilization health
    pub session_health: HealthStatus,
    /// Memory usage health
    pub memory_health: HealthStatus,
    /// Process count health
    pub process_health: HealthStatus,
    /// Blocking session health
    pub blocking_health: HealthStatus,
    /// General performance health
    pub performance_health: HealthStatus,
}
