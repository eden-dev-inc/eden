use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleRedoLogInfo {
    /// Current redo log sequence number
    pub current_sequence: u64,
    /// Total number of online redo log groups
    pub total_log_groups: u64,
    /// Number of active log groups
    pub active_log_groups: u64,
    /// Number of inactive log groups
    pub inactive_log_groups: u64,
    /// Current redo log group being written to
    pub current_log_group: u64,
    /// Size of each redo log file in bytes
    pub log_file_size: u64,
    /// Current redo generation rate (bytes per second)
    pub redo_generation_rate: f64,
    /// Average log switch frequency (switches per hour)
    pub log_switch_frequency: f64,
    /// Time since last log switch in seconds
    pub time_since_last_switch: f64,
    /// Number of log switches in the last hour
    pub switches_last_hour: u64,
    /// Archive lag in seconds (0 if archiving is current)
    pub archive_lag_seconds: f64,
    /// Number of pending archive operations
    pub pending_archive_count: u64,
    /// Archive destination space usage percentage
    pub archive_dest_usage_pct: f64,
    /// Current SCN (System Change Number)
    pub current_scn: u64,
    /// Checkpoint SCN
    pub checkpoint_scn: u64,
    /// SCN gap (current - checkpoint)
    pub scn_gap: u64,
    /// Log buffer hit ratio percentage
    pub log_buffer_hit_ratio: f64,
    /// Average redo write time in milliseconds
    pub avg_redo_write_time: f64,
    /// Total redo size generated today in bytes
    pub redo_size_today: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<OracleRedoDetailedMetrics>,
}

/// Detailed redo log metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleRedoDetailedMetrics {
    /// Individual log group information
    pub log_groups: Vec<OracleLogGroup>,
    /// Archive destination details (only collected when archive issues detected)
    pub archive_destinations: Option<Vec<OracleArchiveDestination>>,
    /// Recent log switch history (collected when frequent switching detected)
    pub recent_log_switches: Option<Vec<OracleLogSwitch>>,
    /// Redo wait events (collected when performance issues detected)
    pub redo_wait_events: Option<Vec<OracleRedoWaitEvent>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLogGroup {
    /// Log group number
    pub group_number: i32,
    /// Thread number (for RAC)
    pub thread_number: i32,
    /// Current sequence number
    pub sequence_number: u64,
    /// Size of the log group in bytes
    pub size_bytes: u64,
    /// Current status (CURRENT, ACTIVE, INACTIVE etc.)
    pub status: String,
    /// Archive status (YES, NO)
    pub archived: String,
    /// Path to the log file
    pub file_path: String,
    /// Type of log file
    pub file_type: Option<String>,
    /// Whether file is in recovery destination
    pub is_recovery_dest_file: Option<String>,
}

/// Archive destination information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleArchiveDestination {
    /// Archive destination ID
    pub dest_id: i32,
    /// Archive destination name
    pub dest_name: Option<String>,
    /// Archive destination path
    pub destination: Option<String>,
    /// Current status
    pub status: String,
    /// Binding type
    pub binding: Option<String>,
    /// Target type
    pub target: Option<String>,
    /// Archiver process
    pub archiver: Option<String>,
    /// Archive schedule
    pub schedule: Option<String>,
    /// Current process
    pub process: Option<String>,
    /// Last error message
    pub error: Option<String>,
    /// Failed sequence number
    pub fail_sequence: Option<i32>,
    /// Failed block number
    pub fail_block: Option<i32>,
    /// Date of last failure
    pub fail_date: Option<DateTimeWrapper>,
}

/// Log switch history information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLogSwitch {
    /// Thread number
    pub thread_number: i32,
    /// Sequence number
    pub sequence_number: u64,
    /// First SCN in the log
    pub first_change: u64,
    /// Next SCN after the log
    pub next_change: u64,
    /// Time when log became current
    pub first_time: DateTimeWrapper,
    /// Time when log was archived
    pub next_time: Option<DateTimeWrapper>,
    /// Number of changes in the log
    pub changes: u64,
    /// Duration the log was current (seconds)
    pub duration_seconds: f64,
}

/// Redo-related wait event information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleRedoWaitEvent {
    /// Name of the wait event
    pub event: String,
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

pub type RedoHealthStatus = HealthStatus;

/// Overall health summary for Oracle redo log activity
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleRedoHealthSummary {
    /// Log switching frequency health
    pub log_switch_health: RedoHealthStatus,
    /// Archive operation health
    pub archive_health: RedoHealthStatus,
    /// Redo performance health
    pub performance_health: RedoHealthStatus,
    /// SCN progression health
    pub scn_health: RedoHealthStatus,
}
