use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDatabaseStats {
    /// Database instance identifier
    pub instance_id: i32,
    /// Database name
    pub database_name: String,
    /// Database unique name
    pub db_unique_name: String,
    /// Database role (PRIMARY, PHYSICAL STANDBY, LOGICAL STANDBY etc.)
    pub database_role: String,
    /// Database status (OPEN, MOUNTED etc.)
    pub database_status: String,
    /// Instance status (OPEN, STARTED etc.)
    pub instance_status: String,
    /// Database startup time
    pub startup_time: DateTimeWrapper,
    /// Database uptime in seconds
    pub uptime_seconds: f64,
    /// Database version
    pub version: String,
    /// Host name where the instance is running
    pub host_name: String,

    // Performance Statistics
    /// Buffer cache hit ratio (percentage)
    pub buffer_cache_hit_ratio: f64,
    /// Library cache hit ratio (percentage)
    pub library_cache_hit_ratio: f64,
    /// Dictionary cache hit ratio (percentage)
    pub dictionary_cache_hit_ratio: f64,
    /// Data dictionary cache hit ratio (percentage)
    pub data_dict_cache_hit_ratio: f64,
    /// Parse ratio (percentage of soft parses)
    pub soft_parse_ratio: f64,
    /// Execute to parse ratio
    pub execute_to_parse_ratio: f64,

    // I/O Statistics
    /// Physical reads per second
    pub physical_reads_per_sec: f64,
    /// Physical writes per second
    pub physical_writes_per_sec: f64,
    /// Logical reads per second
    pub logical_reads_per_sec: f64,
    /// Block changes per second
    pub block_changes_per_sec: f64,
    /// Redo size per second (bytes)
    pub redo_size_per_sec: f64,
    /// User calls per second
    pub user_calls_per_sec: f64,
    /// Transactions per second
    pub transactions_per_sec: f64,
    /// SQL executions per second
    pub executions_per_sec: f64,

    // Transaction Statistics
    /// Total number of user commits
    pub user_commits: u64,
    /// Total number of user rollbacks
    pub user_rollbacks: u64,
    /// User transaction rate (commits + rollbacks per second)
    pub user_transaction_rate: f64,
    /// User commit percentage
    pub user_commit_percentage: f64,

    // Session and Process Statistics
    /// Current number of sessions
    pub current_sessions: u64,
    /// Current number of processes
    pub current_processes: u64,
    /// Peak sessions since startup
    pub peak_sessions: u64,
    /// Peak processes since startup
    pub peak_processes: u64,

    // Memory Statistics
    /// SGA size in bytes
    pub sga_size: u64,
    /// PGA aggregate target in bytes
    pub pga_aggregate_target: u64,
    /// Current PGA used in bytes
    pub pga_used: u64,
    /// Shared pool size in bytes
    pub shared_pool_size: u64,
    /// Buffer cache size in bytes
    pub buffer_cache_size: u64,
    /// Log buffer size in bytes
    pub log_buffer_size: u64,

    // Wait Event Statistics
    /// Top wait events affecting performance
    pub top_wait_events: Vec<OracleWaitEventStats>,

    // Database Size and Growth
    /// Total database size in bytes
    pub database_size: u64,
    /// Used space in bytes
    pub used_space: u64,
    /// Free space in bytes
    pub free_space: u64,
    /// Database growth rate (bytes per day)
    pub growth_rate_per_day: f64,

    // Tablespace Statistics Summary
    /// Number of tablespaces
    pub tablespace_count: u64,
    /// Number of datafiles
    pub datafile_count: u64,
    /// Number of control files
    pub controlfile_count: u64,
    /// Number of redo log groups
    pub redo_log_groups: u64,

    // CPU and Resource Statistics
    /// CPU usage percentage
    pub cpu_usage_percentage: f64,
    /// Database CPU time (microseconds)
    pub db_cpu_time: u64,
    /// Background CPU time (microseconds)
    pub background_cpu_time: u64,
    /// Parse CPU time (microseconds)
    pub parse_cpu_time: u64,

    // Archive Log Statistics
    /// Archive log generation rate (MB per hour)
    pub archive_log_rate_mb_per_hour: f64,
    /// Number of archive logs generated today
    pub archive_logs_today: u64,
    /// Average archive log size in bytes
    pub avg_archive_log_size: u64,

    // Additional Performance Metrics
    /// Response time per transaction (seconds)
    pub response_time_per_txn: f64,
    /// SQL service response time (seconds)
    pub sql_service_response_time: f64,
    /// Database time per second
    pub database_time_per_sec: f64,
    /// Background time per second
    pub background_time_per_sec: f64,

    /// Collection timestamp
    pub collection_timestamp: DateTimeWrapper,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEventStats {
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
    /// Percentage of total database wait time
    pub pct_of_total_time: f64,
}

pub type DatabaseHealthStatus = HealthStatus;

/// Database health assessment.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDatabaseHealth {
    /// Overall health status
    pub overall_status: DatabaseHealthStatus,
    /// List of identified issues
    pub issues: Vec<String>,
    /// Current buffer cache hit ratio
    pub buffer_cache_hit_ratio: f64,
    /// Current library cache hit ratio
    pub library_cache_hit_ratio: f64,
    /// Current soft parse ratio
    pub soft_parse_ratio: f64,
    /// Current space utilization percentage
    pub space_utilization_pct: f64,
    /// Current PGA utilization percentage
    pub pga_utilization_pct: f64,
    /// Current CPU usage percentage
    pub cpu_usage_pct: f64,
    /// Name of the top wait event
    pub top_wait_event_name: Option<String>,
    /// Percentage of total wait time for top event
    pub top_wait_event_pct: f64,
}
