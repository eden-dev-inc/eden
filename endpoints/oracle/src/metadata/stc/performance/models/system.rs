use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SystemStatistics {
    /// CPU utilization percentage
    pub cpu_utilization: f64,
    /// Database CPU time (centiseconds)
    pub db_cpu_time: u64,
    /// Database time (centiseconds)
    pub db_time: u64,
    /// User calls per second
    pub user_calls_per_sec: f64,
    /// Parse count (total)
    pub parse_count_total: u64,
    /// Parse count (hard)
    pub parse_count_hard: u64,
    /// Execute count
    pub execute_count: u64,
    /// Logical reads per second
    pub logical_reads_per_sec: f64,
    /// Physical reads per second
    pub physical_reads_per_sec: f64,
    /// Physical writes per second
    pub physical_writes_per_sec: f64,
    /// Redo generation rate (bytes/sec)
    pub redo_generation_rate: f64,
    /// Buffer cache hit ratio
    pub buffer_cache_hit_ratio: f64,
    /// Library cache hit ratio
    pub library_cache_hit_ratio: f64,
    /// Shared pool free percentage
    pub shared_pool_free_pct: f64,
    /// PGA memory utilization
    pub pga_memory_utilization: f64,
    /// Active sessions count
    pub active_sessions: u32,
    /// Blocked sessions count
    pub blocked_sessions: u32,
    /// Connection count
    pub connection_count: u32,
    /// Commit count per second
    pub commits_per_sec: f64,
    /// Rollback count per second
    pub rollbacks_per_sec: f64,
    /// Network bytes per second
    pub network_bytes_per_sec: f64,
}

/// Wait event statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WaitEventStat {
    /// Wait event name
    pub event_name: String,
    /// Wait class
    pub wait_class: String,
    /// Total waits
    pub total_waits: u64,
    /// Total wait time (centiseconds)
    pub total_wait_time: u64,
    /// Average wait time (centiseconds)
    pub average_wait_time: f64,
    /// Percentage of total database time
    pub pct_db_time: f64,
    /// Waits per second
    pub waits_per_sec: f64,
    /// Wait time per second
    pub wait_time_per_sec: f64,
    /// Event severity level
    pub severity: WaitEventSeverity,
    /// Event category
    pub category: WaitEventCategory,
}
