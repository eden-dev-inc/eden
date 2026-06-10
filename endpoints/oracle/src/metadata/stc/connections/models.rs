use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleConnectionInfo {
    /// Current number of user sessions
    pub current_user_sessions: u64,
    /// Current number of background sessions
    pub current_background_sessions: u64,
    /// Current number of recursive sessions
    pub current_recursive_sessions: u64,
    /// Total active sessions across all types
    pub total_active_sessions: u64,
    /// Maximum sessions allowed (from SESSIONS parameter)
    pub max_sessions: u64,
    /// Maximum processes allowed (from PROCESSES parameter)
    pub max_processes: u64,
    /// Current number of processes
    pub current_processes: u64,
    /// Session utilization percentage (0.0 to 100.0)
    pub session_utilization_pct: f64,
    /// Process utilization percentage (0.0 to 100.0)
    pub process_utilization_pct: f64,
    /// Number of sessions waiting for resources
    pub sessions_waiting: u64,
    /// Number of sessions currently blocking others
    pub sessions_blocking: u64,
    /// Average session memory usage (PGA) in bytes
    pub avg_session_pga: u64,
    /// Total PGA memory allocated in bytes
    pub total_pga_allocated: u64,
    /// PGA memory limit in bytes
    pub pga_aggregate_limit: u64,
    /// Number of sessions that exceeded PGA limit
    pub pga_over_allocation_count: u64,
    /// Current shared pool size in bytes
    pub shared_pool_size: u64,
    /// Shared pool free memory in bytes
    pub shared_pool_free: u64,
    /// Buffer cache size in bytes
    pub buffer_cache_size: u64,
    /// Connection details by service name
    pub connections_by_service: Vec<OracleConnectionsByService>,
    /// Connection details by machine
    pub connections_by_machine: Vec<OracleConnectionsByMachine>,
    /// Connection pooling statistics (if applicable)
    pub connection_pool_stats: Option<OracleConnectionPoolStats>,
    /// Detailed session breakdown
    pub session_breakdown: OracleSessionBreakdown,
}

/// Connection statistics grouped by Oracle service name
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleConnectionsByService {
    /// Service name
    pub service_name: String,
    /// Total connections to this service
    pub total_connections: u64,
    /// Active connections to this service
    pub active_connections: u64,
    /// Inactive connections to this service
    pub inactive_connections: u64,
    /// Killed connections to this service
    pub killed_connections: u64,
    /// Average PGA memory per connection
    pub avg_pga_per_connection: u64,
    /// Longest idle time for any connection
    pub longest_idle_time: i32,
}

/// Connection statistics grouped by client machine
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleConnectionsByMachine {
    /// Machine/hostname
    pub machine_name: String,
    /// Total connections from this machine
    pub total_connections: u64,
    /// Active connections from this machine
    pub active_connections: u64,
    /// Inactive connections from this machine
    pub inactive_connections: u64,
    /// Number of unique users from this machine
    pub unique_users: u64,
    /// Average PGA memory per connection
    pub avg_pga_per_connection: u64,
    /// Earliest login time from this machine
    pub earliest_logon: DateTimeWrapper,
    /// Latest login time from this machine
    pub latest_logon: DateTimeWrapper,
}

/// Oracle Database Resident Connection Pooling (DRCP) or UCP statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleConnectionPoolStats {
    /// Connection pool name
    pub pool_name: String,
    /// Currently active connections
    pub active_connections: u64,
    /// Currently idle connections in pool
    pub idle_connections: u64,
    /// Currently busy connections
    pub busy_connections: u64,
    /// Maximum connections allowed
    pub max_connections: u64,
    /// Minimum connections maintained
    pub min_connections: u64,
    /// Initial number of connections
    pub initial_connections: u64,
    /// Connection increment step
    pub increment_connections: u64,
    /// Connection decrement step
    pub decrement_connections: u64,
    /// Total connection requests
    pub total_requests: u64,
    /// Cache hits (reused connections)
    pub cache_hits: u64,
    /// Cache misses (new connections created)
    pub cache_misses: u64,
    /// Cache hit ratio percentage
    pub hit_ratio: f64,
}

/// Detailed breakdown of sessions by status
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionBreakdown {
    /// Statistics for active sessions
    pub active_sessions: OracleSessionStats,
    /// Statistics for inactive sessions
    pub inactive_sessions: OracleSessionStats,
    /// Statistics for killed sessions
    pub killed_sessions: OracleSessionStats,
    /// Statistics for cached sessions
    pub cached_sessions: OracleSessionStats,
}

/// Session statistics for a specific status
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionStats {
    /// Number of sessions with this status
    pub session_count: u64,
    /// Average PGA memory usage
    pub avg_pga_memory: u64,
    /// Average idle time in seconds
    pub avg_idle_time: f64,
    /// Maximum idle time in seconds
    pub max_idle_time: f64,
    /// Number of sessions blocked by others
    pub blocked_sessions: u64,
    /// Number of sessions blocking others
    pub blocking_sessions: u64,
}
