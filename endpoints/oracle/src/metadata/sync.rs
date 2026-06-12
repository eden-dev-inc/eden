use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLastSyncTimestamps {
    // high priority sync timestamps - critical performance and connection metrics
    pub activity_info_last_sync: u64,     // current SQL execution, blocking sessions
    pub connection_info_last_sync: u64,   // connection pool status, session limits
    pub lock_info_last_sync: u64,         // blocking locks, deadlocks
    pub performance_stats_last_sync: u64, // AWR stats, buffer cache hits, SQL performance
    pub session_info_last_sync: u64,      // active sessions, resource usage
    pub transaction_info_last_sync: u64,  // transaction stats, undo usage
    pub wait_events_last_sync: u64,       // wait events, bottlenecks

    // medium priority sync timestamps - operational metrics
    pub database_stats_last_sync: u64,  // per-database statistics
    pub index_info_last_sync: u64,      // index usage, rebuilds needed
    pub redolog_info_last_sync: u64,    // redo log switches, archiving
    pub segment_info_last_sync: u64,    // segment growth, space usage
    pub storage_info_last_sync: u64,    // datafile usage, temp space
    pub table_info_last_sync: u64,      // table statistics, growth
    pub tablespace_info_last_sync: u64, // tablespace usage, autoextend

    // low priority sync timestamps - configuration and static info
    pub parameter_info_last_sync: u64, // database parameters, configuration
}
