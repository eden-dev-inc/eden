use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLastSyncTimestamps {
    // high priority sync timestamps
    pub activity_info_last_sync: u64,
    pub connection_info_last_sync: u64,
    pub lock_info_last_sync: u64,
    pub performance_stats_last_sync: u64,
    pub replication_info_last_sync: u64,
    pub transaction_info_last_sync: u64,
    pub wal_info_last_sync: u64,

    // medium priority sync timestamps
    pub bgwriter_info_last_sync: u64,
    pub database_stats_last_sync: u64,
    pub index_info_last_sync: u64,
    pub table_info_last_sync: u64,
    pub vacuum_info_last_sync: u64,

    // low priority sync timestamps
    pub extension_info_last_sync: u64,
    pub settings_info_last_sync: u64,
}
