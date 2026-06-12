use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLastSyncTimestamps {
    // high priority sync timestamps
    pub connection_info_last_sync: u64,
    pub lock_info_last_sync: u64,
    pub network_info_last_sync: u64,
    pub performance_stats_last_sync: u64,
    pub replication_info_last_sync: u64,
    pub server_info_last_sync: u64,
    pub transaction_info_last_sync: u64,
    pub wiredtiger_info_last_sync: u64,

    // medium priority sync timestamps
    pub aggregation_stats_last_sync: u64,
    pub collection_info_last_sync: u64,
    pub database_stats_last_sync: u64,
    pub gridfs_info_last_sync: u64,
    pub index_info_last_sync: u64,
    pub oplog_info_last_sync: u64,
    pub profiler_info_last_sync: u64,
    pub sharding_info_last_sync: u64,

    // low priority sync timestamps
    pub balancer_info_last_sync: u64,
    pub memory_info_last_sync: u64,
    pub security_info_last_sync: u64,
    pub user_info_last_sync: u64,
}
