use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

/// Tracks the last sync timestamps for all Cassandra metadata categories
///
/// This struct maintains sync timestamps for each metadata component to enable
/// efficient per-category sync scheduling based on priority levels:
/// - High priority metrics: Updated frequently for critical monitoring
/// - Medium priority metrics: Updated moderately for operational insights
/// - Low priority metrics: Updated less frequently for configuration tracking
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct CassandraLastSyncTimestamps {
    // High priority - critical cluster health and performance metrics
    pub cluster_info_last_sync: u64,    // cluster status and node availability
    pub node_info_last_sync: u64,       // per-node health and performance
    pub threadpool_info_last_sync: u64, // thread pool statistics
    pub compaction_info_last_sync: u64, // compaction status and metrics
    pub repair_info_last_sync: u64,     // repair status and history
    pub tombstone_info_last_sync: u64,  // tombstone warnings and counts

    // Medium priority - operational metrics
    pub keyspace_info_last_sync: u64, // per-keyspace statistics
    pub table_info_last_sync: u64,    // table statistics and metrics
    pub snapshot_info_last_sync: u64, // snapshot status and management

    // Low priority - configuration and static info
    pub schema_info_last_sync: u64, // schema definitions and versions
}

impl CassandraLastSyncTimestamps {
    /// Creates a new instance with all timestamps set to 0
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all timestamps to 0, forcing a full resync on next collection
    pub fn reset_all(&mut self) {
        *self = Self::default();
    }

    /// Resets only high priority timestamps for immediate critical metric collection
    pub fn reset_high_priority(&mut self) {
        self.cluster_info_last_sync = 0;
        self.node_info_last_sync = 0;
        self.threadpool_info_last_sync = 0;
        self.compaction_info_last_sync = 0;
        self.repair_info_last_sync = 0;
        self.tombstone_info_last_sync = 0;
    }

    /// Resets only medium priority timestamps for operational metric collection
    pub fn reset_medium_priority(&mut self) {
        self.keyspace_info_last_sync = 0;
        self.table_info_last_sync = 0;
        self.snapshot_info_last_sync = 0;
    }

    /// Resets only low priority timestamps for configuration metric collection
    pub fn reset_low_priority(&mut self) {
        self.schema_info_last_sync = 0;
    }

    /// Returns the oldest timestamp across all categories
    pub fn oldest_sync(&self) -> u64 {
        [
            self.cluster_info_last_sync,
            self.node_info_last_sync,
            self.threadpool_info_last_sync,
            self.compaction_info_last_sync,
            self.repair_info_last_sync,
            self.tombstone_info_last_sync,
            self.keyspace_info_last_sync,
            self.table_info_last_sync,
            self.snapshot_info_last_sync,
            self.schema_info_last_sync,
        ]
        .iter()
        .min()
        .copied()
        .unwrap_or(0)
    }

    /// Returns the most recent timestamp across all categories
    pub fn newest_sync(&self) -> u64 {
        [
            self.cluster_info_last_sync,
            self.node_info_last_sync,
            self.threadpool_info_last_sync,
            self.compaction_info_last_sync,
            self.repair_info_last_sync,
            self.tombstone_info_last_sync,
            self.keyspace_info_last_sync,
            self.table_info_last_sync,
            self.snapshot_info_last_sync,
            self.schema_info_last_sync,
        ]
        .iter()
        .max()
        .copied()
        .unwrap_or(0)
    }
}
