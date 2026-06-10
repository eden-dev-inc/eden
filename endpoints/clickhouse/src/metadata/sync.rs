use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

/// ClickHouse last sync timestamps for different priority levels
///
/// This tracks when each metadata component was last synchronized,
/// organized by sync frequency priority (High, Medium, Low).
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLastSyncTimestamps {
    // High priority sync timestamps - critical performance and cluster health
    /// Last sync time for activity info (current queries, processes)
    pub activity_info_last_sync: u64,
    /// Last sync time for connection info (connection pool status)
    pub connection_info_last_sync: u64,
    /// Last sync time for query info (query performance, slow queries)
    pub query_info_last_sync: u64,
    /// Last sync time for cluster info (cluster topology, shard health)
    pub cluster_info_last_sync: u64,
    /// Last sync time for replication info (replica status, replication lag)
    pub replication_info_last_sync: u64,
    /// Last sync time for storage info (disk usage, storage metrics)
    pub storage_info_last_sync: u64,
    /// Last sync time for ZooKeeper info (ZooKeeper connectivity, coordination)
    pub zookeeper_info_last_sync: u64,

    // Medium priority sync timestamps - operational metrics for maintenance
    /// Last sync time for merge info (merge tree operations)
    pub merge_info_last_sync: u64,
    /// Last sync time for mutation info (ALTER table mutations)
    pub mutation_info_last_sync: u64,
    /// Last sync time for part info (table parts, merges needed)
    pub part_info_last_sync: u64,
    /// Last sync time for database stats (per-database statistics)
    pub database_stats_last_sync: u64,
    /// Last sync time for table info (table statistics, compression)
    pub table_info_last_sync: u64,

    // Low priority sync timestamps - configuration and static info
    /// Last sync time for dictionary info (external dictionaries)
    pub dictionary_info_last_sync: u64,
    /// Last sync time for settings info (configuration parameters)
    pub settings_info_last_sync: u64,
}

impl ClickhouseLastSyncTimestamps {
    /// Creates a new instance with all timestamps set to 0
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the oldest sync timestamp across all high priority components
    pub fn oldest_high_priority_sync(&self) -> u64 {
        [
            self.activity_info_last_sync,
            self.connection_info_last_sync,
            self.query_info_last_sync,
            self.cluster_info_last_sync,
            self.replication_info_last_sync,
            self.storage_info_last_sync,
            self.zookeeper_info_last_sync,
        ]
        .iter()
        .min()
        .copied()
        .unwrap_or(0)
    }

    /// Gets the oldest sync timestamp across all medium priority components
    pub fn oldest_medium_priority_sync(&self) -> u64 {
        [
            self.merge_info_last_sync,
            self.mutation_info_last_sync,
            self.part_info_last_sync,
            self.database_stats_last_sync,
            self.table_info_last_sync,
        ]
        .iter()
        .min()
        .copied()
        .unwrap_or(0)
    }

    /// Gets the oldest sync timestamp across all low priority components
    pub fn oldest_low_priority_sync(&self) -> u64 {
        [self.dictionary_info_last_sync, self.settings_info_last_sync].iter().min().copied().unwrap_or(0)
    }

    /// Gets the most recent sync timestamp across all components
    pub fn most_recent_sync(&self) -> u64 {
        [
            // High priority
            self.activity_info_last_sync,
            self.connection_info_last_sync,
            self.query_info_last_sync,
            self.cluster_info_last_sync,
            self.replication_info_last_sync,
            self.storage_info_last_sync,
            self.zookeeper_info_last_sync,
            // Medium priority
            self.merge_info_last_sync,
            self.mutation_info_last_sync,
            self.part_info_last_sync,
            self.database_stats_last_sync,
            self.table_info_last_sync,
            // Low priority
            self.dictionary_info_last_sync,
            self.settings_info_last_sync,
        ]
        .iter()
        .max()
        .copied()
        .unwrap_or(0)
    }

    /// Checks if any high priority component needs syncing based on the given threshold
    pub fn needs_high_priority_sync(&self, threshold_seconds: u64) -> bool {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        let oldest = self.oldest_high_priority_sync();
        current_time.saturating_sub(oldest) > threshold_seconds
    }

    /// Checks if any medium priority component needs syncing based on the given threshold
    pub fn needs_medium_priority_sync(&self, threshold_seconds: u64) -> bool {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        let oldest = self.oldest_medium_priority_sync();
        current_time.saturating_sub(oldest) > threshold_seconds
    }

    /// Checks if any low priority component needs syncing based on the given threshold
    pub fn needs_low_priority_sync(&self, threshold_seconds: u64) -> bool {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        let oldest = self.oldest_low_priority_sync();
        current_time.saturating_sub(oldest) > threshold_seconds
    }

    /// Returns a summary of sync status for debugging
    pub fn sync_status_summary(&self) -> String {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        let high_age = current_time.saturating_sub(self.oldest_high_priority_sync());
        let medium_age = current_time.saturating_sub(self.oldest_medium_priority_sync());
        let low_age = current_time.saturating_sub(self.oldest_low_priority_sync());

        format!(
            "ClickHouse Sync Status - High: {}s ago, Medium: {}s ago, Low: {}s ago",
            high_age, medium_age, low_age
        )
    }

    /// Resets all timestamps to 0 (useful for testing or force refresh)
    pub fn reset_all(&mut self) {
        *self = Self::default();
    }

    /// Updates all high priority timestamps to current time
    pub fn update_high_priority_timestamps(&mut self) {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        self.activity_info_last_sync = current_time;
        self.connection_info_last_sync = current_time;
        self.query_info_last_sync = current_time;
        self.cluster_info_last_sync = current_time;
        self.replication_info_last_sync = current_time;
        self.storage_info_last_sync = current_time;
        self.zookeeper_info_last_sync = current_time;
    }

    /// Updates all medium priority timestamps to current time
    pub fn update_medium_priority_timestamps(&mut self) {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        self.merge_info_last_sync = current_time;
        self.mutation_info_last_sync = current_time;
        self.part_info_last_sync = current_time;
        self.database_stats_last_sync = current_time;
        self.table_info_last_sync = current_time;
    }

    /// Updates all low priority timestamps to current time
    pub fn update_low_priority_timestamps(&mut self) {
        let current_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

        self.dictionary_info_last_sync = current_time;
        self.settings_info_last_sync = current_time;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_clickhouse_sync_timestamps_creation() {
        let timestamps = ClickhouseLastSyncTimestamps::new();

        // Verify all timestamps start at 0
        assert_eq!(timestamps.activity_info_last_sync, 0);
        assert_eq!(timestamps.connection_info_last_sync, 0);
        assert_eq!(timestamps.query_info_last_sync, 0);
        assert_eq!(timestamps.cluster_info_last_sync, 0);
        assert_eq!(timestamps.replication_info_last_sync, 0);
        assert_eq!(timestamps.storage_info_last_sync, 0);
        assert_eq!(timestamps.zookeeper_info_last_sync, 0);
        assert_eq!(timestamps.merge_info_last_sync, 0);
        assert_eq!(timestamps.mutation_info_last_sync, 0);
        assert_eq!(timestamps.part_info_last_sync, 0);
        assert_eq!(timestamps.database_stats_last_sync, 0);
        assert_eq!(timestamps.table_info_last_sync, 0);
        assert_eq!(timestamps.dictionary_info_last_sync, 0);
        assert_eq!(timestamps.settings_info_last_sync, 0);
    }

    #[test]
    fn test_oldest_sync_calculations() {
        let timestamps = ClickhouseLastSyncTimestamps {
            activity_info_last_sync: 100,
            connection_info_last_sync: 200,
            query_info_last_sync: 50, // This should be the oldest for high priority
            cluster_info_last_sync: 175,
            replication_info_last_sync: 180,
            storage_info_last_sync: 190,
            zookeeper_info_last_sync: 195,
            merge_info_last_sync: 300,
            mutation_info_last_sync: 150, // This should be the oldest for medium priority
            dictionary_info_last_sync: 400,
            settings_info_last_sync: 250, // This should be the oldest for low priority
            ..Default::default()
        };

        // Set different timestamps for testing

        assert_eq!(timestamps.oldest_high_priority_sync(), 50);
        assert_eq!(timestamps.oldest_medium_priority_sync(), 0); // part_info_last_sync is still 0
        assert_eq!(timestamps.oldest_low_priority_sync(), 250);
        assert_eq!(timestamps.most_recent_sync(), 400);
    }

    #[test]
    fn test_needs_sync_logic() {
        let mut timestamps = ClickhouseLastSyncTimestamps::default();
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        // Set timestamps to 1 hour ago
        let one_hour_ago = current_time - 3600;
        timestamps.activity_info_last_sync = one_hour_ago;
        timestamps.connection_info_last_sync = one_hour_ago;
        timestamps.query_info_last_sync = one_hour_ago;
        timestamps.cluster_info_last_sync = one_hour_ago;
        timestamps.replication_info_last_sync = one_hour_ago;
        timestamps.storage_info_last_sync = one_hour_ago;
        timestamps.zookeeper_info_last_sync = one_hour_ago;
        timestamps.merge_info_last_sync = one_hour_ago;
        timestamps.mutation_info_last_sync = one_hour_ago;
        timestamps.part_info_last_sync = one_hour_ago;
        timestamps.database_stats_last_sync = one_hour_ago;
        timestamps.table_info_last_sync = one_hour_ago;
        timestamps.dictionary_info_last_sync = one_hour_ago;
        timestamps.settings_info_last_sync = one_hour_ago;

        // Test different thresholds
        assert!(timestamps.needs_high_priority_sync(1800)); // 30 minutes threshold
        assert!(!timestamps.needs_high_priority_sync(7200)); // 2 hours threshold

        assert!(timestamps.needs_medium_priority_sync(1800));
        assert!(!timestamps.needs_medium_priority_sync(7200));

        assert!(timestamps.needs_low_priority_sync(1800));
        assert!(!timestamps.needs_low_priority_sync(7200));
    }

    #[test]
    fn test_update_priority_timestamps() {
        let mut timestamps = ClickhouseLastSyncTimestamps::default();

        timestamps.update_high_priority_timestamps();
        assert!(timestamps.activity_info_last_sync > 0);
        assert!(timestamps.connection_info_last_sync > 0);
        assert!(timestamps.query_info_last_sync > 0);
        assert!(timestamps.cluster_info_last_sync > 0);
        assert!(timestamps.replication_info_last_sync > 0);
        assert!(timestamps.storage_info_last_sync > 0);
        assert!(timestamps.zookeeper_info_last_sync > 0);

        // Medium and low should still be 0
        assert_eq!(timestamps.merge_info_last_sync, 0);
        assert_eq!(timestamps.dictionary_info_last_sync, 0);

        timestamps.update_medium_priority_timestamps();
        assert!(timestamps.merge_info_last_sync > 0);
        assert!(timestamps.mutation_info_last_sync > 0);
        assert!(timestamps.part_info_last_sync > 0);
        assert!(timestamps.database_stats_last_sync > 0);
        assert!(timestamps.table_info_last_sync > 0);

        timestamps.update_low_priority_timestamps();
        assert!(timestamps.dictionary_info_last_sync > 0);
        assert!(timestamps.settings_info_last_sync > 0);
    }

    #[test]
    fn test_reset_all() {
        let mut timestamps = ClickhouseLastSyncTimestamps {
            activity_info_last_sync: 12345,
            merge_info_last_sync: 67890,
            dictionary_info_last_sync: 54321,
            ..Default::default()
        };

        // Set some timestamps

        // Reset all
        timestamps.reset_all();

        // Verify all are back to 0
        assert_eq!(timestamps.activity_info_last_sync, 0);
        assert_eq!(timestamps.merge_info_last_sync, 0);
        assert_eq!(timestamps.dictionary_info_last_sync, 0);
    }

    #[test]
    fn test_sync_status_summary() {
        let mut timestamps = ClickhouseLastSyncTimestamps::default();
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        // Set timestamps to specific times ago
        timestamps.activity_info_last_sync = current_time - 60; // 1 minute ago
        timestamps.merge_info_last_sync = current_time - 300; // 5 minutes ago
        timestamps.dictionary_info_last_sync = current_time - 900; // 15 minutes ago

        let summary = timestamps.sync_status_summary();
        assert!(summary.contains("ClickHouse Sync Status"));
        assert!(summary.contains("High:"));
        assert!(summary.contains("Medium:"));
        assert!(summary.contains("Low:"));
    }

    #[test]
    fn test_edge_cases() {
        let timestamps = ClickhouseLastSyncTimestamps::default();

        // All zeros should return 0 for oldest calculations
        assert_eq!(timestamps.oldest_high_priority_sync(), 0);
        assert_eq!(timestamps.oldest_medium_priority_sync(), 0);
        assert_eq!(timestamps.oldest_low_priority_sync(), 0);
        assert_eq!(timestamps.most_recent_sync(), 0);

        // Should need sync when all timestamps are 0
        assert!(timestamps.needs_high_priority_sync(1));
        assert!(timestamps.needs_medium_priority_sync(1));
        assert!(timestamps.needs_low_priority_sync(1));
    }
}
