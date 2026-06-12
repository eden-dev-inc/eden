use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

mod core_sync;
mod detailed_sync;
mod parsers;

/// Clickhouse table information and statistics.
///
/// Covers table storage, partition health and data distribution.
/// Query-specific metrics live in `ClickhouseActivityInfo`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseTableInfo {
    /// Total number of tables across all databases
    pub total_tables: u64,
    /// Total data size across all tables in bytes
    pub total_data_size: u64,
    /// Total number of parts across all tables
    pub total_parts: u64,
    /// Total number of partitions
    pub total_partitions: u64,
    /// Number of tables with excessive parts (indicating merge issues)
    pub tables_with_excessive_parts: u64,
    /// Number of broken or detached parts
    pub broken_parts: u64,
    /// Average compression ratio across all tables
    pub avg_compression_ratio: f64,
    /// Number of tables with recent inserts (last hour)
    pub recently_active_tables: u64,
    /// Total rows across all tables
    pub total_rows: u64,
    /// Number of tables requiring optimization
    pub tables_needing_optimization: u64,
    /// Size of the largest table in bytes
    pub largest_table_size: u64,
    /// Number of tables with old partitions (older than retention policy)
    pub tables_with_old_partitions: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<ClickhouseTableDetailedMetrics>,
}

/// Detailed table metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseTableDetailedMetrics {
    /// Tables with excessive parts (collected when tables_with_excessive_parts > 0)
    pub problematic_tables: Vec<ClickhouseProblematicTable>,
    /// Largest tables by size (collected when storage usage is high)
    pub largest_tables: Vec<ClickhouseLargeTable>,
    /// Tables with broken parts (collected when broken_parts > 0)
    pub tables_with_broken_parts: Vec<ClickhouseBrokenPartsTable>,
    /// Partition distribution analysis (collected when partition issues detected)
    pub partition_analysis: Option<Vec<ClickhousePartitionInfo>>,
    /// Storage breakdown by database
    pub storage_by_database: Option<Vec<ClickhouseStorageByDatabase>>,
}

impl MetadataCollection for ClickhouseTableInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_TABLE_OVERVIEW,
                query(
                    "SELECT
                    count(DISTINCT concat(database, '.', table)) as total_tables,
                    sum(bytes_on_disk) as total_data_size,
                    count() as total_parts,
                    uniq(partition) as total_partitions,
                    sum(rows) as total_rows,
                    avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as avg_compression_ratio,
                    max(bytes_on_disk) as largest_table_size,
                    (SELECT count() FROM (SELECT database, table FROM system.parts WHERE active = 1 GROUP BY database, table HAVING count() > 100)) as tables_with_excessive_parts
                FROM system.parts
                WHERE active = 1"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_TABLE_HEALTH,
                query(
                    "SELECT
                    (SELECT count() FROM system.detached_parts) as broken_parts,
                    (SELECT count(DISTINCT concat(database, '.', table))
                     FROM system.parts
                     WHERE active = 1 AND modification_time > now() - INTERVAL 1 HOUR) as recently_active_tables"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_OPTIMIZATION_STATS,
                query(
                    "SELECT
                    count() as tables_needing_optimization
                FROM (
                    SELECT database, table
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table
                    HAVING count() > 50
                )"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_PARTITION_AGE,
                query(
                    "SELECT
                    count(DISTINCT concat(database, '.', table)) as tables_with_old_partitions
                FROM system.parts
                WHERE active = 1
                    AND partition_id < formatDateTime(now() - INTERVAL 30 DAY, '%Y%m%d')
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse table metrics with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "tables"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseTableInfo {
    const QUERY_TABLE_OVERVIEW: &'static str = "table_overview";
    const QUERY_TABLE_HEALTH: &'static str = "table_health";
    const QUERY_OPTIMIZATION_STATS: &'static str = "optimization_stats";
    const QUERY_PARTITION_AGE: &'static str = "partition_age";
    const EXCESSIVE_PARTS_THRESHOLD: u64 = 100;
    const LARGE_TABLE_THRESHOLD: u64 = 10_737_418_240; // 10GB
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: ClickhouseAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        core_sync::sync_metadata(self, context).await
    }
}

/// Information about tables with problematic part counts
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseProblematicTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of parts in the table
    pub part_count: u64,
    /// Total size in bytes
    pub total_size: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Last modification time
    pub last_modification: DateTimeWrapper,
    /// Number of partitions
    pub partition_count: u64,
    /// Compression ratio
    pub compression_ratio: f64,
}

/// Information about large tables
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargeTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Total size in bytes
    pub total_size: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Number of parts
    pub part_count: u64,
    /// Number of partitions
    pub partition_count: u64,
    /// Last modification time
    pub last_modification: DateTimeWrapper,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Table engine type
    pub engine: String,
}

/// Information about tables with broken parts
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseBrokenPartsTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of broken parts
    pub broken_part_count: u64,
    /// Time of last error
    pub last_error_time: DateTimeWrapper,
    /// Sample exception message
    pub sample_exception: Option<String>,
}

/// Information about table partitions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartitionInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Partition identifier
    pub partition: String,
    /// Partition size in bytes
    pub partition_size: u64,
    /// Number of rows in partition
    pub partition_rows: u64,
    /// Number of parts in partition
    pub part_count: u64,
    /// Oldest date in partition
    pub oldest_date: DateTimeWrapper,
    /// Newest date in partition
    pub newest_date: DateTimeWrapper,
}

/// Storage information grouped by database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStorageByDatabase {
    /// Database name
    pub database: String,
    /// Number of tables in database
    pub table_count: u64,
    /// Total storage size in bytes
    pub total_size: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Total number of parts
    pub total_parts: u64,
}

impl ClickhouseTableInfo {
    /// Checks if there are tables with excessive parts
    pub fn has_excessive_parts(&self) -> bool {
        self.tables_with_excessive_parts > 0
    }

    /// Checks if there are broken parts
    pub fn has_broken_parts(&self) -> bool {
        self.broken_parts > 0
    }

    /// Checks if there are tables needing optimization
    pub fn has_optimization_needed(&self) -> bool {
        self.tables_needing_optimization > 0
    }

    /// Checks if there are old partitions
    pub fn has_old_partitions(&self) -> bool {
        self.tables_with_old_partitions > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets total storage in GB
    pub fn get_total_storage_gb(&self) -> f64 {
        self.total_data_size as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets average table size in MB
    pub fn get_avg_table_size_mb(&self) -> f64 {
        if self.total_tables == 0 {
            0.0
        } else {
            (self.total_data_size as f64 / self.total_tables as f64) / 1_048_576.0
            // Convert to MB
        }
    }

    /// Gets average parts per table
    pub fn get_avg_parts_per_table(&self) -> f64 {
        if self.total_tables == 0 {
            0.0
        } else {
            self.total_parts as f64 / self.total_tables as f64
        }
    }

    /// Checks if storage usage is concerning
    pub fn has_concerning_storage(&self, threshold_gb: f64) -> bool {
        self.get_total_storage_gb() > threshold_gb
    }
}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_metadata_table_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let table_info = ClickhouseTableInfo::default();
//
//         let result = table_info
//             .sync_metadata(
//                 clickhouse_ep
//                     .0
//                     .read_conn_async(&endpoint_cache_uuid, telemetry_wrapper)
//                     .await?;
//                     .expect("failed to get connection")
//                     .to_owned(),
//                 telemetry_wrapper,
//             )
//             .await;
//
//         assert!(result.is_ok());
//         let info = result.unwrap_or_default();
//
//         // Verify core metrics are collected
//         assert!(info.total_tables >= 0);
//         assert!(info.total_data_size >= 0);
//         assert!(info.avg_compression_ratio >= 0.0);
//     }
//
//     #[test]
//     fn test_clickhouse_table_calculations() {
//         let mut table_info = ClickhouseTableInfo::default();
//         table_info.total_tables = 10;
//         table_info.total_data_size = 10_737_418_240; // 10GB
//         table_info.total_parts = 150;
//         table_info.tables_with_excessive_parts = 2;
//         table_info.broken_parts = 1;
//
//         assert_eq!(table_info.get_total_storage_gb(), 10.0);
//         assert_eq!(table_info.get_avg_table_size_mb(), 1024.0); // 1GB per table in MB
//         assert_eq!(table_info.get_avg_parts_per_table(), 15.0);
//         assert!(table_info.has_excessive_parts());
//         assert!(table_info.has_broken_parts());
//         assert!(table_info.has_concerning_storage(5.0)); // Above 5GB threshold
//     }
//
//     #[test]
//     fn test_table_info_health_checks() {
//         let mut table_info = ClickhouseTableInfo::default();
//
//         // Test healthy state
//         assert!(!table_info.has_excessive_parts());
//         assert!(!table_info.has_broken_parts());
//         assert!(!table_info.has_optimization_needed());
//         assert!(!table_info.has_old_partitions());
//         assert!(!table_info.has_detailed_metrics());
//
//         // Test problematic state
//         table_info.tables_with_excessive_parts = 5;
//         table_info.broken_parts = 3;
//         table_info.tables_needing_optimization = 8;
//         table_info.tables_with_old_partitions = 2;
//
//         assert!(table_info.has_excessive_parts());
//         assert!(table_info.has_broken_parts());
//         assert!(table_info.has_optimization_needed());
//         assert!(table_info.has_old_partitions());
//     }
//
//     #[test]
//     fn test_edge_cases() {
//         let table_info = ClickhouseTableInfo::default();
//
//         // Test division by zero cases
//         assert_eq!(table_info.get_avg_table_size_mb(), 0.0);
//         assert_eq!(table_info.get_avg_parts_per_table(), 0.0);
//
//         // Test zero storage
//         assert_eq!(table_info.get_total_storage_gb(), 0.0);
//         assert!(!table_info.has_concerning_storage(1.0));
//     }
// }
