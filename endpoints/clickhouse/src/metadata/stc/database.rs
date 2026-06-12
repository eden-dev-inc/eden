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

/// Clickhouse database statistics and storage information.
///
/// Covers database sizes, table metrics and disk usage.
/// Query performance metrics live in `ClickhouseActivityInfo`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDatabaseStats {
    /// Total number of databases
    pub total_databases: u64,
    /// Total number of tables across all databases
    pub total_tables: u64,
    /// Total disk space used by all databases in bytes
    pub total_disk_usage: u64,
    /// Total number of rows across all tables
    pub total_rows: u64,
    /// Total number of parts across all tables
    pub total_parts: u64,
    /// Number of active parts (not yet merged)
    pub active_parts: u64,
    /// Average compression ratio across all tables
    pub avg_compression_ratio: f64,
    /// Total uncompressed data size in bytes
    pub total_uncompressed_size: u64,
    /// Total compressed data size in bytes
    pub total_compressed_size: u64,
    /// Number of tables that need optimization (high part count)
    pub tables_needing_optimization: u64,
    /// Number of detached parts
    pub detached_parts: u64,
    /// Number of temporary tables
    pub temporary_tables: u64,
    /// Detailed database breakdown collected when requested
    pub detailed_stats: Option<ClickhouseDatabaseDetailedStats>,
}

/// Detailed database statistics collected when needed
///
/// This provides granular information about individual databases and tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDatabaseDetailedStats {
    /// Statistics for each database
    pub database_breakdown: Vec<ClickhouseDatabaseInfo>,
    /// Largest tables by size
    pub largest_tables: Vec<ClickhouseTableInfo>,
    /// Tables with high part counts
    pub fragmented_tables: Vec<ClickhouseFragmentedTable>,
    /// Recent table modifications
    pub recent_modifications: Vec<ClickhouseTableModification>,
}

impl MetadataCollection for ClickhouseDatabaseStats {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_DATABASE_OVERVIEW,
                query(
                    "SELECT
                    count(DISTINCT database) as total_databases,
                    count(DISTINCT concat(database, '.', table)) as total_tables,
                    sum(bytes_on_disk) as total_disk_usage,
                    sum(rows) as total_rows,
                    count() as total_parts,
                    count() as active_parts,
                    sum(bytes_on_disk) as total_compressed_size,
                    sum(data_uncompressed_bytes) as total_uncompressed_size,
                    (SELECT count() FROM (SELECT database, table FROM system.parts WHERE active = 1 GROUP BY database, table HAVING count() > 100)) as tables_needing_optimization
                FROM system.parts
                WHERE active = 1"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_TABLE_STATS,
                query(
                    "SELECT
                    (SELECT count() FROM system.tables WHERE engine = 'Memory') as temporary_tables,
                    (SELECT count() FROM system.detached_parts) as detached_parts
                "
                    .to_string(),
                ),
            ),
            (
                Self::QUERY_COMPRESSION_STATS,
                query(
                    "SELECT
                    avgIf(data_uncompressed_bytes / nullif(bytes_on_disk, 0), bytes_on_disk > 0) as avg_compression_ratio
                FROM system.parts
                WHERE active = 1 AND bytes_on_disk > 0"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse database and storage statistics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "database"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseDatabaseStats {
    const QUERY_DATABASE_OVERVIEW: &'static str = "database_overview";
    const QUERY_TABLE_STATS: &'static str = "table_stats";
    const QUERY_COMPRESSION_STATS: &'static str = "compression_stats";
    const HIGH_PART_COUNT_THRESHOLD: u64 = 100;
    const LARGE_TABLE_SIZE_THRESHOLD: u64 = 10_737_418_240; // 10GB
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

/// Database-level information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDatabaseInfo {
    /// Database name
    pub database: String,
    /// Number of tables in the database
    pub table_count: u64,
    /// Total size of the database in bytes
    pub total_size: u64,
    /// Total number of rows in the database
    pub total_rows: u64,
    /// Total number of parts in the database
    pub total_parts: u64,
    /// Average compression ratio for the database
    pub avg_compression_ratio: f64,
}

/// Table-level information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseTableInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Total size of the table in bytes
    pub total_size: u64,
    /// Total number of rows in the table
    pub total_rows: u64,
    /// Number of parts in the table
    pub part_count: u64,
    /// Last modification time
    pub last_modified: DateTimeWrapper,
    /// Table engine (MergeTree etc.)
    pub engine: String,
    /// Uncompressed data size
    pub uncompressed_size: u64,
    /// Compressed data size
    pub compressed_size: u64,
}

/// Information about tables with high part counts (fragmented)
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFragmentedTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of parts (high values indicate fragmentation)
    pub part_count: u64,
    /// Total size of the table
    pub total_size: u64,
    /// Number of partitions
    pub partition_count: u64,
    /// Last modification time
    pub last_modified: DateTimeWrapper,
}

/// Recent table modification information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseTableModification {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// When the table was last modified
    pub last_modified: DateTimeWrapper,
    /// Current size of the table
    pub current_size: u64,
    /// Number of parts created recently
    pub recent_parts: u64,
}

impl ClickhouseDatabaseStats {
    /// Checks if databases have high fragmentation
    pub fn has_fragmented_tables(&self) -> bool {
        self.tables_needing_optimization > 0
    }

    /// Checks if there are detached parts that need attention
    pub fn has_detached_parts(&self) -> bool {
        self.detached_parts > 0
    }

    /// Checks if compression ratio is below expected threshold
    pub fn has_poor_compression(&self, min_ratio: f64) -> bool {
        self.avg_compression_ratio < min_ratio
    }

    /// Gets total disk usage in GB
    pub fn get_disk_usage_gb(&self) -> f64 {
        self.total_disk_usage as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets compression efficiency as a percentage
    pub fn get_compression_efficiency(&self) -> f64 {
        if self.total_uncompressed_size == 0 {
            return 0.0;
        }
        ((self.total_uncompressed_size - self.total_compressed_size) as f64 / self.total_uncompressed_size as f64) * 100.0
    }

    /// Gets average rows per table
    pub fn get_avg_rows_per_table(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        self.total_rows as f64 / self.total_tables as f64
    }

    /// Gets average parts per table
    pub fn get_avg_parts_per_table(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        self.total_parts as f64 / self.total_tables as f64
    }

    /// Returns true if detailed stats were collected
    pub fn has_detailed_stats(&self) -> bool {
        self.detailed_stats.is_some()
    }

    /// Gets fragmentation ratio (active parts vs total parts)
    pub fn get_fragmentation_ratio(&self) -> f64 {
        if self.total_parts == 0 {
            return 0.0;
        }
        self.active_parts as f64 / self.total_parts as f64
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_database_stats() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let db_stats = ClickhouseDatabaseStats::default();
//
//         let result = db_stats
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
//         let stats = result.unwrap_or_default();
//
//         // Verify core metrics are collected
//         assert!(stats.total_databases >= 0);
//         assert!(stats.avg_compression_ratio >= 0.0);
//         assert!(stats.get_disk_usage_gb() >= 0.0);
//     }
//
//     #[test]
//     fn test_clickhouse_database_calculations() {
//         let mut db_stats = ClickhouseDatabaseStats::default();
//         db_stats.total_tables = 10;
//         db_stats.total_rows = 1_000_000;
//         db_stats.total_parts = 150;
//         db_stats.active_parts = 120;
//         db_stats.total_disk_usage = 5_368_709_120; // 5GB
//         db_stats.total_uncompressed_size = 10_737_418_240; // 10GB
//         db_stats.total_compressed_size = 5_368_709_120; // 5GB
//
//         assert_eq!(db_stats.get_avg_rows_per_table(), 100_000.0);
//         assert_eq!(db_stats.get_avg_parts_per_table(), 15.0);
//         assert_eq!(db_stats.get_disk_usage_gb(), 5.0);
//         assert_eq!(db_stats.get_compression_efficiency(), 50.0);
//         assert_eq!(db_stats.get_fragmentation_ratio(), 0.8);
//     }
// }
