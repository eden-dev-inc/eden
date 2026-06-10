use super::*;

impl MetadataCollection for ClickhouseStorageInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_STORAGE_OVERVIEW,
                query(
                    "SELECT
                    sum(total_bytes) as total_disk_usage,
                    count() as total_tables,
                    sum(total_rows) as total_rows,
                    count(DISTINCT database) as total_databases,
                    sum(total_bytes_uncompressed) as total_uncompressed_size,
                    sum(total_bytes) as total_compressed_size,
                    avg(total_bytes / nullIf(total_bytes_uncompressed, 0)) as avg_compression_ratio
                FROM system.tables
                WHERE engine NOT IN ('View', 'MaterializedView', 'Dictionary')"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_PARTS_OVERVIEW,
                query(
                    "SELECT
                    count() as total_parts,
                    countIf(active = 1) as active_parts,
                    countIf(active = 0) as inactive_parts,
                    sum(bytes_on_disk) as total_parts_size,
                    sum(rows) as total_parts_rows
                FROM system.parts"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_TABLE_SIZES,
                query(
                    "SELECT
                    avg(total_bytes) as avg_table_size,
                    max(total_bytes) as largest_table_size,
                    countIf(total_bytes > 10737418240) as large_tables_count,
                    countIf(total_bytes / nullIf(total_bytes_uncompressed, 0) < 0.1) as poorly_compressed_count
                FROM system.tables
                WHERE engine NOT IN ('View', 'MaterializedView', 'Dictionary')
                    AND total_bytes > 0"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_MERGE_OPERATIONS,
                query(
                    "SELECT
                    count() as active_merges,
                    sum(bytes_read_uncompressed) as merge_bytes_processed,
                    sum(rows_read) as merge_rows_processed,
                    avg(elapsed) as avg_merge_time
                FROM system.merges"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_FRAGMENTATION_CHECK,
                query(
                    "SELECT
                    count(DISTINCT concat(database, '.', table)) as fragmented_tables
                FROM system.parts
                WHERE active = 1
                GROUP BY database, table
                HAVING count() > 100"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_PARTITION_STATS,
                query(
                    "SELECT
                    count(DISTINCT partition) as total_partitions,
                    count(DISTINCT concat(database, '.', table)) as partitioned_tables
                FROM system.parts
                WHERE active = 1"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse storage and data management metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "storage"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseStorageInfo {
    pub(super) const LARGE_TABLE_THRESHOLD: u64 = 10_737_418_240; // 10GB
    pub(super) const POOR_COMPRESSION_THRESHOLD: f64 = 0.1; // 10% compression ratio
    pub(super) const HIGH_FRAGMENTATION_THRESHOLD: u64 = 100; // More than 100 parts per table
    pub(super) const LARGE_PARTITION_THRESHOLD: u64 = 1000; // More than 1000 partitions
    pub(super) const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    pub(super) const MAX_DETAILED_RESULTS: usize = 100;
    pub(super) const QUERY_STORAGE_OVERVIEW: &'static str = "storage_overview";
    pub(super) const QUERY_PARTS_OVERVIEW: &'static str = "parts_overview";
    pub(super) const QUERY_TABLE_SIZES: &'static str = "table_sizes";
    pub(super) const QUERY_MERGE_OPERATIONS: &'static str = "merge_operations";
    pub(super) const QUERY_FRAGMENTATION_CHECK: &'static str = "fragmentation_check";
    pub(super) const QUERY_PARTITION_STATS: &'static str = "partition_stats";
    pub(super) const DETAIL_QUERY_LARGE_TABLES: &'static str = "large_tables";
    pub(super) const DETAIL_QUERY_COMPRESSION_TABLES: &'static str = "compression_tables";
    pub(super) const DETAIL_QUERY_FRAGMENTED_TABLES: &'static str = "fragmented_tables";
    pub(super) const DETAIL_QUERY_ACTIVE_MERGES: &'static str = "active_merges";
    pub(super) const DETAIL_QUERY_DATABASE_STATS: &'static str = "database_stats";
    pub(super) const DETAIL_QUERY_PARTITION_INFO: &'static str = "partition_info";

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

    pub(super) fn should_collect_detailed_metrics(core_info: &ClickhouseStorageInfo) -> bool {
        core_info.largest_table_size > Self::LARGE_TABLE_THRESHOLD
            || core_info.poorly_compressed_tables > 0
            || core_info.fragmented_tables > 0
            || core_info.active_merges > 0
            || core_info.total_partitions > Self::LARGE_PARTITION_THRESHOLD
            || core_info.tables_needing_optimization > 0
            || core_info.total_disk_usage > 1_099_511_627_776
    }

    pub(super) fn calculate_estimated_completion(progress: f64, elapsed_seconds: f64) -> Option<f64> {
        if progress > 0.0 && progress < 1.0 {
            Some((elapsed_seconds / progress) - elapsed_seconds)
        } else {
            None
        }
    }
}
