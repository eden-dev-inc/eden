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

/// Clickhouse data parts information and storage metrics.
///
/// Covers part counts, sizes and fragmentation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartInfo {
    /// Total number of active parts across all tables
    pub total_active_parts: u64,
    /// Total number of inactive parts
    pub total_inactive_parts: u64,
    /// Total number of detached parts
    pub total_detached_parts: u64,
    /// Total disk space used by active parts in bytes
    pub total_disk_usage: u64,
    /// Total uncompressed data size in bytes
    pub total_uncompressed_size: u64,
    /// Total number of rows across all parts
    pub total_rows: u64,
    /// Average compression ratio across all parts
    pub avg_compression_ratio: f64,
    /// Number of tables with excessive parts (fragmentation)
    pub fragmented_tables: u64,
    /// Number of parts created in the last hour
    pub parts_created_last_hour: u64,
    /// Number of parts removed in the last hour
    pub parts_removed_last_hour: u64,
    /// Average part size in bytes
    pub avg_part_size: u64,
    /// Largest part size in bytes
    pub largest_part_size: u64,
    /// Smallest part size in bytes
    pub smallest_part_size: u64,
    /// Number of parts with poor compression
    pub poorly_compressed_parts: u64,
    /// Number of old parts (not accessed recently)
    pub old_parts: u64,
    /// Detailed metrics collected when problems are detected
    pub detailed_metrics: Option<ClickhousePartDetailedMetrics>,
}

/// Detailed part metrics collected when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartDetailedMetrics {
    /// Tables with excessive part counts
    pub highly_fragmented_tables: Vec<ClickhouseFragmentedTable>,
    /// Largest parts by size
    pub largest_parts: Vec<ClickhouseLargePart>,
    /// Parts with poor compression ratios
    pub poorly_compressed_parts_details: Vec<ClickhousePoorCompressionPart>,
    /// Recently created parts
    pub recent_parts: Vec<ClickhouseRecentPart>,
    /// Detached parts requiring attention
    pub detached_parts_details: Vec<ClickhouseDetachedPart>,
    /// Old parts that may need archival
    pub old_parts_details: Vec<ClickhouseOldPart>,
    /// Part size distribution analysis
    pub size_distribution: Vec<ClickhousePartSizeDistribution>,
    /// Partition-level analysis
    pub partition_analysis: Vec<ClickhousePartitionInfo>,
}

impl MetadataCollection for ClickhousePartInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (Self::QUERY_PART_OVERVIEW,
             query(
                 "SELECT
                    countIf(active = 1) as total_active_parts,
                    countIf(active = 0) as total_inactive_parts,
                    sumIf(bytes_on_disk, active = 1) as total_disk_usage,
                    sumIf(data_uncompressed_bytes, active = 1) as total_uncompressed_size,
                    sumIf(rows, active = 1) as total_rows,
                    avgIf(data_uncompressed_bytes / nullif(bytes_on_disk, 0), active = 1 AND bytes_on_disk > 0) as avg_compression_ratio,
                    avgIf(bytes_on_disk, active = 1) as avg_part_size,
                    maxIf(bytes_on_disk, active = 1) as largest_part_size,
                    minIf(bytes_on_disk, active = 1 AND bytes_on_disk > 0) as smallest_part_size
                FROM system.parts".to_string(),
)
            ),
            (Self::QUERY_FRAGMENTATION_STATS,
             query(
                 "SELECT
                    count(DISTINCT concat(database, '.', table)) as fragmented_tables
                FROM system.parts
                WHERE active = 1
                GROUP BY database, table
                HAVING count() > 100".to_string(),
)
            ),
            (Self::QUERY_PART_ACTIVITY,
             query(
                 "SELECT
                    countIf(modification_time >= now() - INTERVAL 1 HOUR) as parts_created_last_hour,
                    countIf(remove_time >= now() - INTERVAL 1 HOUR AND remove_time != toDateTime(0)) as parts_removed_last_hour
                FROM system.parts".to_string(),
)
            ),
            (Self::QUERY_DETACHED_PARTS_COUNT,
             query(
                 "SELECT count() as total_detached_parts FROM system.detached_parts".to_string(),
)
            ),
            (Self::QUERY_COMPRESSION_QUALITY,
             query(
                 "SELECT
                    countIf(data_uncompressed_bytes / nullif(bytes_on_disk, 0) < 2.0 AND active = 1 AND bytes_on_disk > 0) as poorly_compressed_parts,
                    countIf(modification_time < now() - INTERVAL 30 DAY AND active = 1) as old_parts
                FROM system.parts".to_string(),
)
            )
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse data parts and storage optimization metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "parts"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhousePartInfo {
    const QUERY_PART_OVERVIEW: &'static str = "part_overview";
    const QUERY_FRAGMENTATION_STATS: &'static str = "fragmentation_stats";
    const QUERY_PART_ACTIVITY: &'static str = "part_activity";
    const QUERY_DETACHED_PARTS_COUNT: &'static str = "detached_parts_count";
    const QUERY_COMPRESSION_QUALITY: &'static str = "compression_quality";
    const DETAIL_QUERY_FRAGMENTED_TABLES: &'static str = "fragmented_tables";
    const DETAIL_QUERY_LARGEST_PARTS: &'static str = "largest_parts";
    const DETAIL_QUERY_POOR_COMPRESSION: &'static str = "poor_compression";
    const DETAIL_QUERY_RECENT_PARTS: &'static str = "recent_parts";
    const DETAIL_QUERY_DETACHED_PARTS: &'static str = "detached_parts";
    const DETAIL_QUERY_OLD_PARTS: &'static str = "old_parts";
    const DETAIL_QUERY_SIZE_DISTRIBUTION: &'static str = "size_distribution";
    const DETAIL_QUERY_PARTITION_ANALYSIS: &'static str = "partition_analysis";
    const HIGH_PART_COUNT_THRESHOLD: u64 = 100;
    const LARGE_PART_SIZE_THRESHOLD: u64 = 10_737_418_240; // 10GB
    const POOR_COMPRESSION_THRESHOLD: f64 = 2.0; // Less than 2:1 compression
    const OLD_PART_THRESHOLD_DAYS: i64 = 30;
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 100;

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

    fn should_collect_detailed_metrics(core_info: &ClickhousePartInfo) -> bool {
        core_info.fragmented_tables > 0
            || core_info.largest_part_size > Self::LARGE_PART_SIZE_THRESHOLD
            || core_info.poorly_compressed_parts > 0
            || core_info.old_parts > 100
            || core_info.total_detached_parts > 0
            || core_info.parts_created_last_hour > 50
            || core_info.parts_removed_last_hour > 50
    }
}

/// Fragmented table with excessive parts
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFragmentedTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of parts (indicating fragmentation level)
    pub part_count: u64,
    /// Total size of all parts
    pub total_size: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Number of partitions
    pub partition_count: u64,
    /// Most recent part modification time
    pub last_modification: Option<DateTimeWrapper>,
    /// Earliest part modification time
    pub first_modification: Option<DateTimeWrapper>,
    /// Table engine
    pub engine: String,
    /// Average compression ratio across parts
    pub avg_compression_ratio: f64,
}

/// Large data part information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargePart {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Part name
    pub part_name: String,
    /// Partition identifier
    pub partition: String,
    /// Size on disk in bytes
    pub bytes_on_disk: u64,
    /// Uncompressed data size in bytes
    pub data_uncompressed_bytes: u64,
    /// Number of rows in the part
    pub rows: u64,
    /// When the part was created/modified
    pub modification_time: Option<DateTimeWrapper>,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Merge tree level
    pub level: u64,
    /// Whether this part was created by a mutation
    pub is_mutation: bool,
    /// Number of marks in the part
    pub marks_count: u64,
    /// Primary key size in memory
    pub primary_key_bytes_in_memory: u64,
}

/// Part with poor compression ratio
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePoorCompressionPart {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Part name
    pub part_name: String,
    /// Partition identifier
    pub partition: String,
    /// Size on disk in bytes
    pub bytes_on_disk: u64,
    /// Uncompressed data size in bytes
    pub data_uncompressed_bytes: u64,
    /// Poor compression ratio
    pub compression_ratio: f64,
    /// Number of rows
    pub rows: u64,
    /// Creation/modification time
    pub modification_time: Option<DateTimeWrapper>,
    /// Number of marks
    pub marks_count: u64,
    /// Merge tree level
    pub level: u64,
}

/// Recently created part information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseRecentPart {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Part name
    pub part_name: String,
    /// Partition identifier
    pub partition: String,
    /// Size on disk in bytes
    pub bytes_on_disk: u64,
    /// Number of rows
    pub rows: u64,
    /// Creation time
    pub modification_time: Option<DateTimeWrapper>,
    /// Merge tree level
    pub level: u64,
    /// Whether created by mutation
    pub is_mutation: bool,
    /// Compression ratio
    pub compression_ratio: f64,
}

/// Detached part information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDetachedPart {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Partition identifier
    pub partition_id: String,
    /// Part name
    pub part_name: String,
    /// Disk where the part is stored
    pub disk: String,
    /// Reason for detachment
    pub reason: Option<String>,
    /// Minimum block number
    pub min_block_number: u64,
    /// Maximum block number
    pub max_block_number: u64,
    /// Merge tree level
    pub level: u64,
}

/// Old part that may need archival
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseOldPart {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Part name
    pub part_name: String,
    /// Partition identifier
    pub partition: String,
    /// Size on disk in bytes
    pub bytes_on_disk: u64,
    /// Number of rows
    pub rows: u64,
    /// Last modification time
    pub modification_time: Option<DateTimeWrapper>,
    /// Age in seconds
    pub age_seconds: u64,
    /// Merge tree level
    pub level: u64,
    /// Number of marks
    pub marks_count: u64,
}

/// Part size distribution analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartSizeDistribution {
    /// Size category (e.g., "1-10MB", "100MB-1GB")
    pub size_category: String,
    /// Number of parts in this category
    pub part_count: u64,
    /// Total size of parts in this category
    pub total_size: u64,
    /// Total rows in this category
    pub total_rows: u64,
    /// Average compression ratio for this category
    pub avg_compression_ratio: f64,
}

/// Partition-level analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartitionInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Partition identifier
    pub partition: String,
    /// Number of parts in this partition
    pub part_count: u64,
    /// Total size of the partition
    pub total_size: u64,
    /// Total rows in the partition
    pub total_rows: u64,
    /// Most recent part in the partition
    pub latest_part_time: Option<DateTimeWrapper>,
    /// Earliest part in the partition
    pub earliest_part_time: Option<DateTimeWrapper>,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Total marks count
    pub total_marks: u64,
}

impl ClickhousePartInfo {
    /// Checks if there are tables with excessive fragmentation
    pub fn has_fragmented_tables(&self) -> bool {
        self.fragmented_tables > 0
    }

    /// Checks if there are parts with poor compression
    pub fn has_poor_compression_parts(&self) -> bool {
        self.poorly_compressed_parts > 0
    }

    /// Checks if there are detached parts requiring attention
    pub fn has_detached_parts(&self) -> bool {
        self.total_detached_parts > 0
    }

    /// Checks if there are old parts that may need archival
    pub fn has_old_parts(&self, threshold: u64) -> bool {
        self.old_parts > threshold
    }

    /// Checks if there's high part creation activity
    pub fn has_high_part_activity(&self, threshold: u64) -> bool {
        self.parts_created_last_hour > threshold
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets total disk usage in GB
    pub fn get_disk_usage_gb(&self) -> f64 {
        self.total_disk_usage as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets total disk usage in TB
    pub fn get_disk_usage_tb(&self) -> f64 {
        self.total_disk_usage as f64 / 1_099_511_627_776.0 // Convert bytes to TB
    }

    /// Gets compression efficiency as percentage
    pub fn get_compression_efficiency_percent(&self) -> f64 {
        if self.total_uncompressed_size == 0 {
            return 0.0;
        }
        let saved_bytes = self.total_uncompressed_size.saturating_sub(self.total_disk_usage);
        (saved_bytes as f64 / self.total_uncompressed_size as f64) * 100.0
    }

    /// Gets average part size in MB
    pub fn get_avg_part_size_mb(&self) -> f64 {
        self.avg_part_size as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets largest part size in GB
    pub fn get_largest_part_size_gb(&self) -> f64 {
        self.largest_part_size as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets part creation rate (parts per hour)
    pub fn get_part_creation_rate(&self) -> u64 {
        self.parts_created_last_hour
    }

    /// Gets part removal rate (parts per hour)
    pub fn get_part_removal_rate(&self) -> u64 {
        self.parts_removed_last_hour
    }

    /// Gets net part growth rate (created - removed per hour)
    pub fn get_net_part_growth_rate(&self) -> i64 {
        self.parts_created_last_hour as i64 - self.parts_removed_last_hour as i64
    }

    /// Gets fragmentation ratio (fragmented tables vs total expected tables)
    pub fn get_fragmentation_ratio(&self) -> f64 {
        if self.total_active_parts == 0 {
            return 0.0;
        }
        // Estimate total tables based on part distribution
        let estimated_tables = (self.total_active_parts / Self::HIGH_PART_COUNT_THRESHOLD).max(1);
        self.fragmented_tables as f64 / estimated_tables as f64
    }

    /// Gets storage efficiency score (0.0 to 1.0)
    pub fn get_storage_efficiency_score(&self) -> f64 {
        let mut score = 1.0;

        // Penalize poor compression
        if self.avg_compression_ratio < 2.0 {
            score -= 0.3;
        } else if self.avg_compression_ratio < 3.0 {
            score -= 0.1;
        }

        // Penalize fragmentation
        if self.fragmented_tables > 0 {
            let fragmentation_penalty = (self.fragmented_tables as f64 / 10.0).min(0.4);
            score -= fragmentation_penalty;
        }

        // Penalize detached parts
        if self.total_detached_parts > 0 {
            let detached_penalty = (self.total_detached_parts as f64 / 100.0).min(0.2);
            score -= detached_penalty;
        }

        // Penalize poor compression parts
        if self.poorly_compressed_parts > 0 {
            let poor_compression_penalty = (self.poorly_compressed_parts as f64 / self.total_active_parts as f64 * 0.3).min(0.2);
            score -= poor_compression_penalty;
        }

        score.max(0.0)
    }

    /// Gets part health status
    pub fn get_part_health_status(&self) -> PartHealthStatus {
        let efficiency_score = self.get_storage_efficiency_score();
        let has_detached = self.total_detached_parts > 0;
        let high_fragmentation = self.fragmented_tables > 10;
        let poor_compression_ratio = self.poorly_compressed_parts as f64 / self.total_active_parts as f64;

        if efficiency_score < 0.5 || (has_detached && high_fragmentation) {
            PartHealthStatus::Critical
        } else if efficiency_score < 0.7 || has_detached || high_fragmentation || poor_compression_ratio > 0.2 {
            PartHealthStatus::Warning
        } else if efficiency_score < 0.85 || poor_compression_ratio > 0.1 {
            PartHealthStatus::Attention
        } else {
            PartHealthStatus::Healthy
        }
    }

    /// Gets part activity level
    pub fn get_part_activity_level(&self) -> PartActivityLevel {
        let total_activity = self.parts_created_last_hour + self.parts_removed_last_hour;

        if total_activity == 0 {
            PartActivityLevel::Idle
        } else if total_activity <= 10 {
            PartActivityLevel::Low
        } else if total_activity <= 50 {
            PartActivityLevel::Moderate
        } else if total_activity <= 200 {
            PartActivityLevel::High
        } else {
            PartActivityLevel::VeryHigh
        }
    }

    /// Gets average rows per part
    pub fn get_avg_rows_per_part(&self) -> f64 {
        if self.total_active_parts == 0 {
            return 0.0;
        }
        self.total_rows as f64 / self.total_active_parts as f64
    }

    /// Gets total number of parts (active + inactive)
    pub fn get_total_parts(&self) -> u64 {
        self.total_active_parts + self.total_inactive_parts
    }

    /// Gets active parts ratio
    pub fn get_active_parts_ratio(&self) -> f64 {
        let total = self.get_total_parts();
        if total == 0 {
            return 0.0;
        }
        self.total_active_parts as f64 / total as f64
    }
}

/// Part health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum PartHealthStatus {
    /// Parts are well-optimized and healthy
    Healthy,
    /// Minor issues that should be monitored
    Attention,
    /// Issues that require investigation
    Warning,
    /// Critical issues requiring immediate attention
    Critical,
}

/// Part activity level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum PartActivityLevel {
    /// No part activity
    Idle,
    /// Low part activity (1-10 operations/hour)
    Low,
    /// Moderate part activity (11-50 operations/hour)
    Moderate,
    /// High part activity (51-200 operations/hour)
    High,
    /// Very high part activity (200+ operations/hour)
    VeryHigh,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_part_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let part_info = ClickhousePartInfo::default();
//
//         let result = part_info
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
//         assert!(info.get_disk_usage_gb() >= 0.0);
//         assert!(info.get_compression_efficiency_percent() >= 0.0);
//         assert!(info.get_storage_efficiency_score() >= 0.0);
//         assert!(info.get_storage_efficiency_score() <= 1.0);
//     }
//
//     #[test]
//     fn test_clickhouse_part_calculations() {
//         let mut part_info = ClickhousePartInfo::default();
//         part_info.total_active_parts = 1000;
//         part_info.total_inactive_parts = 200;
//         part_info.total_disk_usage = 5_368_709_120; // 5GB
//         part_info.total_uncompressed_size = 10_737_418_240; // 10GB
//         part_info.total_rows = 10_000_000;
//         part_info.avg_compression_ratio = 2.0;
//         part_info.fragmented_tables = 5;
//         part_info.parts_created_last_hour = 25;
//         part_info.parts_removed_last_hour = 15;
//         part_info.poorly_compressed_parts = 50;
//         part_info.total_detached_parts = 3;
//
//         assert_eq!(part_info.get_disk_usage_gb(), 5.0);
//         assert_eq!(part_info.get_compression_efficiency_percent(), 50.0);
//         assert_eq!(part_info.get_part_creation_rate(), 25);
//         assert_eq!(part_info.get_part_removal_rate(), 15);
//         assert_eq!(part_info.get_net_part_growth_rate(), 10);
//         assert_eq!(part_info.get_avg_rows_per_part(), 10_000.0);
//         assert_eq!(part_info.get_total_parts(), 1200);
//         assert!(part_info.get_active_parts_ratio() > 0.83);
//
//         assert!(part_info.has_fragmented_tables());
//         assert!(part_info.has_poor_compression_parts());
//         assert!(part_info.has_detached_parts());
//
//         let activity_level = part_info.get_part_activity_level();
//         assert!(matches!(activity_level, PartActivityLevel::Moderate));
//
//         let health_status = part_info.get_part_health_status();
//         // Should be Warning due to fragmentation and poor compression
//         assert!(matches!(health_status, PartHealthStatus::Warning));
//
//         let efficiency_score = part_info.get_storage_efficiency_score();
//         assert!(efficiency_score > 0.0 && efficiency_score < 1.0);
//     }
//
//     #[test]
//     fn test_part_health_classification() {
//         // Test healthy status
//         let mut healthy_parts = ClickhousePartInfo::default();
//         healthy_parts.total_active_parts = 500;
//         healthy_parts.avg_compression_ratio = 4.0;
//         healthy_parts.fragmented_tables = 0;
//         healthy_parts.total_detached_parts = 0;
//         healthy_parts.poorly_compressed_parts = 5; // 1% of parts
//
//         assert!(matches!(healthy_parts.get_part_health_status(), PartHealthStatus::Healthy));
//
//         // Test critical status
//         let mut critical_parts = ClickhousePartInfo::default();
//         critical_parts.total_active_parts = 1000;
//         critical_parts.avg_compression_ratio = 1.5; // Poor compression
//         critical_parts.fragmented_tables = 20; // High fragmentation
//         critical_parts.total_detached_parts = 50;
//         critical_parts.poorly_compressed_parts = 300; // 30% of parts
//
//         assert!(matches!(critical_parts.get_part_health_status(), PartHealthStatus::Critical));
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhousePartInfo;

    #[test]
    fn parts_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhousePartInfo::default();
        assert!(!ClickhousePartInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn parts_detailed_gate_true_for_detached_parts() {
        let info = ClickhousePartInfo { total_detached_parts: 1, ..ClickhousePartInfo::default() };
        assert!(ClickhousePartInfo::should_collect_detailed_metrics(&info));
    }
}
