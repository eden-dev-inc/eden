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

/// Clickhouse merge operations and background process information.
///
/// Covers active/pending merges, throughput and per-table merge stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMergeInfo {
    /// Number of currently running merges
    pub running_merges: u64,
    /// Number of merges in the queue waiting to execute
    pub queued_merges: u64,
    /// Number of currently running mutations
    pub running_mutations: u64,
    /// Number of mutations in the queue
    pub queued_mutations: u64,
    /// Duration of the longest running merge in seconds
    pub longest_merge_duration: f64,
    /// Average merge duration across all running merges
    pub avg_running_merge_duration: f64,
    /// Total bytes being processed by current merges
    pub merge_bytes_in_progress: u64,
    /// Total rows being processed by current merges
    pub merge_rows_in_progress: u64,
    /// Number of parts being merged currently
    pub parts_being_merged: u64,
    /// Estimated time to complete all running merges in seconds
    pub estimated_completion_time: f64,
    /// Number of merges that failed in the last hour
    pub failed_merges_last_hour: u64,
    /// Number of mutations that failed in the last hour
    pub failed_mutations_last_hour: u64,
    /// Average merge throughput in bytes per second
    pub avg_merge_throughput: f64,
    /// Number of background cleanup operations running
    pub background_cleanup_operations: u64,
    /// Number of tables with excessive parts (needing merges)
    pub tables_needing_merges: u64,
    /// Detailed metrics collected when problems are detected
    pub detailed_metrics: Option<ClickhouseMergeDetailedMetrics>,
}

/// Detailed merge metrics collected when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMergeDetailedMetrics {
    /// Long-running merge operations
    pub long_running_merges: Vec<ClickhouseLongMerge>,
    /// Failed merge operations with details
    pub failed_merge_details: Vec<ClickhouseFailedMerge>,
    /// Large merge operations by size
    pub large_merge_operations: Vec<ClickhouseLargeMerge>,
    /// Mutation operation details
    pub mutation_operations: Vec<ClickhouseMutationInfo>,
    /// Tables with fragmentation issues
    pub fragmented_tables: Vec<ClickhouseFragmentedTableMerge>,
    /// Merge queue analysis
    pub merge_queue_analysis: Vec<ClickhouseMergeQueueInfo>,
    /// Background process breakdown
    pub background_process_breakdown: Vec<ClickhouseBackgroundProcess>,
}

impl MetadataCollection for ClickhouseMergeInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (Self::QUERY_MERGE_OVERVIEW,
             query(
                 "SELECT
                    count() as running_merges,
                    sum(total_size_bytes_compressed) as merge_bytes_in_progress,
                    sum(total_size_marks) as merge_rows_in_progress,
                    sum(num_parts) as parts_being_merged,
                    max(elapsed) as longest_merge_duration,
                    avg(elapsed) as avg_running_merge_duration,
                    sum(total_size_bytes_compressed) / nullif(sum(elapsed), 0) as avg_merge_throughput
                FROM system.merges".to_string())
            ),
            (Self::QUERY_MUTATION_OVERVIEW,
             query(
                 "SELECT
                    countIf(is_done = 0) as running_mutations,
                    countIf(is_done = 0 AND latest_failed_part != '') as queued_mutations
                FROM system.mutations".to_string())
            ),
            (Self::QUERY_MERGE_QUEUE_STATS,
             query(
                 "SELECT
                    (SELECT sum(merges_in_queue) FROM system.replicas) as queued_merges,
                    (SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask') as background_cleanup_operations
                ".to_string())
            ),
            (Self::QUERY_FRAGMENTATION_STATS,
             query(
                 "SELECT
                    count(DISTINCT concat(database, '.', table)) as tables_needing_merges
                FROM system.parts
                WHERE active = 1
                GROUP BY database, table
                HAVING count() > 100".to_string())
            ),
            (Self::QUERY_RECENT_FAILURES,
             query(
                 "SELECT
                    countIf(event_time >= now() - INTERVAL 1 HOUR AND exception != '' AND query LIKE '%OPTIMIZE%') as failed_merges_last_hour,
                    countIf(event_time >= now() - INTERVAL 1 HOUR AND exception != '' AND query LIKE '%ALTER%MUTATION%') as failed_mutations_last_hour
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 HOUR".to_string())
            )
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse merge operations and background process metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "merge"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseMergeInfo {
    const QUERY_MERGE_OVERVIEW: &'static str = "merge_overview";
    const QUERY_MUTATION_OVERVIEW: &'static str = "mutation_overview";
    const QUERY_MERGE_QUEUE_STATS: &'static str = "merge_queue_stats";
    const QUERY_FRAGMENTATION_STATS: &'static str = "fragmentation_stats";
    const QUERY_RECENT_FAILURES: &'static str = "recent_failures";
    const DETAIL_QUERY_LONG_RUNNING_MERGES: &'static str = "long_running_merges";
    const DETAIL_QUERY_LARGE_MERGES: &'static str = "large_merges";
    const DETAIL_QUERY_MUTATIONS: &'static str = "mutations";
    const DETAIL_QUERY_FRAGMENTED_TABLES: &'static str = "fragmented_tables";
    const DETAIL_QUERY_QUEUE_ANALYSIS: &'static str = "queue_analysis";
    const DETAIL_QUERY_BACKGROUND_PROCESSES: &'static str = "background_processes";
    const LONG_MERGE_THRESHOLD: f64 = 300.0; // 5 minutes
    const LARGE_MERGE_THRESHOLD: u64 = 1_073_741_824; // 1GB
    const HIGH_PART_COUNT_THRESHOLD: u64 = 100;
    const QUERY_TIMEOUT: Duration = Duration::from_secs(8);
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

    pub(super) fn calculate_estimated_completion(info: &ClickhouseMergeInfo) -> f64 {
        if info.avg_merge_throughput <= 0.0 || info.merge_bytes_in_progress == 0 {
            return 0.0;
        }
        info.merge_bytes_in_progress as f64 / info.avg_merge_throughput
    }

    fn should_collect_detailed_metrics(core_info: &ClickhouseMergeInfo) -> bool {
        core_info.longest_merge_duration > Self::LONG_MERGE_THRESHOLD
            || core_info.merge_bytes_in_progress > Self::LARGE_MERGE_THRESHOLD
            || core_info.failed_merges_last_hour > 0
            || core_info.failed_mutations_last_hour > 0
            || core_info.tables_needing_merges > 0
            || core_info.queued_merges > 10
            || core_info.queued_mutations > 5
    }
}

/// Long-running merge operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLongMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Duration the merge has been running in seconds
    pub elapsed: f64,
    /// Progress of the merge (0.0 to 1.0)
    pub progress: f64,
    /// Total bytes being processed
    pub total_size_bytes: u64,
    /// Total marks being processed
    pub total_size_marks: u64,
    /// Number of parts being merged
    pub num_parts: u64,
    /// Name of the resulting part
    pub result_part_name: Option<String>,
    /// Type of merge operation
    pub merge_type: String,
    /// Algorithm used for merging
    pub merge_algorithm: String,
    /// First source part name
    pub first_source_part: Option<String>,
    /// Whether this is a mutation operation
    pub is_mutation: bool,
}

/// Large merge operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargeMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Total bytes being processed
    pub total_size_bytes: u64,
    /// Total marks being processed
    pub total_size_marks: u64,
    /// Number of parts being merged
    pub num_parts: u64,
    /// Duration elapsed so far
    pub elapsed: f64,
    /// Progress of the merge
    pub progress: f64,
    /// Type of merge operation
    pub merge_type: String,
    /// Algorithm used for merging
    pub merge_algorithm: String,
    /// Name of the resulting part
    pub result_part_name: Option<String>,
    /// Whether this is a mutation operation
    pub is_mutation: bool,
}

/// Failed merge operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// When the merge failed
    pub failure_time: DateTimeWrapper,
    /// Exception message
    pub exception: String,
    /// Duration before failure
    pub duration_before_failure: f64,
    /// Size of data that was being merged
    pub attempted_size_bytes: u64,
    /// Number of parts that were being merged
    pub attempted_parts: u64,
    /// Type of merge that failed
    pub merge_type: String,
    /// Part names that were being merged
    pub source_parts: Option<String>,
}

/// Mutation operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMutationInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Unique mutation identifier
    pub mutation_id: String,
    /// Mutation command
    pub command: String,
    /// When the mutation was created
    pub create_time: DateTimeWrapper,
    /// Block number for the mutation
    pub block_number: u64,
    /// Parts that still need to be mutated
    pub parts_to_do: Option<String>,
    /// Whether the mutation is complete
    pub is_done: bool,
    /// Latest failed part name
    pub latest_failed_part: Option<String>,
    /// When the latest failure occurred
    pub latest_fail_time: Option<DateTimeWrapper>,
    /// Reason for the latest failure
    pub latest_fail_reason: Option<String>,
}

/// Fragmented table requiring merges
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFragmentedTableMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of parts (high indicates fragmentation)
    pub part_count: u64,
    /// Total size of all parts
    pub total_size: u64,
    /// Number of partitions
    pub partition_count: u64,
    /// Last modification time
    pub last_modified: DateTimeWrapper,
    /// Table engine
    pub engine: String,
}

/// Merge queue analysis information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMergeQueueInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Type of operation (MERGE_PARTS, MUTATE_PART etc.)
    pub operation_type: String,
    /// When the operation was queued
    pub create_time: DateTimeWrapper,
    /// Required quorum for the operation
    pub required_quorum: u64,
    /// Source replica for the operation
    pub source_replica: Option<String>,
    /// Name of the new part being created
    pub new_part_name: Option<String>,
    /// Parts being merged
    pub parts_to_merge: Option<String>,
    /// Whether currently executing
    pub is_currently_executing: bool,
    /// Number of attempts made
    pub num_tries: u64,
    /// Last attempt time
    pub last_attempt_time: Option<DateTimeWrapper>,
    /// Last exception encountered
    pub last_exception: Option<String>,
    /// Reason for postponement
    pub postpone_reason: Option<String>,
}

/// Background process information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseBackgroundProcess {
    /// Task name
    pub task_name: String,
    /// Process type
    pub process_type: String,
    /// When the task is scheduled
    pub schedule_time: Option<DateTimeWrapper>,
    /// Last execution time
    pub last_execution_time: Option<DateTimeWrapper>,
    /// Exception if any
    pub exception: Option<String>,
}

impl ClickhouseMergeInfo {
    /// Checks if there are long-running merges
    pub fn has_long_running_merges(&self, threshold_seconds: f64) -> bool {
        self.longest_merge_duration > threshold_seconds
    }

    /// Checks if there are large merge operations
    pub fn has_large_merge_operations(&self, threshold_bytes: u64) -> bool {
        self.merge_bytes_in_progress > threshold_bytes
    }

    /// Checks if there are recent merge failures
    pub fn has_recent_merge_failures(&self) -> bool {
        self.failed_merges_last_hour > 0
    }

    /// Checks if there are recent mutation failures
    pub fn has_recent_mutation_failures(&self) -> bool {
        self.failed_mutations_last_hour > 0
    }

    /// Checks if there are tables needing optimization
    pub fn has_fragmented_tables(&self) -> bool {
        self.tables_needing_merges > 0
    }

    /// Checks if there's a significant merge queue backlog
    pub fn has_merge_queue_backlog(&self, threshold: u64) -> bool {
        self.queued_merges > threshold
    }

    /// Checks if there's a significant mutation queue backlog
    pub fn has_mutation_queue_backlog(&self, threshold: u64) -> bool {
        self.queued_mutations > threshold
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets merge throughput in MB/s
    pub fn get_merge_throughput_mb_per_sec(&self) -> f64 {
        self.avg_merge_throughput / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets total bytes being processed in GB
    pub fn get_merge_bytes_gb(&self) -> f64 {
        self.merge_bytes_in_progress as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets estimated completion time in minutes
    pub fn get_estimated_completion_minutes(&self) -> f64 {
        self.estimated_completion_time / 60.0
    }

    /// Gets merge efficiency (parts per second being processed)
    pub fn get_merge_efficiency(&self) -> f64 {
        if self.avg_running_merge_duration <= 0.0 {
            return 0.0;
        }
        self.parts_being_merged as f64 / self.avg_running_merge_duration
    }

    /// Gets overall merge activity level
    pub fn get_merge_activity_level(&self) -> MergeActivityLevel {
        let total_operations = self.running_merges + self.running_mutations + self.queued_merges + self.queued_mutations;

        if total_operations == 0 {
            MergeActivityLevel::Idle
        } else if total_operations <= 5 {
            MergeActivityLevel::Low
        } else if total_operations <= 20 {
            MergeActivityLevel::Moderate
        } else if total_operations <= 50 {
            MergeActivityLevel::High
        } else {
            MergeActivityLevel::VeryHigh
        }
    }

    /// Gets merge health status
    pub fn get_merge_health_status(&self) -> MergeHealthStatus {
        let has_failures = self.failed_merges_last_hour > 0 || self.failed_mutations_last_hour > 0;
        let has_long_operations = self.longest_merge_duration > 600.0; // 10 minutes
        let has_excessive_fragmentation = self.tables_needing_merges > 10;
        let has_large_queue = self.queued_merges > 50 || self.queued_mutations > 20;

        if has_failures && (has_long_operations || has_large_queue) {
            MergeHealthStatus::Critical
        } else if has_failures || has_long_operations || has_excessive_fragmentation {
            MergeHealthStatus::Warning
        } else if has_large_queue {
            MergeHealthStatus::Attention
        } else {
            MergeHealthStatus::Healthy
        }
    }

    /// Gets fragmentation ratio (tables needing merges vs total activity)
    pub fn get_fragmentation_ratio(&self) -> f64 {
        let total_activity = self.running_merges + self.queued_merges + self.tables_needing_merges;
        if total_activity == 0 {
            return 0.0;
        }
        self.tables_needing_merges as f64 / total_activity as f64
    }

    /// Gets merge queue pressure (queued vs running ratio)
    pub fn get_merge_queue_pressure(&self) -> f64 {
        if self.running_merges == 0 {
            return if self.queued_merges > 0 { f64::INFINITY } else { 0.0 };
        }
        self.queued_merges as f64 / self.running_merges as f64
    }

    /// Gets mutation queue pressure
    pub fn get_mutation_queue_pressure(&self) -> f64 {
        if self.running_mutations == 0 {
            return if self.queued_mutations > 0 { f64::INFINITY } else { 0.0 };
        }
        self.queued_mutations as f64 / self.running_mutations as f64
    }
}

/// Merge activity level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MergeActivityLevel {
    /// No merge operations running
    Idle,
    /// Low merge activity (1-5 operations)
    Low,
    /// Moderate merge activity (6-20 operations)
    Moderate,
    /// High merge activity (21-50 operations)
    High,
    /// Very high merge activity (50+ operations)
    VeryHigh,
}

/// Merge health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MergeHealthStatus {
    /// All merge operations are healthy
    Healthy,
    /// Minor issues that should be monitored
    Attention,
    /// Issues that require investigation
    Warning,
    /// Critical issues requiring immediate attention
    Critical,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_merge_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let merge_info = ClickhouseMergeInfo::default();
//
//         let result = merge_info
//             .sync_metadata(
//                 clickhouse_ep
//                     .0
//                     .read_conn_async(&endpoint_cache_uuid, telemetry_wrapper)
//                     .await
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
//         assert!(info.get_merge_throughput_mb_per_sec() >= 0.0);
//         assert!(info.get_estimated_completion_minutes() >= 0.0);
//     }
//
//     #[test]
//     fn test_clickhouse_merge_calculations() {
//         let mut merge_info = ClickhouseMergeInfo::default();
//         merge_info.running_merges = 5;
//         merge_info.queued_merges = 15;
//         merge_info.running_mutations = 2;
//         merge_info.queued_mutations = 8;
//         merge_info.longest_merge_duration = 420.0; // 7 minutes
//         merge_info.merge_bytes_in_progress = 5_368_709_120; // 5GB
//         merge_info.avg_merge_throughput = 104_857_600.0; // 100MB/s
//         merge_info.tables_needing_merges = 12;
//         merge_info.failed_merges_last_hour = 2;
//
//         assert!(merge_info.has_long_running_merges(300.0)); // 5 minutes threshold
//         assert!(merge_info.has_large_merge_operations(1_073_741_824)); // 1GB threshold
//         assert!(merge_info.has_recent_merge_failures());
//         assert!(merge_info.has_fragmented_tables());
//         assert!(merge_info.has_merge_queue_backlog(10));
//
//         assert_eq!(merge_info.get_merge_throughput_mb_per_sec(), 100.0);
//         assert_eq!(merge_info.get_merge_bytes_gb(), 5.0);
//         assert_eq!(merge_info.get_estimated_completion_minutes(), 0.85); // ~51 seconds
//         assert_eq!(merge_info.get_merge_queue_pressure(), 3.0);
//         assert_eq!(merge_info.get_mutation_queue_pressure(), 4.0);
//
//         let activity_level = merge_info.get_merge_activity_level();
//         assert!(matches!(activity_level, MergeActivityLevel::High));
//
//         let health_status = merge_info.get_merge_health_status();
//         assert!(matches!(health_status, MergeHealthStatus::Warning));
//     }
//
//     #[test]
//     fn test_merge_health_classification() {
//         // Test healthy status
//         let mut healthy_merge = ClickhouseMergeInfo::default();
//         healthy_merge.running_merges = 3;
//         healthy_merge.queued_merges = 5;
//         healthy_merge.longest_merge_duration = 120.0; // 2 minutes
//         healthy_merge.failed_merges_last_hour = 0;
//         healthy_merge.tables_needing_merges = 2;
//
//         assert!(matches!(healthy_merge.get_merge_health_status(), MergeHealthStatus::Healthy));
//
//         // Test critical status
//         let mut critical_merge = ClickhouseMergeInfo::default();
//         critical_merge.running_merges = 10;
//         critical_merge.queued_merges = 100;
//         critical_merge.longest_merge_duration = 1200.0; // 20 minutes
//         critical_merge.failed_merges_last_hour = 5;
//         critical_merge.failed_mutations_last_hour = 3;
//
//         assert!(matches!(critical_merge.get_merge_health_status(), MergeHealthStatus::Critical));
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhouseMergeInfo;

    #[test]
    fn merges_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhouseMergeInfo::default();
        assert!(!ClickhouseMergeInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn merges_detailed_gate_true_for_failures() {
        let info = ClickhouseMergeInfo { failed_merges_last_hour: 1, ..ClickhouseMergeInfo::default() };
        assert!(ClickhouseMergeInfo::should_collect_detailed_metrics(&info));
    }
}
