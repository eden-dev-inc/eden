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

/// Clickhouse mutation operations and ALTER table command information.
///
/// Covers mutation progress and schema change operations.
/// Merge operations live in `ClickhouseMergeInfo`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMutationInfo {
    /// Total number of mutations across all tables
    pub total_mutations: u64,
    /// Number of mutations currently in progress
    pub active_mutations: u64,
    /// Number of completed mutations
    pub completed_mutations: u64,
    /// Number of failed mutations
    pub failed_mutations: u64,
    /// Number of mutations waiting to start
    pub waiting_mutations: u64,
    /// Average mutation completion time in seconds
    pub avg_completion_time: f64,
    /// Duration of the longest running mutation in seconds
    pub longest_mutation_duration: f64,
    /// Total number of parts affected by all active mutations
    pub total_parts_to_mutate: u64,
    /// Total number of parts already mutated
    pub total_parts_mutated: u64,
    /// Number of mutations that failed in the last 24 hours
    pub failed_mutations_last_24h: u64,
    /// Number of mutations completed in the last hour
    pub completed_mutations_last_hour: u64,
    /// Average progress across all active mutations (0.0 to 1.0)
    pub avg_mutation_progress: f64,
    /// Number of tables currently undergoing mutations
    pub tables_with_active_mutations: u64,
    /// Number of mutations stuck (no progress for extended time)
    pub stuck_mutations: u64,
    /// Detailed metrics collected when problems are detected
    pub detailed_metrics: Option<ClickhouseMutationDetailedMetrics>,
}

/// Detailed mutation metrics collected when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMutationDetailedMetrics {
    /// Long-running mutation operations
    pub long_running_mutations: Vec<ClickhouseLongMutation>,
    /// Failed mutation operations with error details
    pub failed_mutation_details: Vec<ClickhouseFailedMutation>,
    /// Stuck mutations with no recent progress
    pub stuck_mutation_details: Vec<ClickhouseStuckMutation>,
    /// Large mutation operations by parts count
    pub large_mutation_operations: Vec<ClickhouseLargeMutation>,
    /// Recent mutation completions
    pub recent_completions: Vec<ClickhouseMutationCompletion>,
    /// Mutation command type breakdown
    pub command_type_breakdown: Vec<ClickhouseMutationCommandStats>,
    /// Tables with multiple concurrent mutations
    pub tables_with_multiple_mutations: Vec<ClickhouseTableMutationInfo>,
}

impl MetadataCollection for ClickhouseMutationInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (Self::QUERY_MUTATION_OVERVIEW,
             query(
                 "SELECT
                    count() as total_mutations,
                    countIf(is_done = 0) as active_mutations,
                    countIf(is_done = 1) as completed_mutations,
                    countIf(latest_failed_part != '') as failed_mutations,
                    countIf(is_done = 0 AND parts_to_do > 0) as waiting_mutations,
                    0 as avg_completion_time,
                    count(DISTINCT concat(database, '.', table)) as tables_with_mutations
                FROM system.mutations".to_string(),
)
            ),
            (Self::QUERY_MUTATION_PROGRESS,
             query(
                 "SELECT
                    sum(parts_to_do) as total_parts_to_mutate,
                    sum(length(parts_to_do_names)) as total_parts_mutated,
                    toFloat64(maxIf(now() - create_time, is_done = 0)) as longest_mutation_duration,
                    ifNull(avgIf(toFloat64(length(parts_to_do_names)) / nullif(toFloat64(parts_to_do) + toFloat64(length(parts_to_do_names)), 0), is_done = 0), 0) as avg_mutation_progress,
                    count(DISTINCT concat(database, '.', table)) as tables_with_active_mutations
                FROM system.mutations
                WHERE is_done = 0".to_string(),
)
            ),
            (Self::QUERY_RECENT_MUTATION_ACTIVITY,
             query(
                 "SELECT
                    countIf(latest_fail_time >= now() - INTERVAL 24 HOUR AND latest_failed_part != '') as failed_mutations_last_24h,
                    countIf(is_done = 1 AND create_time >= now() - INTERVAL 1 HOUR) as completed_mutations_last_hour
                FROM system.mutations".to_string(),
)
            ),
            (Self::QUERY_STUCK_MUTATIONS,
             query(
                 "SELECT
                    countIf(is_done = 0 AND create_time < now() - INTERVAL 6 HOUR AND parts_to_do > 0) as stuck_mutations
                FROM system.mutations".to_string(),
)
            )
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse mutation operations and schema change metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "mutation"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseMutationInfo {
    const QUERY_MUTATION_OVERVIEW: &'static str = "mutation_overview";
    const QUERY_MUTATION_PROGRESS: &'static str = "mutation_progress";
    const QUERY_RECENT_MUTATION_ACTIVITY: &'static str = "recent_mutation_activity";
    const QUERY_STUCK_MUTATIONS: &'static str = "stuck_mutations";
    const DETAIL_QUERY_LONG_RUNNING_MUTATIONS: &'static str = "long_running_mutations";
    const DETAIL_QUERY_FAILED_MUTATIONS: &'static str = "failed_mutations";
    const DETAIL_QUERY_STUCK_MUTATIONS: &'static str = "stuck_mutations";
    const DETAIL_QUERY_LARGE_MUTATIONS: &'static str = "large_mutations";
    const DETAIL_QUERY_RECENT_COMPLETIONS: &'static str = "recent_completions";
    const DETAIL_QUERY_COMMAND_BREAKDOWN: &'static str = "command_breakdown";
    const DETAIL_QUERY_MULTIPLE_MUTATIONS: &'static str = "multiple_mutations";
    const LONG_MUTATION_THRESHOLD: f64 = 1800.0; // 30 minutes
    const LARGE_MUTATION_THRESHOLD: u64 = 1000; // 1000 parts
    // Threshold reserved for future health-check reporting
    #[allow(dead_code)]
    const STUCK_MUTATION_THRESHOLD: f64 = 21600.0; // 6 hours without progress
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

    fn should_collect_detailed_metrics(core_info: &ClickhouseMutationInfo) -> bool {
        core_info.longest_mutation_duration > Self::LONG_MUTATION_THRESHOLD
            || core_info.failed_mutations_last_24h > 0
            || core_info.stuck_mutations > 0
            || core_info.total_parts_to_mutate > Self::LARGE_MUTATION_THRESHOLD
            || core_info.tables_with_active_mutations > 5
    }
}

/// Long-running mutation operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLongMutation {
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
    /// Duration the mutation has been running in seconds
    pub duration: f64,
    /// Number of parts still to be mutated
    pub parts_to_do: u64,
    /// Number of parts already completed
    pub parts_completed: u64,
    /// Latest failed part name
    pub latest_failed_part: Option<String>,
    /// When the latest failure occurred
    pub latest_fail_time: Option<DateTimeWrapper>,
    /// Reason for the latest failure
    pub latest_fail_reason: Option<String>,
    /// Block number for the mutation
    pub block_number: u64,
}

/// Failed mutation operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedMutation {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Unique mutation identifier
    pub mutation_id: String,
    /// Mutation command that failed
    pub command: String,
    /// When the mutation was created
    pub create_time: DateTimeWrapper,
    /// Name of the part that failed
    pub latest_failed_part: String,
    /// When the failure occurred
    pub latest_fail_time: DateTimeWrapper,
    /// Reason for the failure
    pub latest_fail_reason: Option<String>,
    /// Number of parts remaining to mutate
    pub parts_to_do: u64,
    /// Number of parts completed before failure
    pub parts_completed_before_failure: u64,
    /// Block number for the mutation
    pub block_number: u64,
}

/// Stuck mutation operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStuckMutation {
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
    /// Duration the mutation has been stuck in seconds
    pub stuck_duration: f64,
    /// Number of parts still to be mutated
    pub parts_to_do: u64,
    /// Number of parts completed
    pub parts_completed: u64,
    /// Last time a failure occurred
    pub latest_fail_time: Option<DateTimeWrapper>,
    /// Last failure reason
    pub latest_fail_reason: Option<String>,
    /// Last failed part name
    pub latest_failed_part: Option<String>,
}

/// Large mutation operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargeMutation {
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
    /// Number of parts still to be mutated
    pub parts_to_do: u64,
    /// Number of parts completed
    pub parts_completed: u64,
    /// Total number of parts involved
    pub total_parts: u64,
    /// Duration elapsed so far
    pub duration: f64,
    /// Last failure time if any
    pub latest_fail_time: Option<DateTimeWrapper>,
    /// Whether the mutation is complete
    pub is_done: bool,
}

/// Mutation completion information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMutationCompletion {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Unique mutation identifier
    pub mutation_id: String,
    /// Mutation command that completed
    pub command: String,
    /// When the mutation was created
    pub create_time: DateTimeWrapper,
    /// When the mutation completed
    pub completion_time: DateTimeWrapper,
    /// Total duration from start to completion
    pub total_duration: f64,
    /// Number of parts processed
    pub parts_processed: u64,
    /// Block number for the mutation
    pub block_number: u64,
}

/// Mutation command type statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMutationCommandStats {
    /// Type of command (ALTER TABLE, UPDATE, DELETE etc.)
    pub command_type: String,
    /// Total number of mutations of this type
    pub total_count: u64,
    /// Number currently active
    pub active_count: u64,
    /// Number completed
    pub completed_count: u64,
    /// Number that failed
    pub failed_count: u64,
    /// Average duration for this command type
    pub avg_duration: f64,
}

/// Table with multiple concurrent mutations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseTableMutationInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Total number of mutations on this table
    pub mutation_count: u64,
    /// Number of active mutations
    pub active_mutation_count: u64,
    /// Total parts to be mutated across all mutations
    pub total_parts_to_mutate: u64,
    /// Age of the oldest mutation in seconds
    pub oldest_mutation_age: f64,
    /// Number of failed mutations
    pub failed_mutation_count: u64,
}

impl ClickhouseMutationInfo {
    /// Checks if there are long-running mutations
    pub fn has_long_running_mutations(&self, threshold_seconds: f64) -> bool {
        self.longest_mutation_duration > threshold_seconds
    }

    /// Checks if there are recent mutation failures
    pub fn has_recent_failures(&self) -> bool {
        self.failed_mutations_last_24h > 0
    }

    /// Checks if there are stuck mutations
    pub fn has_stuck_mutations(&self) -> bool {
        self.stuck_mutations > 0
    }

    /// Checks if there are active mutations
    pub fn has_active_mutations(&self) -> bool {
        self.active_mutations > 0
    }

    /// Checks if there are waiting mutations
    pub fn has_waiting_mutations(&self) -> bool {
        self.waiting_mutations > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets mutation completion rate (0.0 to 1.0)
    pub fn get_completion_rate(&self) -> f64 {
        if self.total_mutations == 0 {
            return 0.0;
        }
        self.completed_mutations as f64 / self.total_mutations as f64
    }

    /// Gets mutation failure rate (0.0 to 1.0)
    pub fn get_failure_rate(&self) -> f64 {
        if self.total_mutations == 0 {
            return 0.0;
        }
        self.failed_mutations as f64 / self.total_mutations as f64
    }

    /// Gets overall progress across all active mutations (0.0 to 1.0)
    pub fn get_overall_progress(&self) -> f64 {
        self.avg_mutation_progress
    }

    /// Gets average completion time in minutes
    pub fn get_avg_completion_time_minutes(&self) -> f64 {
        self.avg_completion_time / 60.0
    }

    /// Gets longest mutation duration in minutes
    pub fn get_longest_mutation_duration_minutes(&self) -> f64 {
        self.longest_mutation_duration / 60.0
    }

    /// Gets mutation throughput (completions per hour)
    pub fn get_mutation_throughput_per_hour(&self) -> f64 {
        self.completed_mutations_last_hour as f64
    }

    /// Gets parts processing efficiency (parts completed vs remaining)
    pub fn get_parts_processing_efficiency(&self) -> f64 {
        let total_parts = self.total_parts_to_mutate + self.total_parts_mutated;
        if total_parts == 0 {
            return 0.0;
        }
        self.total_parts_mutated as f64 / total_parts as f64
    }

    /// Gets mutation activity level
    pub fn get_mutation_activity_level(&self) -> MutationActivityLevel {
        if self.active_mutations == 0 {
            MutationActivityLevel::Idle
        } else if self.active_mutations <= 5 {
            MutationActivityLevel::Low
        } else if self.active_mutations <= 15 {
            MutationActivityLevel::Moderate
        } else if self.active_mutations <= 30 {
            MutationActivityLevel::High
        } else {
            MutationActivityLevel::VeryHigh
        }
    }

    /// Gets mutation health status
    pub fn get_mutation_health_status(&self) -> MutationHealthStatus {
        let failure_rate = self.get_failure_rate();
        let has_stuck = self.stuck_mutations > 0;
        let has_long_running = self.longest_mutation_duration > 3600.0; // 1 hour
        let recent_failure_rate = if self.active_mutations > 0 {
            self.failed_mutations_last_24h as f64 / self.active_mutations as f64
        } else {
            0.0
        };

        if has_stuck && (failure_rate > 0.2 || recent_failure_rate > 0.5) {
            MutationHealthStatus::Critical
        } else if has_stuck || failure_rate > 0.1 || has_long_running || recent_failure_rate > 0.2 {
            MutationHealthStatus::Warning
        } else if failure_rate > 0.05 || self.failed_mutations_last_24h > 0 {
            MutationHealthStatus::Attention
        } else {
            MutationHealthStatus::Healthy
        }
    }

    /// Gets estimated time to complete all active mutations in minutes
    pub fn get_estimated_completion_time_minutes(&self) -> f64 {
        if self.avg_completion_time <= 0.0 || self.active_mutations == 0 {
            return 0.0;
        }
        // Estimate based on average progress and remaining work
        let avg_remaining_work = 1.0 - self.avg_mutation_progress;
        (self.avg_completion_time * avg_remaining_work) / 60.0
    }

    /// Gets table contention level (tables with multiple concurrent mutations)
    pub fn get_table_contention_ratio(&self) -> f64 {
        if self.tables_with_active_mutations == 0 {
            return 0.0;
        }
        // This would need to be calculated from detailed metrics
        // For now, estimate based on mutations per table
        let avg_mutations_per_table = self.active_mutations as f64 / self.tables_with_active_mutations as f64;
        if avg_mutations_per_table > 2.0 {
            0.8
        } else if avg_mutations_per_table > 1.5 {
            0.5
        } else {
            0.2
        }
    }

    /// Gets stuck mutation ratio
    pub fn get_stuck_mutation_ratio(&self) -> f64 {
        if self.active_mutations == 0 {
            return 0.0;
        }
        self.stuck_mutations as f64 / self.active_mutations as f64
    }
}

/// Mutation activity level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MutationActivityLevel {
    /// No mutations running
    Idle,
    /// Low mutation activity (1-5 mutations)
    Low,
    /// Moderate mutation activity (6-15 mutations)
    Moderate,
    /// High mutation activity (16-30 mutations)
    High,
    /// Very high mutation activity (30+ mutations)
    VeryHigh,
}

/// Mutation health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MutationHealthStatus {
    /// All mutations are healthy
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
//     async fn test_clickhouse_mutation_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let mutation_info = ClickhouseMutationInfo::default();
//
//         let result = mutation_info
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
//         assert!(info.get_completion_rate() >= 0.0);
//         assert!(info.get_completion_rate() <= 1.0);
//         assert!(info.get_failure_rate() >= 0.0);
//         assert!(info.get_failure_rate() <= 1.0);
//     }
//
//     #[test]
//     fn test_clickhouse_mutation_calculations() {
//         let mut mutation_info = ClickhouseMutationInfo::default();
//         mutation_info.total_mutations = 20;
//         mutation_info.active_mutations = 8;
//         mutation_info.completed_mutations = 10;
//         mutation_info.failed_mutations = 2;
//         mutation_info.longest_mutation_duration = 2400.0; // 40 minutes
//         mutation_info.avg_completion_time = 1800.0; // 30 minutes
//         mutation_info.avg_mutation_progress = 0.6;
//         mutation_info.stuck_mutations = 1;
//         mutation_info.failed_mutations_last_24h = 3;
//         mutation_info.completed_mutations_last_hour = 2;
//
//         assert!(mutation_info.has_long_running_mutations(1800.0)); // 30 minutes threshold
//         assert!(mutation_info.has_recent_failures());
//         assert!(mutation_info.has_stuck_mutations());
//         assert!(mutation_info.has_active_mutations());
//
//         assert_eq!(mutation_info.get_completion_rate(), 0.5);
//         assert_eq!(mutation_info.get_failure_rate(), 0.1);
//         assert_eq!(mutation_info.get_overall_progress(), 0.6);
//         assert_eq!(mutation_info.get_avg_completion_time_minutes(), 30.0);
//         assert_eq!(mutation_info.get_longest_mutation_duration_minutes(), 40.0);
//         assert_eq!(mutation_info.get_mutation_throughput_per_hour(), 2.0);
//         assert_eq!(mutation_info.get_stuck_mutation_ratio(), 0.125);
//
//         let activity_level = mutation_info.get_mutation_activity_level();
//         assert!(matches!(activity_level, MutationActivityLevel::Moderate));
//
//         let health_status = mutation_info.get_mutation_health_status();
//         assert!(matches!(health_status, MutationHealthStatus::Warning));
//     }
//
//     #[test]
//     fn test_mutation_health_classification() {
//         // Test healthy status
//         let mut healthy_mutation = ClickhouseMutationInfo::default();
//         healthy_mutation.total_mutations = 10;
//         healthy_mutation.active_mutations = 5;
//         healthy_mutation.completed_mutations = 5;
//         healthy_mutation.failed_mutations = 0;
//         healthy_mutation.stuck_mutations = 0;
//         healthy_mutation.longest_mutation_duration = 600.0; // 10 minutes
//
//         assert!(matches!(healthy_mutation.get_mutation_health_status(), MutationHealthStatus::Healthy));
//
//         // Test critical status
//         let mut critical_mutation = ClickhouseMutationInfo::default();
//         critical_mutation.total_mutations = 20;
//         critical_mutation.active_mutations = 10;
//         critical_mutation.completed_mutations = 5;
//         critical_mutation.failed_mutations = 5; // 25% failure rate
//         critical_mutation.stuck_mutations = 3;
//         critical_mutation.failed_mutations_last_24h = 6;
//
//         assert!(matches!(critical_mutation.get_mutation_health_status(), MutationHealthStatus::Critical));
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhouseMutationInfo;

    #[test]
    fn mutations_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhouseMutationInfo::default();
        assert!(!ClickhouseMutationInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn mutations_detailed_gate_true_for_stuck_mutations() {
        let info = ClickhouseMutationInfo { stuck_mutations: 1, ..ClickhouseMutationInfo::default() };
        assert!(ClickhouseMutationInfo::should_collect_detailed_metrics(&info));
    }
}
