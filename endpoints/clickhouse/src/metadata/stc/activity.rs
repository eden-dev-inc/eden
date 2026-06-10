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

/// Clickhouse database activity information and statistics.
///
/// Covers query performance and workload metrics.
/// Connection-specific metrics live in `ClickhouseConnectionInfo`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseActivityInfo {
    /// Number of currently running queries
    pub running_queries: u64,
    /// Number of queries in the queue waiting to execute
    pub queued_queries: u64,
    /// Duration of the longest running query in seconds
    pub longest_query_duration: f64,
    /// Average query duration across all running queries
    pub avg_running_query_duration: f64,
    /// Number of queries that failed in the last minute
    pub failed_queries_last_minute: u64,
    /// Number of queries per second (QPS) - current rate
    pub queries_per_second: f64,
    /// Memory usage by all running queries in bytes
    pub query_memory_usage: u64,
    /// Number of running background merges
    pub running_merges: u64,
    /// Number of running mutations
    pub running_mutations: u64,
    /// Number of running distributed queries
    pub distributed_queries: u64,
    /// Average rows per second being processed
    pub rows_per_second: u64,
    /// Average bytes per second being read
    pub bytes_per_second: u64,
    /// Number of queries waiting for locks or resources
    pub waiting_queries: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<ClickhouseDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDetailedMetrics {
    /// Long-running queries (only collected when longest_query_duration > threshold)
    pub long_running_queries: Vec<ClickhouseActiveQuery>,
    /// Failed queries details (only collected when failed_queries_last_minute > 0)
    pub recent_failed_queries: Vec<ClickhouseFailedQuery>,
    /// Memory-intensive queries (collected when memory usage is high)
    pub memory_intensive_queries: Vec<ClickhouseMemoryQuery>,
    /// Connection breakdown by user and database
    pub connections_by_user: Option<Vec<ClickhouseConnectionsByUser>>,
}

impl MetadataCollection for ClickhouseActivityInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_STATS,
                query(
                    "SELECT
                    count() as running_queries,
                    sumIf(1, query = '') as queued_queries,
                    max(elapsed) as longest_query_duration,
                    avg(elapsed) as avg_running_query_duration,
                    sum(memory_usage) as query_memory_usage,
                    sum(read_rows) / nullif(max(elapsed), 0) as rows_per_second,
                    sum(read_bytes) / nullif(max(elapsed), 0) as bytes_per_second,
                    sumIf(1, query LIKE '%Distributed%' OR query LIKE '%GLOBAL%') as distributed_queries,
                    0 as waiting_queries
                FROM system.processes
                WHERE query != ''"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_PERFORMANCE_STATS,
                query(
                    "SELECT
                    countIf(event_time >= now() - INTERVAL 1 MINUTE AND exception != '') as failed_queries_last_minute,
                    countIf(event_time >= now() - INTERVAL 1 SECOND) as queries_last_second
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 MINUTE"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_BACKGROUND_OPS,
                query(
                    "SELECT
                    (SELECT count() FROM system.merges) as running_merges,
                    (SELECT count() FROM system.mutations WHERE is_done = 0) as running_mutations"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse activity metrics with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "activity"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseActivityInfo {
    const QUERY_STATS: &'static str = "query_stats";
    const QUERY_PERFORMANCE_STATS: &'static str = "performance_stats";
    const QUERY_BACKGROUND_OPS: &'static str = "background_ops";
    const LONG_QUERY_THRESHOLD: f64 = 30.0; // 30 seconds
    const HIGH_MEMORY_THRESHOLD: u64 = 1_073_741_824; // 1GB
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
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

/// A currently running ClickHouse query.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseActiveQuery {
    /// Unique query identifier
    pub query_id: String,
    /// Username executing the query
    pub user: String,
    /// Database name where the query is executing
    pub database: String,
    /// SQL query text (truncated for safety)
    pub query: String,
    /// Duration the query has been running (seconds)
    pub duration: f64,
    /// Memory usage by this query in bytes
    pub memory_usage: u64,
    /// Number of rows read by the query
    pub read_rows: u64,
    /// Number of bytes read by the query
    pub read_bytes: u64,
    /// Time when the query started
    pub query_start_time: DateTimeWrapper,
    /// Type of query (Select, Insert etc.)
    pub query_kind: String,
    /// Client application name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Main thread ID executing the query
    pub main_thread_id: u64,
}

/// Information about failed queries
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedQuery {
    /// Unique query identifier
    pub query_id: String,
    /// Username that executed the query
    pub user: String,
    /// Database where the query failed
    pub database: String,
    /// SQL query that failed (truncated)
    pub query: String,
    /// Exception message describing the failure
    pub exception: String,
    /// Duration the query ran before failing (seconds)
    pub duration: f64,
    /// When the query failed
    pub event_time: DateTimeWrapper,
    /// Type of query that failed
    pub query_kind: String,
    /// Client application name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Memory usage when the query failed
    pub memory_usage: u64,
    /// Rows read before failure
    pub read_rows: u64,
    /// Bytes read before failure
    pub read_bytes: u64,
}

/// Information about memory-intensive queries
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMemoryQuery {
    /// Unique query identifier
    pub query_id: String,
    /// Username executing the query
    pub user: String,
    /// Database name
    pub database: String,
    /// SQL query text (truncated)
    pub query: String,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Duration the query has been running (seconds)
    pub duration: f64,
    /// Number of rows read
    pub read_rows: u64,
    /// Number of bytes read
    pub read_bytes: u64,
    /// Time when the query started
    pub query_start_time: DateTimeWrapper,
}

/// Connection statistics grouped by user
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseConnectionsByUser {
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Total connections for this user/database combination
    pub total_connections: u64,
    /// Active queries for this user
    pub active_queries: u64,
    /// Protocol breakdown (HTTP, TCP etc.)
    pub connection_protocols: HashMap<String, u64>,
}

impl ClickhouseActivityInfo {
    /// Checks if there are long-running queries
    pub fn has_long_running_queries(&self, threshold_seconds: f64) -> bool {
        self.longest_query_duration > threshold_seconds
    }

    /// Checks if there are failed queries recently
    pub fn has_recent_failures(&self) -> bool {
        self.failed_queries_last_minute > 0
    }

    /// Checks if memory usage is high
    pub fn has_high_memory_usage(&self, threshold_bytes: u64) -> bool {
        self.query_memory_usage > threshold_bytes
    }

    /// Checks if there are queries queued (indicating capacity issues)
    pub fn has_queued_queries(&self) -> bool {
        self.queued_queries > 0
    }

    /// Checks if there are background operations running
    pub fn has_background_operations(&self) -> bool {
        self.running_merges > 0 || self.running_mutations > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets queries per second rate
    pub fn get_qps(&self) -> f64 {
        self.queries_per_second
    }

    /// Gets memory usage in MB
    pub fn get_memory_usage_mb(&self) -> f64 {
        self.query_memory_usage as f64 / 1_048_576.0 // Convert bytes to MB
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
//     async fn test_clickhouse_metadata_activity() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let activity_info = ClickhouseActivityInfo::default();
//
//         let result = activity_info
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
//         assert!(info.connection_utilization_pct >= 0.0);
//         assert!(info.connection_utilization_pct <= 100.0);
//         assert!(info.queries_per_second >= 0.0);
//     }
//
//     #[test]
//     fn test_clickhouse_activity_calculations() {
//         let mut activity = ClickhouseActivityInfo::default();
//         activity.running_queries = 5;
//         activity.total_connections = 20;
//         activity.failed_queries_last_minute = 2;
//         activity.query_memory_usage = 2_147_483_648; // 2GB
//
//         assert_eq!(activity.query_utilization_percentage(), 25.0);
//         assert!(activity.has_recent_failures());
//         assert!(activity.has_high_memory_usage(1_073_741_824)); // 1GB threshold
//         assert_eq!(activity.get_memory_usage_mb(), 2048.0);
//     }
// }
