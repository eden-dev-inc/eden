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

/// Clickhouse query performance and execution metrics.
///
/// Covers running and historical query stats, execution times and
/// resource usage.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseQueryInfo {
    /// Total number of currently running queries
    pub running_queries: u64,
    /// Number of slow running queries (over threshold)
    pub slow_queries: u64,
    /// Number of queries using high memory
    pub high_memory_queries: u64,
    /// Number of queries with long execution time
    pub long_running_queries: u64,
    /// Total queries executed in the last hour
    pub queries_last_hour: u64,
    /// Failed queries in the last hour
    pub failed_queries_last_hour: u64,
    /// Average query execution time in seconds
    pub avg_query_execution_time: f64,
    /// Maximum query execution time currently running
    pub max_running_query_time: f64,
    /// Total memory usage by all queries in bytes
    pub total_query_memory_usage: u64,
    /// Average memory usage per query in bytes
    pub avg_query_memory_usage: u64,
    /// Maximum memory usage by a single query in bytes
    pub max_query_memory_usage: u64,
    /// Number of queries waiting for locks
    pub queries_waiting_for_locks: u64,
    /// Number of queries reading from disk
    pub queries_reading_from_disk: u64,
    /// Total bytes read by all queries
    pub total_bytes_read: u64,
    /// Total rows processed by all queries
    pub total_rows_processed: u64,
    /// Number of cancelled queries in last hour
    pub cancelled_queries_last_hour: u64,
    /// Detailed metrics collected when performance issues are detected
    pub detailed_metrics: Option<ClickhouseQueryDetailedMetrics>,
}

/// Detailed query metrics collected when performance issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseQueryDetailedMetrics {
    /// Currently running slow queries
    pub slow_running_queries: Vec<ClickhouseSlowQuery>,
    /// Queries using excessive memory
    pub high_memory_queries: Vec<ClickhouseHighMemoryQuery>,
    /// Long running queries
    pub long_running_queries: Vec<ClickhouseLongRunningQuery>,
    /// Recently failed queries
    pub recent_failed_queries: Vec<ClickhouseFailedQuery>,
    /// Queries waiting for resources
    pub blocked_queries: Vec<ClickhouseBlockedQuery>,
    /// Most expensive queries by resource usage
    pub expensive_queries: Vec<ClickhouseExpensiveQuery>,
    /// Query performance statistics by database
    pub database_query_stats: Vec<ClickhouseDatabaseQueryStats>,
    /// Query performance statistics by user
    pub user_query_stats: Vec<ClickhouseUserQueryStats>,
}

impl MetadataCollection for ClickhouseQueryInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_OVERVIEW,
                query(
                    "SELECT
                    count() as running_queries,
                    countIf(elapsed > 10) as slow_queries,
                    countIf(memory_usage > 1000000000) as high_memory_queries,
                    countIf(elapsed > 300) as long_running_queries,
                    sum(memory_usage) as total_query_memory_usage,
                    avg(memory_usage) as avg_query_memory_usage,
                    max(memory_usage) as max_query_memory_usage,
                    max(elapsed) as max_running_query_time,
                    sum(read_bytes) as total_bytes_read,
                    sum(read_rows) as total_rows_processed,
                    countIf(is_cancelled = 1) as cancelled_queries
                FROM system.processes"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_LOCKS,
                query(
                    "SELECT
                    countIf(query LIKE '%LOCK%' OR query LIKE '%ALTER%') as queries_waiting_for_locks
                FROM system.processes"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_DISK_USAGE,
                query(
                    "SELECT
                    countIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] > 0) as queries_reading_from_disk
                FROM system.processes
                WHERE ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] IS NOT NULL"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_RECENT_STATS,
                query(
                    "SELECT
                    count() as queries_last_hour,
                    countIf(exception != '') as failed_queries_last_hour,
                    avg(query_duration_ms / 1000) as avg_query_execution_time,
                    countIf(exception_code = 394) as cancelled_queries_last_hour
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 HOUR
                    AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse query performance and execution metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "query_performance"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseQueryInfo {
    const QUERY_OVERVIEW: &'static str = "query_overview";
    const QUERY_LOCKS: &'static str = "query_locks";
    const QUERY_DISK_USAGE: &'static str = "query_disk_usage";
    const QUERY_RECENT_STATS: &'static str = "recent_query_stats";
    const DETAIL_QUERY_SLOW_QUERIES: &'static str = "slow_queries";
    const DETAIL_QUERY_HIGH_MEMORY_QUERIES: &'static str = "high_memory_queries";
    const DETAIL_QUERY_LONG_RUNNING_QUERIES: &'static str = "long_running_queries";
    const DETAIL_QUERY_FAILED_QUERIES: &'static str = "failed_queries";
    const DETAIL_QUERY_BLOCKED_QUERIES: &'static str = "blocked_queries";
    const DETAIL_QUERY_EXPENSIVE_QUERIES: &'static str = "expensive_queries";
    const DETAIL_QUERY_DATABASE_STATS: &'static str = "database_stats";
    const DETAIL_QUERY_USER_STATS: &'static str = "user_stats";
    const SLOW_QUERY_THRESHOLD: f64 = 10.0; // 10 seconds
    const HIGH_MEMORY_THRESHOLD: u64 = 1_000_000_000; // 1GB
    const LONG_RUNNING_THRESHOLD: f64 = 300.0; // 5 minutes
    const EXPENSIVE_QUERY_THRESHOLD: u64 = 100_000_000; // 100MB memory
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

    fn should_collect_detailed_metrics(core_info: &ClickhouseQueryInfo) -> bool {
        core_info.slow_queries > 0
            || core_info.high_memory_queries > 0
            || core_info.long_running_queries > 0
            || core_info.failed_queries_last_hour > 0
            || core_info.queries_waiting_for_locks > 0
            || core_info.running_queries > 10
            || core_info.cancelled_queries_last_hour > 0
    }
}

/// Slow running query information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSlowQuery {
    /// Query ID
    pub query_id: String,
    /// User executing the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Approximate total rows to process
    pub total_rows_approx: u64,
    /// Client name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// High memory usage query information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseHighMemoryQuery {
    /// Query ID
    pub query_id: String,
    /// User executing the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Peak memory usage in bytes
    pub peak_memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Bytes written
    pub written_bytes: u64,
    /// Rows written
    pub written_rows: u64,
    /// Client name
    pub client_name: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// Long running query information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLongRunningQuery {
    /// Query ID
    pub query_id: String,
    /// User executing the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Client name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// Failed query information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedQuery {
    /// Query ID
    pub query_id: String,
    /// User who executed the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Exception message
    pub exception: String,
    /// When the failure occurred
    pub event_time: DateTimeWrapper,
    /// Query duration in milliseconds
    pub query_duration_ms: u64,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Bytes written
    pub written_bytes: u64,
    /// Rows written
    pub written_rows: u64,
    /// Result bytes
    pub result_bytes: u64,
    /// Result rows
    pub result_rows: u64,
    /// Client name
    pub client_name: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// Blocked query information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseBlockedQuery {
    /// Query ID
    pub query_id: String,
    /// User executing the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Client name
    pub client_name: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// Expensive query information (high resource usage)
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseExpensiveQuery {
    /// Query ID
    pub query_id: String,
    /// User executing the query
    pub user: String,
    /// Database name
    pub database: Option<String>,
    /// Query text
    pub query: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Peak memory usage in bytes
    pub peak_memory_usage: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Rows read
    pub read_rows: u64,
    /// Bytes written
    pub written_bytes: u64,
    /// Rows written
    pub written_rows: u64,
    /// CPU time in microseconds
    pub cpu_time_microseconds: u64,
    /// IO wait time in microseconds
    pub io_wait_microseconds: u64,
    /// Client name
    pub client_name: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
}

/// Database query statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDatabaseQueryStats {
    /// Database name
    pub database: String,
    /// Total number of queries
    pub query_count: u64,
    /// Average query duration in seconds
    pub avg_duration_seconds: f64,
    /// Maximum query duration in seconds
    pub max_duration_seconds: f64,
    /// Total memory usage across all queries
    pub total_memory_usage: u64,
    /// Average memory usage per query
    pub avg_memory_usage: u64,
    /// Total bytes read across all queries
    pub total_bytes_read: u64,
    /// Total rows read across all queries
    pub total_rows_read: u64,
    /// Number of failed queries
    pub failed_queries: u64,
}

/// User query statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseUserQueryStats {
    /// User name
    pub user: String,
    /// Total number of queries
    pub query_count: u64,
    /// Average query duration in seconds
    pub avg_duration_seconds: f64,
    /// Maximum query duration in seconds
    pub max_duration_seconds: f64,
    /// Total memory usage across all queries
    pub total_memory_usage: u64,
    /// Average memory usage per query
    pub avg_memory_usage: u64,
    /// Total bytes read across all queries
    pub total_bytes_read: u64,
    /// Total rows read across all queries
    pub total_rows_read: u64,
    /// Number of failed queries
    pub failed_queries: u64,
}

impl ClickhouseQueryInfo {
    /// Checks if there are slow running queries
    pub fn has_slow_queries(&self) -> bool {
        self.slow_queries > 0
    }

    /// Checks if there are high memory usage queries
    pub fn has_high_memory_queries(&self) -> bool {
        self.high_memory_queries > 0
    }

    /// Checks if there are long running queries
    pub fn has_long_running_queries(&self) -> bool {
        self.long_running_queries > 0
    }

    /// Checks if there were recent query failures
    pub fn has_recent_failures(&self) -> bool {
        self.failed_queries_last_hour > 0
    }

    /// Checks if there are queries waiting for locks
    pub fn has_blocked_queries(&self) -> bool {
        self.queries_waiting_for_locks > 0
    }

    /// Checks if there are queries reading from disk
    pub fn has_disk_intensive_queries(&self) -> bool {
        self.queries_reading_from_disk > 0
    }

    /// Checks if there were recent query cancellations
    pub fn has_recent_cancellations(&self) -> bool {
        self.cancelled_queries_last_hour > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets query failure rate (0.0 to 1.0)
    pub fn get_failure_rate(&self) -> f64 {
        if self.queries_last_hour == 0 {
            return 0.0;
        }
        self.failed_queries_last_hour as f64 / self.queries_last_hour as f64
    }

    /// Gets query cancellation rate (0.0 to 1.0)
    pub fn get_cancellation_rate(&self) -> f64 {
        if self.queries_last_hour == 0 {
            return 0.0;
        }
        self.cancelled_queries_last_hour as f64 / self.queries_last_hour as f64
    }

    /// Gets slow query rate (proportion of running queries that are slow)
    pub fn get_slow_query_rate(&self) -> f64 {
        if self.running_queries == 0 {
            return 0.0;
        }
        self.slow_queries as f64 / self.running_queries as f64
    }

    /// Gets high memory query rate
    pub fn get_high_memory_query_rate(&self) -> f64 {
        if self.running_queries == 0 {
            return 0.0;
        }
        self.high_memory_queries as f64 / self.running_queries as f64
    }

    /// Gets blocked query rate
    pub fn get_blocked_query_rate(&self) -> f64 {
        if self.running_queries == 0 {
            return 0.0;
        }
        self.queries_waiting_for_locks as f64 / self.running_queries as f64
    }

    /// Gets average memory usage in MB
    pub fn get_avg_memory_usage_mb(&self) -> f64 {
        self.avg_query_memory_usage as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets total memory usage in MB
    pub fn get_total_memory_usage_mb(&self) -> f64 {
        self.total_query_memory_usage as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets max memory usage in MB
    pub fn get_max_memory_usage_mb(&self) -> f64 {
        self.max_query_memory_usage as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets total bytes read in MB
    pub fn get_total_bytes_read_mb(&self) -> f64 {
        self.total_bytes_read as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets average query execution time in minutes
    pub fn get_avg_query_execution_time_minutes(&self) -> f64 {
        self.avg_query_execution_time / 60.0
    }

    /// Gets max running query time in minutes
    pub fn get_max_running_query_time_minutes(&self) -> f64 {
        self.max_running_query_time / 60.0
    }

    /// Gets disk usage intensity (proportion of queries reading from disk)
    pub fn get_disk_usage_intensity(&self) -> f64 {
        if self.running_queries == 0 {
            return 0.0;
        }
        self.queries_reading_from_disk as f64 / self.running_queries as f64
    }

    /// Gets query throughput (queries per minute in last hour)
    pub fn get_query_throughput_per_minute(&self) -> f64 {
        self.queries_last_hour as f64 / 60.0
    }

    /// Gets query performance status
    pub fn get_performance_status(&self) -> QueryPerformanceStatus {
        let failure_rate = self.get_failure_rate();
        let slow_rate = self.get_slow_query_rate();
        let memory_rate = self.get_high_memory_query_rate();
        let blocked_rate = self.get_blocked_query_rate();

        if failure_rate > 0.2 || slow_rate > 0.5 || memory_rate > 0.3 || blocked_rate > 0.4 {
            QueryPerformanceStatus::Critical
        } else if failure_rate > 0.1 || slow_rate > 0.3 || memory_rate > 0.2 || blocked_rate > 0.2 {
            QueryPerformanceStatus::Warning
        } else if failure_rate > 0.05 || slow_rate > 0.1 || memory_rate > 0.1 || blocked_rate > 0.1 {
            QueryPerformanceStatus::Attention
        } else {
            QueryPerformanceStatus::Healthy
        }
    }

    /// Gets query load level
    pub fn get_query_load_level(&self) -> QueryLoadLevel {
        if self.running_queries == 0 {
            QueryLoadLevel::Idle
        } else if self.running_queries <= 5 {
            QueryLoadLevel::Low
        } else if self.running_queries <= 20 {
            QueryLoadLevel::Moderate
        } else if self.running_queries <= 50 {
            QueryLoadLevel::High
        } else {
            QueryLoadLevel::VeryHigh
        }
    }

    /// Gets memory pressure level
    pub fn get_memory_pressure_level(&self) -> MemoryPressureLevel {
        let total_memory_gb = self.total_query_memory_usage as f64 / 1_073_741_824.0; // Convert to GB

        if total_memory_gb == 0.0 {
            MemoryPressureLevel::None
        } else if total_memory_gb <= 1.0 {
            MemoryPressureLevel::Low
        } else if total_memory_gb <= 5.0 {
            MemoryPressureLevel::Moderate
        } else if total_memory_gb <= 20.0 {
            MemoryPressureLevel::High
        } else {
            MemoryPressureLevel::Critical
        }
    }

    /// Gets resource efficiency (higher is better)
    pub fn get_resource_efficiency(&self) -> f64 {
        if self.running_queries == 0 {
            return 1.0;
        }

        let slow_penalty = self.get_slow_query_rate() * 0.4;
        let memory_penalty = self.get_high_memory_query_rate() * 0.3;
        let blocked_penalty = self.get_blocked_query_rate() * 0.3;

        (1.0 - (slow_penalty + memory_penalty + blocked_penalty)).max(0.0)
    }
}

/// Query performance status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum QueryPerformanceStatus {
    /// All queries are performing well
    Healthy,
    /// Minor performance issues that should be monitored
    Attention,
    /// Performance issues that require investigation
    Warning,
    /// Critical performance issues requiring immediate attention
    Critical,
}

/// Query load level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum QueryLoadLevel {
    /// No queries running
    Idle,
    /// Low query load
    Low,
    /// Moderate query load
    Moderate,
    /// High query load
    High,
    /// Very high query load
    VeryHigh,
}

/// Memory pressure level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MemoryPressureLevel {
    /// No memory pressure
    None,
    /// Low memory usage
    Low,
    /// Moderate memory usage
    Moderate,
    /// High memory usage
    High,
    /// Critical memory usage
    Critical,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_query_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let query_info = ClickhouseQueryInfo::default();
//
//         let result = query_info
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
//         assert!(info.get_failure_rate() >= 0.0);
//         assert!(info.get_failure_rate() <= 1.0);
//         assert!(info.get_slow_query_rate() >= 0.0);
//         assert!(info.get_slow_query_rate() <= 1.0);
//     }
//
//     #[test]
//     fn test_clickhouse_query_calculations() {
//         let mut query_info = ClickhouseQueryInfo::default();
//         query_info.running_queries = 25;
//         query_info.slow_queries = 5;
//         query_info.high_memory_queries = 3;
//         query_info.long_running_queries = 2;
//         query_info.queries_last_hour = 1000;
//         query_info.failed_queries_last_hour = 50;
//         query_info.cancelled_queries_last_hour = 10;
//         query_info.avg_query_execution_time = 15.5; // 15.5 seconds
//         query_info.max_running_query_time = 420.0; // 7 minutes
//         query_info.total_query_memory_usage = 5_368_709_120; // 5GB
//         query_info.avg_query_memory_usage = 214_748_364; // ~200MB
//         query_info.max_query_memory_usage = 1_073_741_824; // 1GB
//         query_info.queries_waiting_for_locks = 2;
//         query_info.queries_reading_from_disk = 8;
//         query_info.total_bytes_read = 104_857_600; // 100MB
//         query_info.total_rows_processed = 10_000_000;
//
//         assert_eq!(query_info.get_failure_rate(), 0.05);
//         assert_eq!(query_info.get_cancellation_rate(), 0.01);
//         assert_eq!(query_info.get_slow_query_rate(), 0.2);
//         assert_eq!(query_info.get_high_memory_query_rate(), 0.12);
//         assert_eq!(query_info.get_blocked_query_rate(), 0.08);
//
//         assert!((query_info.get_avg_memory_usage_mb() - 204.8).abs() < 1.0);
//         assert!((query_info.get_total_memory_usage_mb() - 5120.0).abs() < 1.0);
//         assert!((query_info.get_max_memory_usage_mb() - 1024.0).abs() < 1.0);
//         assert!((query_info.get_total_bytes_read_mb() - 100.0).abs() < 1.0);
//
//         assert!((query_info.get_avg_query_execution_time_minutes() - 0.258).abs() < 0.01);
//         assert_eq!(query_info.get_max_running_query_time_minutes(), 7.0);
//         assert_eq!(query_info.get_disk_usage_intensity(), 0.32);
//         assert!((query_info.get_query_throughput_per_minute() - 16.67).abs() < 0.1);
//
//         assert!(query_info.has_slow_queries());
//         assert!(query_info.has_high_memory_queries());
//         assert!(query_info.has_long_running_queries());
//         assert!(query_info.has_recent_failures());
//         assert!(query_info.has_blocked_queries());
//         assert!(query_info.has_disk_intensive_queries());
//         assert!(query_info.has_recent_cancellations());
//
//         let load_level = query_info.get_query_load_level();
//         assert!(matches!(load_level, QueryLoadLevel::Moderate));
//
//         let performance_status = query_info.get_performance_status();
//         assert!(matches!(performance_status, QueryPerformanceStatus::Attention));
//
//         let memory_pressure = query_info.get_memory_pressure_level();
//         assert!(matches!(memory_pressure, MemoryPressureLevel::Moderate));
//
//         let efficiency = query_info.get_resource_efficiency();
//         assert!(efficiency > 0.0 && efficiency < 1.0);
//     }
//
//     #[test]
//     fn test_query_performance_classification() {
//         // Test healthy status
//         let mut healthy_query = ClickhouseQueryInfo::default();
//         healthy_query.running_queries = 10;
//         healthy_query.slow_queries = 0;
//         healthy_query.high_memory_queries = 0;
//         healthy_query.queries_last_hour = 500;
//         healthy_query.failed_queries_last_hour = 1; // 0.2% failure rate
//         healthy_query.queries_waiting_for_locks = 0;
//         healthy_query.cancelled_queries_last_hour = 0;
//
//         assert!(matches!(healthy_query.get_performance_status(), QueryPerformanceStatus::Healthy));
//
//         // Test critical status
//         let mut critical_query = ClickhouseQueryInfo::default();
//         critical_query.running_queries = 20;
//         critical_query.slow_queries = 12; // 60% slow rate
//         critical_query.high_memory_queries = 8; // 40% high memory rate
//         critical_query.queries_last_hour = 100;
//         critical_query.failed_queries_last_hour = 25; // 25% failure rate
//         critical_query.queries_waiting_for_locks = 10; // 50% blocked rate
//
//         assert!(matches!(critical_query.get_performance_status(), QueryPerformanceStatus::Critical));
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhouseQueryInfo;

    #[test]
    fn query_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhouseQueryInfo::default();
        assert!(!ClickhouseQueryInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn query_detailed_gate_true_for_failures() {
        let info = ClickhouseQueryInfo {
            failed_queries_last_hour: 1,
            ..ClickhouseQueryInfo::default()
        };
        assert!(ClickhouseQueryInfo::should_collect_detailed_metrics(&info));
    }
}
