use crate::api::lib::QueryUnpagedInput;
use crate::metadata::capabilities::CASSANDRA_HAS_VIRTUAL_TABLES;
use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DEFAULT_QUERY_TIMEOUT, get_string, get_u64_or_zero, map_rows, run_optional_query};

/// Cassandra thread pool information and performance monitoring.
///
/// On Cassandra 4.0+ thread pool metrics are collected via the
/// `system_views.thread_pools` virtual table, which is queryable through
/// ordinary CQL.  On Cassandra 3.x (no virtual tables) this collector
/// returns default/zero values because JMX is needed for full visibility on
/// those versions.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolInfo {
    /// Total number of thread pools monitored
    pub total_thread_pools: u64,
    /// Number of thread pools with high utilization
    pub high_utilization_pools: u64,
    /// Number of thread pools with queue backlogs
    pub pools_with_backlogs: u64,
    /// Number of thread pools showing dropped tasks
    pub pools_with_dropped_tasks: u64,
    /// Total active threads across all pools
    pub total_active_threads: u64,
    /// Total pending tasks across all pools
    pub total_pending_tasks: u64,
    /// Total completed tasks across all pools
    pub total_completed_tasks: u64,
    /// Total dropped tasks across all pools
    pub total_dropped_tasks: u64,
    /// Average thread pool utilization percentage
    pub avg_utilization_pct: f64,
    /// Peak thread pool utilization percentage
    pub peak_utilization_pct: f64,
    /// Overall thread pool health score (0-100)
    pub overall_health_score: f64,
    /// Detailed information for each thread pool
    pub thread_pool_details: Vec<CassandraThreadPoolDetail>,
    /// Thread pool performance metrics
    pub performance_metrics: CassandraThreadPoolPerformanceMetrics,
    /// Thread pool resource utilization
    pub resource_utilization: CassandraThreadPoolResourceUtilization,
    /// Thread pool health and alerts
    pub health_alerts: CassandraThreadPoolHealthAlerts,
    /// Thread pool configuration recommendations
    pub configuration_recommendations: Vec<CassandraThreadPoolRecommendation>,
}

/// Detailed information about a specific thread pool
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolDetail {
    /// Thread pool name
    pub pool_name: String,
    /// Pool type category (REQUEST, INTERNAL, TRANSPORT)
    pub pool_category: String,
    /// Current number of active threads
    pub active_threads: u64,
    /// Maximum configured threads
    pub max_threads: u64,
    /// Core number of threads
    pub core_threads: u64,
    /// Current pending tasks in queue
    pub pending_tasks: u64,
    /// Maximum queue size
    pub max_queue_size: u64,
    /// Total completed tasks since startup
    pub completed_tasks: u64,
    /// Total dropped tasks since startup
    pub dropped_tasks: u64,
    /// Currently blocked tasks
    pub blocked_tasks: u64,
    /// Thread pool utilization percentage
    pub utilization_pct: f64,
    /// Queue utilization percentage
    pub queue_utilization_pct: f64,
    /// Average task execution time (ms)
    pub avg_task_duration_ms: f64,
    /// Peak task execution time (ms)
    pub peak_task_duration_ms: f64,
    /// Tasks per second throughput
    pub tasks_per_second: f64,
    /// Drop rate percentage
    pub drop_rate_pct: f64,
    /// Pool health status (HEALTHY, WARNING, CRITICAL)
    pub health_status: String,
    /// Performance score (0-100)
    pub performance_score: f64,
    /// Configuration parameters
    pub configuration: CassandraThreadPoolConfiguration,
    /// Recent performance history
    pub recent_metrics: Vec<CassandraThreadPoolHistoricalMetric>,
    /// Alert conditions met
    pub active_alerts: Vec<String>,
}

/// Thread pool configuration parameters
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolConfiguration {
    /// Core pool size
    pub core_pool_size: u64,
    /// Maximum pool size
    pub maximum_pool_size: u64,
    /// Keep alive time (seconds)
    pub keep_alive_time_seconds: u64,
    /// Queue type (BOUNDED, UNBOUNDED, SYNCHRONOUS)
    pub queue_type: String,
    /// Queue capacity
    pub queue_capacity: u64,
    /// Thread priority
    pub thread_priority: u64,
    /// Allow core thread timeout
    pub allow_core_thread_timeout: bool,
    /// Rejection policy
    pub rejection_policy: String,
    /// Is user-configured
    pub is_user_configured: bool,
}

/// Historical performance metric for a thread pool
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolHistoricalMetric {
    /// Timestamp of the metric
    pub timestamp: String,
    /// Active threads at this time
    pub active_threads: u64,
    /// Pending tasks at this time
    pub pending_tasks: u64,
    /// Utilization percentage at this time
    pub utilization_pct: f64,
    /// Throughput at this time (tasks/sec)
    pub throughput: f64,
}

/// Overall thread pool performance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolPerformanceMetrics {
    /// Total throughput across all pools (tasks/sec)
    pub total_throughput: f64,
    /// Average task latency across all pools (ms)
    pub avg_task_latency_ms: f64,
    /// 95th percentile task latency (ms)
    pub p95_task_latency_ms: f64,
    /// 99th percentile task latency (ms)
    pub p99_task_latency_ms: f64,
    /// Task completion rate (percentage)
    pub task_completion_rate_pct: f64,
    /// Thread efficiency score (0-100)
    pub thread_efficiency_score: f64,
    /// Queue efficiency score (0-100)
    pub queue_efficiency_score: f64,
    /// Resource contention level (0-100)
    pub contention_level: f64,
    /// Thread pool saturation percentage
    pub saturation_pct: f64,
    /// Context switch rate (switches/sec)
    pub context_switch_rate: f64,
}

/// Thread pool resource utilization metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolResourceUtilization {
    /// CPU utilization by thread pools (percentage)
    pub cpu_utilization_pct: f64,
    /// Memory usage by thread pools (MB)
    pub memory_usage_mb: f64,
    /// Thread stack memory usage (MB)
    pub thread_stack_memory_mb: f64,
    /// Native memory usage (MB)
    pub native_memory_mb: f64,
    /// GC pressure from thread activity
    pub gc_pressure_score: f64,
    /// Lock contention rate
    pub lock_contention_rate: f64,
    /// IO wait time percentage
    pub io_wait_pct: f64,
    /// Network utilization by pools (MB/s)
    pub network_utilization_mb_per_sec: f64,
    /// Resource efficiency score (0-100)
    pub resource_efficiency_score: f64,
}

/// Thread pool health alerts and warnings
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolHealthAlerts {
    /// Critical alerts requiring immediate attention
    pub critical_alerts: Vec<CassandraThreadPoolAlert>,
    /// Warning alerts for monitoring
    pub warning_alerts: Vec<CassandraThreadPoolAlert>,
    /// Informational alerts
    pub info_alerts: Vec<CassandraThreadPoolAlert>,
    /// Number of pools in critical state
    pub critical_pools: u64,
    /// Number of pools in warning state
    pub warning_pools: u64,
    /// Alert summary by category
    pub alert_summary: HashMap<String, u64>,
    /// Recent alert history
    pub recent_alert_history: Vec<CassandraThreadPoolAlertHistory>,
}

/// Individual thread pool alert
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolAlert {
    /// Alert severity (CRITICAL, WARNING, INFO)
    pub severity: String,
    /// Thread pool name
    pub pool_name: String,
    /// Alert type
    pub alert_type: String,
    /// Alert message
    pub message: String,
    /// Current value triggering alert
    pub current_value: f64,
    /// Threshold value
    pub threshold_value: f64,
    /// Alert timestamp
    pub timestamp: String,
    /// Duration alert has been active
    pub duration_minutes: f64,
    /// Recommended action
    pub recommended_action: String,
}

/// Historical alert information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolAlertHistory {
    /// Alert timestamp
    pub timestamp: String,
    /// Pool name
    pub pool_name: String,
    /// Alert type
    pub alert_type: String,
    /// Alert severity
    pub severity: String,
    /// Alert resolved or still active
    pub is_resolved: bool,
    /// Resolution timestamp
    pub resolved_at: Option<String>,
}

/// Configuration recommendation for thread pools
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolRecommendation {
    /// Thread pool name
    pub pool_name: String,
    /// Recommendation type
    pub recommendation_type: String,
    /// Current configuration value
    pub current_value: String,
    /// Recommended configuration value
    pub recommended_value: String,
    /// Justification for recommendation
    pub justification: String,
    /// Expected improvement
    pub expected_improvement: String,
    /// Implementation priority (HIGH, MEDIUM, LOW)
    pub priority: String,
    /// Potential risks of change
    pub risks: Vec<String>,
}

impl MetadataCollection for CassandraThreadPoolInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        // Thread pool metrics on Cassandra 4.0+ are fetched ad-hoc via
        // `run_optional_query` against `system_views.thread_pools`.
        // No pre-registered queries are needed here.
        HashMap::new()
    }

    fn description(&self) -> &'static str {
        "Cassandra thread pool metrics (via virtual tables on 4.0+)"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "threadpool"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl CassandraThreadPoolInfo {
    // Utilization thresholds used in health status derivation and analysis helpers.
    pub(crate) const HIGH_UTILIZATION_THRESHOLD: f64 = 80.0;
    pub(crate) const CRITICAL_UTILIZATION_THRESHOLD: f64 = 95.0;
    // Queue/drop thresholds are kept for analysis methods but not derivable from
    // virtual tables (no queue-size or per-second drop-rate columns).
    #[allow(dead_code)]
    pub(crate) const HIGH_QUEUE_THRESHOLD: f64 = 70.0;
    pub(crate) const CRITICAL_QUEUE_THRESHOLD: f64 = 90.0;
    #[allow(dead_code)]
    pub(crate) const HIGH_DROP_RATE_THRESHOLD: f64 = 1.0;
    pub(crate) const CRITICAL_DROP_RATE_THRESHOLD: f64 = 5.0;

    // Known Cassandra thread pool names and their categories, used to look up
    // `pool_category` when parsing virtual table rows.
    pub(crate) const THREAD_POOLS: &'static [(&'static str, &'static str)] = &[
        ("Native-Transport-Requests", "TRANSPORT"),
        ("ReadStage", "REQUEST"),
        ("MutationStage", "REQUEST"),
        ("ViewMutationStage", "REQUEST"),
        ("CounterMutationStage", "REQUEST"),
        ("ReadRepairStage", "REQUEST"),
        ("CompactionExecutor", "INTERNAL"),
        ("ValidationExecutor", "INTERNAL"),
        ("AntiEntropyStage", "INTERNAL"),
        ("GossipStage", "INTERNAL"),
        ("RequestResponseStage", "INTERNAL"),
        ("InternalResponseStage", "INTERNAL"),
        ("MemtableFlushWriter", "INTERNAL"),
        ("MemtablePostFlush", "INTERNAL"),
        ("PerDiskMemtableFlushWriter_0", "INTERNAL"),
        ("SecondaryIndexManagement", "INTERNAL"),
        ("HintsDispatcher", "INTERNAL"),
        ("BatchlogManager", "INTERNAL"),
        ("SamplingExecutor", "INTERNAL"),
    ];

    /// Look up the category for a pool name, defaulting to "UNKNOWN".
    fn pool_category(name: &str) -> &'static str {
        Self::THREAD_POOLS.iter().find(|(pool_name, _)| *pool_name == name).map(|(_, category)| *category).unwrap_or("UNKNOWN")
    }

    /// Derive a health status string from a utilization percentage.
    fn health_status_from_utilization(utilization_pct: f64) -> &'static str {
        if utilization_pct >= Self::CRITICAL_UTILIZATION_THRESHOLD {
            "CRITICAL"
        } else if utilization_pct >= Self::HIGH_UTILIZATION_THRESHOLD {
            "WARNING"
        } else {
            "HEALTHY"
        }
    }

    /// Parse a single virtual table row into a `CassandraThreadPoolDetail`.
    ///
    /// Returns `None` if the mandatory `name` field is absent so that
    /// `map_rows` can skip the malformed row silently.
    fn parse_virtual_table_row(row: &serde_json::Value) -> Option<CassandraThreadPoolDetail> {
        let pool_name = get_string(row, "name")?;

        let active_threads = get_u64_or_zero(row, "active_tasks");
        let max_threads = get_u64_or_zero(row, "active_tasks_limit");
        let pending_tasks = get_u64_or_zero(row, "pending_tasks");
        let completed_tasks = get_u64_or_zero(row, "completed_tasks");
        let blocked_tasks = get_u64_or_zero(row, "blocked_tasks");
        let blocked_tasks_all_time = get_u64_or_zero(row, "blocked_tasks_all_time");

        let utilization_pct = if max_threads > 0 {
            (active_threads as f64 / max_threads as f64) * 100.0
        } else {
            0.0
        };

        let health_status = Self::health_status_from_utilization(utilization_pct).to_string();
        let pool_category = Self::pool_category(&pool_name).to_string();

        Some(CassandraThreadPoolDetail {
            pool_name,
            pool_category,
            active_threads,
            max_threads,
            // core_threads is not exposed by the virtual table.
            core_threads: 0,
            pending_tasks,
            // max_queue_size is not exposed by the virtual table.
            max_queue_size: 0,
            completed_tasks,
            // dropped_tasks: closest available proxy is blocked_tasks_all_time.
            dropped_tasks: blocked_tasks_all_time,
            blocked_tasks,
            utilization_pct,
            // queue_utilization_pct: queue size is not exposed by the virtual table.
            queue_utilization_pct: 0.0,
            // Timing metrics are not available from virtual tables.
            avg_task_duration_ms: 0.0,
            peak_task_duration_ms: 0.0,
            tasks_per_second: 0.0,
            drop_rate_pct: 0.0,
            health_status,
            // Performance score not computable without timing data.
            performance_score: 0.0,
            configuration: CassandraThreadPoolConfiguration::default(),
            recent_metrics: Vec::new(),
            active_alerts: Vec::new(),
        })
    }

    /// Aggregate a list of pool details into the top-level `CassandraThreadPoolInfo` fields.
    fn aggregate(details: Vec<CassandraThreadPoolDetail>) -> Self {
        let total_thread_pools = details.len() as u64;
        let total_active_threads: u64 = details.iter().map(|d| d.active_threads).sum();
        let total_pending_tasks: u64 = details.iter().map(|d| d.pending_tasks).sum();
        let total_completed_tasks: u64 = details.iter().map(|d| d.completed_tasks).sum();
        let total_dropped_tasks: u64 = details.iter().map(|d| d.dropped_tasks).sum();

        let high_utilization_pools = details.iter().filter(|d| d.utilization_pct > Self::HIGH_UTILIZATION_THRESHOLD).count() as u64;
        let pools_with_backlogs = details.iter().filter(|d| d.pending_tasks > 0).count() as u64;
        let pools_with_dropped_tasks = details.iter().filter(|d| d.dropped_tasks > 0).count() as u64;

        let avg_utilization_pct = if total_thread_pools > 0 {
            details.iter().map(|d| d.utilization_pct).sum::<f64>() / total_thread_pools as f64
        } else {
            0.0
        };

        let peak_utilization_pct = details.iter().map(|d| d.utilization_pct).fold(0.0_f64, f64::max);

        // Simple health score: start at 100 and apply penalties.
        // -2 per high-utilization pool, -5 per pool with drops.
        let penalty = (high_utilization_pools as f64 * 2.0) + (pools_with_dropped_tasks as f64 * 5.0);
        let overall_health_score = (100.0_f64 - penalty).max(0.0);

        Self {
            total_thread_pools,
            high_utilization_pools,
            pools_with_backlogs,
            pools_with_dropped_tasks,
            total_active_threads,
            total_pending_tasks,
            total_completed_tasks,
            total_dropped_tasks,
            avg_utilization_pct,
            peak_utilization_pct,
            overall_health_score,
            thread_pool_details: details,
            ..Self::default()
        }
    }

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        if !capabilities.has(&CASSANDRA_HAS_VIRTUAL_TABLES) {
            // Cassandra 3.x: virtual tables not available; return empty defaults.
            return Ok(Self::default());
        }

        let result = run_optional_query(
            "SELECT name, active_tasks, active_tasks_limit, pending_tasks, \
             completed_tasks, blocked_tasks, blocked_tasks_all_time \
             FROM system_views.thread_pools",
            context,
            DEFAULT_QUERY_TIMEOUT,
            "system_views.thread_pools",
        )
        .await;

        let Some(data) = result else {
            // Query soft-failed (e.g. permission denied or transient error);
            // return defaults rather than propagating an error.
            return Ok(Self::default());
        };

        let details = map_rows(&data, Self::parse_virtual_table_row);

        Ok(Self::aggregate(details))
    }
}

impl CassandraThreadPoolInfo {
    /// Checks if there are critical thread pool issues requiring immediate attention.
    pub fn has_critical_thread_pool_issues(&self) -> bool {
        self.overall_health_score < 60.0
            || self.health_alerts.critical_pools > 0
            || self.pools_with_dropped_tasks > 0
            || self.peak_utilization_pct > Self::CRITICAL_UTILIZATION_THRESHOLD
    }

    /// Gets thread pools that need immediate attention.
    pub fn thread_pools_needing_attention(&self) -> Vec<&CassandraThreadPoolDetail> {
        self.thread_pool_details
            .iter()
            .filter(|pool| {
                pool.health_status == "CRITICAL"
                    || pool.utilization_pct > Self::CRITICAL_UTILIZATION_THRESHOLD
                    || pool.queue_utilization_pct > Self::CRITICAL_QUEUE_THRESHOLD
                    || pool.drop_rate_pct > Self::CRITICAL_DROP_RATE_THRESHOLD
            })
            .collect()
    }

    /// Gets thread pools with performance issues.
    pub fn thread_pools_with_performance_issues(&self) -> Vec<&CassandraThreadPoolDetail> {
        self.thread_pool_details
            .iter()
            .filter(|pool| pool.performance_score < 70.0 || pool.avg_task_duration_ms > 100.0 || !pool.active_alerts.is_empty())
            .collect()
    }

    /// Gets the most utilized thread pools.
    pub fn most_utilized_thread_pools(&self, limit: usize) -> Vec<&CassandraThreadPoolDetail> {
        let mut pools = self.thread_pool_details.iter().collect::<Vec<_>>();
        pools.sort_by(|a, b| b.utilization_pct.partial_cmp(&a.utilization_pct).unwrap_or(std::cmp::Ordering::Equal));
        pools.into_iter().take(limit).collect()
    }

    /// Gets thread pools with the highest throughput.
    pub fn highest_throughput_pools(&self, limit: usize) -> Vec<&CassandraThreadPoolDetail> {
        let mut pools = self.thread_pool_details.iter().collect::<Vec<_>>();
        pools.sort_by(|a, b| b.tasks_per_second.partial_cmp(&a.tasks_per_second).unwrap_or(std::cmp::Ordering::Equal));
        pools.into_iter().take(limit).collect()
    }

    /// Gets overall thread pool health rating (A-F scale).
    pub fn thread_pool_health_rating(&self) -> String {
        match self.overall_health_score {
            s if s >= 90.0 => "A".to_string(),
            s if s >= 80.0 => "B".to_string(),
            s if s >= 70.0 => "C".to_string(),
            s if s >= 60.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Calculates thread pool efficiency score.
    pub fn thread_pool_efficiency_score(&self) -> f64 {
        let resource_efficiency = self.resource_utilization.resource_efficiency_score;
        let performance_efficiency =
            (self.performance_metrics.thread_efficiency_score + self.performance_metrics.queue_efficiency_score) / 2.0;

        (resource_efficiency + performance_efficiency) / 2.0
    }

    /// Gets recommended thread pool optimization actions.
    pub fn get_thread_pool_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.has_critical_thread_pool_issues() {
            recommendations.push("CRITICAL: Address thread pool bottlenecks immediately".to_string());
        }

        if self.health_alerts.critical_pools > 0 {
            recommendations.push(format!(
                "{} thread pools in critical state - immediate intervention required",
                self.health_alerts.critical_pools
            ));
        }

        if self.pools_with_dropped_tasks > 0 {
            recommendations.push(format!("{} thread pools dropping tasks - increase capacity", self.pools_with_dropped_tasks));
        }

        if self.avg_utilization_pct > 80.0 {
            recommendations.push("High average thread pool utilization - consider capacity planning".to_string());
        }

        if self.performance_metrics.avg_task_latency_ms > 50.0 {
            recommendations.push("High task latencies detected - investigate blocking operations".to_string());
        }

        if self.resource_utilization.lock_contention_rate > 50.0 {
            recommendations.push("High lock contention detected - review synchronization patterns".to_string());
        }

        if self.performance_metrics.context_switch_rate > 10000.0 {
            recommendations.push("High context switch rate - consider thread pool sizing".to_string());
        }

        if !self.configuration_recommendations.is_empty() {
            recommendations.push(format!("{} configuration optimizations available", self.configuration_recommendations.len()));
        }

        if recommendations.is_empty() {
            recommendations.push("Thread pool configuration appears optimal - continue monitoring".to_string());
        }

        recommendations
    }

    /// Gets thread pool distribution statistics.
    pub fn get_thread_pool_distribution_stats(&self) -> CassandraThreadPoolDistributionStats {
        CassandraThreadPoolDistributionStats {
            pools_by_category: self.get_pools_by_category(),
            pools_by_utilization_ranges: self.get_pools_by_utilization_ranges(),
            pools_by_health_status: self.get_pools_by_health_status(),
            alert_distribution: self.health_alerts.alert_summary.clone(),
        }
    }

    fn get_pools_by_category(&self) -> HashMap<String, u64> {
        let mut category_counts: HashMap<String, u64> = HashMap::new();
        for pool in &self.thread_pool_details {
            *category_counts.entry(pool.pool_category.clone()).or_insert(0) += 1;
        }
        category_counts
    }

    fn get_pools_by_utilization_ranges(&self) -> HashMap<String, u64> {
        let mut ranges = HashMap::new();
        ranges.insert("0-50%".to_string(), 0);
        ranges.insert("51-80%".to_string(), 0);
        ranges.insert("81-95%".to_string(), 0);
        ranges.insert("96-100%".to_string(), 0);

        for pool in &self.thread_pool_details {
            let range_key = match pool.utilization_pct {
                u if u <= 50.0 => "0-50%",
                u if u <= 80.0 => "51-80%",
                u if u <= 95.0 => "81-95%",
                _ => "96-100%",
            };
            if let Some(count) = ranges.get_mut(range_key) {
                *count += 1;
            }
        }

        ranges
    }

    fn get_pools_by_health_status(&self) -> HashMap<String, u64> {
        let mut status_counts: HashMap<String, u64> = HashMap::new();
        for pool in &self.thread_pool_details {
            *status_counts.entry(pool.health_status.clone()).or_insert(0) += 1;
        }
        status_counts
    }

    /// Gets summary for reporting.
    pub fn get_thread_pool_summary(&self) -> CassandraThreadPoolSummary {
        CassandraThreadPoolSummary {
            total_thread_pools: self.total_thread_pools,
            total_active_threads: self.total_active_threads,
            avg_utilization_pct: self.avg_utilization_pct,
            peak_utilization_pct: self.peak_utilization_pct,
            pools_with_issues: self.high_utilization_pools + self.pools_with_backlogs + self.pools_with_dropped_tasks,
            health_score: self.overall_health_score,
            health_rating: self.thread_pool_health_rating(),
            efficiency_score: self.thread_pool_efficiency_score(),
            critical_alerts: self.health_alerts.critical_alerts.len() as u64,
            total_dropped_tasks: self.total_dropped_tasks,
            has_critical_issues: self.has_critical_thread_pool_issues(),
        }
    }
}

/// Thread pool distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraThreadPoolDistributionStats {
    pub pools_by_category: HashMap<String, u64>,
    pub pools_by_utilization_ranges: HashMap<String, u64>,
    pub pools_by_health_status: HashMap<String, u64>,
    pub alert_distribution: HashMap<String, u64>,
}

/// Summary statistics for thread pool information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraThreadPoolSummary {
    pub total_thread_pools: u64,
    pub total_active_threads: u64,
    pub avg_utilization_pct: f64,
    pub peak_utilization_pct: f64,
    pub pools_with_issues: u64,
    pub health_score: f64,
    pub health_rating: String,
    pub efficiency_score: f64,
    pub critical_alerts: u64,
    pub total_dropped_tasks: u64,
    pub has_critical_issues: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_critical_issues_detection() {
        let mut thread_pool_info = CassandraThreadPoolInfo { overall_health_score: 80.0, ..Default::default() };

        // No issues
        thread_pool_info.health_alerts.critical_pools = 0;
        thread_pool_info.pools_with_dropped_tasks = 0;
        thread_pool_info.peak_utilization_pct = 70.0;
        assert!(!thread_pool_info.has_critical_thread_pool_issues());

        // Low health score
        thread_pool_info.overall_health_score = 50.0;
        assert!(thread_pool_info.has_critical_thread_pool_issues());

        // Reset and test critical pools
        thread_pool_info.overall_health_score = 80.0;
        thread_pool_info.health_alerts.critical_pools = 1;
        assert!(thread_pool_info.has_critical_thread_pool_issues());

        // Reset and test dropped tasks
        thread_pool_info.health_alerts.critical_pools = 0;
        thread_pool_info.pools_with_dropped_tasks = 1;
        assert!(thread_pool_info.has_critical_thread_pool_issues());

        // Reset and test peak utilization
        thread_pool_info.pools_with_dropped_tasks = 0;
        thread_pool_info.peak_utilization_pct = 98.0;
        assert!(thread_pool_info.has_critical_thread_pool_issues());
    }

    #[test]
    fn test_health_rating() {
        let mut thread_pool_info = CassandraThreadPoolInfo { overall_health_score: 95.0, ..Default::default() };

        assert_eq!(thread_pool_info.thread_pool_health_rating(), "A");

        thread_pool_info.overall_health_score = 85.0;
        assert_eq!(thread_pool_info.thread_pool_health_rating(), "B");

        thread_pool_info.overall_health_score = 75.0;
        assert_eq!(thread_pool_info.thread_pool_health_rating(), "C");

        thread_pool_info.overall_health_score = 65.0;
        assert_eq!(thread_pool_info.thread_pool_health_rating(), "D");

        thread_pool_info.overall_health_score = 45.0;
        assert_eq!(thread_pool_info.thread_pool_health_rating(), "F");
    }

    #[test]
    fn test_efficiency_score_calculation() {
        let mut thread_pool_info = CassandraThreadPoolInfo::default();

        thread_pool_info.resource_utilization.resource_efficiency_score = 80.0;
        thread_pool_info.performance_metrics.thread_efficiency_score = 85.0;
        thread_pool_info.performance_metrics.queue_efficiency_score = 75.0;

        let efficiency = thread_pool_info.thread_pool_efficiency_score();
        assert_eq!(efficiency, 80.0); // (80 + (85 + 75) / 2) / 2 = 80
    }

    #[test]
    fn test_utilization_ranges() {
        let thread_pool_info = CassandraThreadPoolInfo {
            thread_pool_details: vec![
                CassandraThreadPoolDetail {
                    pool_name: "low_util".to_string(),
                    pool_category: "REQUEST".to_string(),
                    active_threads: 10,
                    max_threads: 100,
                    core_threads: 50,
                    pending_tasks: 5,
                    max_queue_size: 1000,
                    completed_tasks: 10000,
                    dropped_tasks: 0,
                    blocked_tasks: 0,
                    utilization_pct: 30.0, // 0-50% range
                    queue_utilization_pct: 5.0,
                    avg_task_duration_ms: 5.0,
                    peak_task_duration_ms: 50.0,
                    tasks_per_second: 100.0,
                    drop_rate_pct: 0.0,
                    health_status: "HEALTHY".to_string(),
                    performance_score: 90.0,
                    configuration: CassandraThreadPoolConfiguration::default(),
                    recent_metrics: vec![],
                    active_alerts: vec![],
                },
                CassandraThreadPoolDetail {
                    pool_name: "high_util".to_string(),
                    pool_category: "REQUEST".to_string(),
                    active_threads: 85,
                    max_threads: 100,
                    core_threads: 50,
                    pending_tasks: 50,
                    max_queue_size: 1000,
                    completed_tasks: 15000,
                    dropped_tasks: 10,
                    blocked_tasks: 5,
                    utilization_pct: 85.0, // 81-95% range
                    queue_utilization_pct: 15.0,
                    avg_task_duration_ms: 15.0,
                    peak_task_duration_ms: 150.0,
                    tasks_per_second: 80.0,
                    drop_rate_pct: 0.1,
                    health_status: "WARNING".to_string(),
                    performance_score: 70.0,
                    configuration: CassandraThreadPoolConfiguration::default(),
                    recent_metrics: vec![],
                    active_alerts: vec!["High utilization".to_string()],
                },
            ],
            ..Default::default()
        };

        let ranges = thread_pool_info.get_pools_by_utilization_ranges();
        assert_eq!(ranges.get("0-50%"), Some(&1));
        assert_eq!(ranges.get("81-95%"), Some(&1));
        assert_eq!(ranges.get("51-80%"), Some(&0));
        assert_eq!(ranges.get("96-100%"), Some(&0));
    }

    #[test]
    fn test_alert_generation_thresholds() {
        let critical_utilization = std::hint::black_box(CassandraThreadPoolInfo::CRITICAL_UTILIZATION_THRESHOLD);
        let high_utilization = std::hint::black_box(CassandraThreadPoolInfo::HIGH_UTILIZATION_THRESHOLD);
        let high_queue = std::hint::black_box(CassandraThreadPoolInfo::HIGH_QUEUE_THRESHOLD);
        let critical_queue = std::hint::black_box(CassandraThreadPoolInfo::CRITICAL_QUEUE_THRESHOLD);
        let high_drop_rate = std::hint::black_box(CassandraThreadPoolInfo::HIGH_DROP_RATE_THRESHOLD);
        let critical_drop_rate = std::hint::black_box(CassandraThreadPoolInfo::CRITICAL_DROP_RATE_THRESHOLD);

        assert!(90.0 < critical_utilization);
        assert!(high_utilization < critical_utilization);
        assert!(high_queue < critical_queue);
        assert!(high_drop_rate < critical_drop_rate);
    }

    #[test]
    fn test_thread_pool_categories() {
        let categories: Vec<&str> = CassandraThreadPoolInfo::THREAD_POOLS.iter().map(|(_, category)| *category).collect();

        assert!(categories.contains(&"TRANSPORT"));
        assert!(categories.contains(&"REQUEST"));
        assert!(categories.contains(&"INTERNAL"));
    }

    #[test]
    fn test_virtual_table_row_parsing() {
        let row = json!({
            "name": "ReadStage",
            "active_tasks": 4_i64,
            "active_tasks_limit": 32_i64,
            "pending_tasks": 2_i64,
            "completed_tasks": 100000_i64,
            "blocked_tasks": 0_i64,
            "blocked_tasks_all_time": 5_i64
        });

        let detail = CassandraThreadPoolInfo::parse_virtual_table_row(&row).expect("should parse a well-formed row");

        assert_eq!(detail.pool_name, "ReadStage");
        assert_eq!(detail.pool_category, "REQUEST");
        assert_eq!(detail.active_threads, 4);
        assert_eq!(detail.max_threads, 32);
        assert_eq!(detail.pending_tasks, 2);
        assert_eq!(detail.completed_tasks, 100000);
        assert_eq!(detail.blocked_tasks, 0);
        assert_eq!(detail.dropped_tasks, 5); // mapped from blocked_tasks_all_time
        assert!((detail.utilization_pct - 12.5).abs() < f64::EPSILON); // 4/32*100
        assert_eq!(detail.health_status, "HEALTHY");
        assert_eq!(detail.queue_utilization_pct, 0.0);
    }

    #[test]
    fn test_virtual_table_row_parsing_missing_name() {
        // A row without "name" must be skipped by map_rows.
        let row = json!({
            "active_tasks": 4_i64,
            "active_tasks_limit": 32_i64,
            "pending_tasks": 0_i64,
            "completed_tasks": 0_i64,
            "blocked_tasks": 0_i64,
            "blocked_tasks_all_time": 0_i64
        });

        let result = CassandraThreadPoolInfo::parse_virtual_table_row(&row);
        assert!(result.is_none());
    }

    #[test]
    fn test_virtual_table_row_div_by_zero_guard() {
        // active_tasks_limit = 0 must not panic and must yield 0% utilization.
        let row = json!({
            "name": "SomePool",
            "active_tasks": 10_i64,
            "active_tasks_limit": 0_i64,
            "pending_tasks": 0_i64,
            "completed_tasks": 0_i64,
            "blocked_tasks": 0_i64,
            "blocked_tasks_all_time": 0_i64
        });

        let detail = CassandraThreadPoolInfo::parse_virtual_table_row(&row).expect("should parse even with zero limit");

        assert_eq!(detail.utilization_pct, 0.0);
        assert_eq!(detail.health_status, "HEALTHY");
    }

    #[test]
    fn test_aggregate_calculations() {
        // Three pools: one healthy, one high-utilization, one with drops.
        let data = json!([
            {
                "name": "ReadStage",
                "active_tasks": 4_i64,
                "active_tasks_limit": 32_i64,
                "pending_tasks": 0_i64,
                "completed_tasks": 50000_i64,
                "blocked_tasks": 0_i64,
                "blocked_tasks_all_time": 0_i64
            },
            {
                "name": "MutationStage",
                "active_tasks": 28_i64,
                "active_tasks_limit": 32_i64,
                "pending_tasks": 15_i64,
                "completed_tasks": 200000_i64,
                "blocked_tasks": 3_i64,
                "blocked_tasks_all_time": 10_i64
            },
            {
                "name": "Native-Transport-Requests",
                "active_tasks": 10_i64,
                "active_tasks_limit": 128_i64,
                "pending_tasks": 0_i64,
                "completed_tasks": 999000_i64,
                "blocked_tasks": 0_i64,
                "blocked_tasks_all_time": 0_i64
            }
        ]);

        let details = map_rows(&data, CassandraThreadPoolInfo::parse_virtual_table_row);
        let info = CassandraThreadPoolInfo::aggregate(details);

        assert_eq!(info.total_thread_pools, 3);
        assert_eq!(info.total_active_threads, 4 + 28 + 10);
        assert_eq!(info.total_pending_tasks, 15);
        assert_eq!(info.total_completed_tasks, 50000 + 200000 + 999000);
        // dropped_tasks mapped from blocked_tasks_all_time: 0 + 10 + 0 = 10
        assert_eq!(info.total_dropped_tasks, 10);

        // MutationStage: 28/32 = 87.5%, above HIGH_UTILIZATION_THRESHOLD (80%)
        assert_eq!(info.high_utilization_pools, 1);

        // pools_with_backlogs: MutationStage has 15 pending
        assert_eq!(info.pools_with_backlogs, 1);

        // pools_with_dropped_tasks: MutationStage has dropped_tasks = 10
        assert_eq!(info.pools_with_dropped_tasks, 1);

        // avg_utilization_pct: (12.5 + 87.5 + 7.8125) / 3 ≈ 35.94
        assert!(info.avg_utilization_pct > 0.0);

        // peak should be ~87.5 (MutationStage)
        assert!((info.peak_utilization_pct - 87.5).abs() < 0.01);

        // health_score: 100 - (1*2 + 1*5) = 93
        assert!((info.overall_health_score - 93.0).abs() < f64::EPSILON);

        // Categories assigned from THREAD_POOLS lookup
        let mutation = info.thread_pool_details.iter().find(|d| d.pool_name == "MutationStage").unwrap();
        assert_eq!(mutation.pool_category, "REQUEST");
        assert_eq!(mutation.health_status, "WARNING");

        let transport = info.thread_pool_details.iter().find(|d| d.pool_name == "Native-Transport-Requests").unwrap();
        assert_eq!(transport.pool_category, "TRANSPORT");
    }

    #[test]
    fn test_aggregate_empty_pools() {
        let info = CassandraThreadPoolInfo::aggregate(Vec::new());
        assert_eq!(info.total_thread_pools, 0);
        assert_eq!(info.avg_utilization_pct, 0.0);
        assert_eq!(info.peak_utilization_pct, 0.0);
        assert_eq!(info.overall_health_score, 100.0);
    }

    #[test]
    fn test_unknown_pool_category() {
        let row = json!({
            "name": "SomeNewPool",
            "active_tasks": 1_i64,
            "active_tasks_limit": 10_i64,
            "pending_tasks": 0_i64,
            "completed_tasks": 0_i64,
            "blocked_tasks": 0_i64,
            "blocked_tasks_all_time": 0_i64
        });

        let detail = CassandraThreadPoolInfo::parse_virtual_table_row(&row).unwrap();
        assert_eq!(detail.pool_category, "UNKNOWN");
    }

    #[test]
    fn test_health_status_thresholds() {
        // Below HIGH_UTILIZATION_THRESHOLD → HEALTHY
        assert_eq!(CassandraThreadPoolInfo::health_status_from_utilization(50.0), "HEALTHY");
        // At HIGH_UTILIZATION_THRESHOLD → WARNING
        assert_eq!(CassandraThreadPoolInfo::health_status_from_utilization(80.0), "WARNING");
        // Between thresholds → WARNING
        assert_eq!(CassandraThreadPoolInfo::health_status_from_utilization(90.0), "WARNING");
        // At CRITICAL_UTILIZATION_THRESHOLD → CRITICAL
        assert_eq!(CassandraThreadPoolInfo::health_status_from_utilization(95.0), "CRITICAL");
        // Above CRITICAL_UTILIZATION_THRESHOLD → CRITICAL
        assert_eq!(CassandraThreadPoolInfo::health_status_from_utilization(99.0), "CRITICAL");
    }
}
