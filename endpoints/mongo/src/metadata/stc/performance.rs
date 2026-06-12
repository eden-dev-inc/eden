use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, execute_admin_command_as_profiled, fetch};

/// MongoDB Performance statistics and comprehensive metrics
///
/// Aggregates multiple performance dimensions including query performance,
/// index effectiveness, resource utilization, and operational bottlenecks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPerformanceStats {
    /// Query performance metrics
    pub query_performance: MongoQueryPerformance,
    /// Index usage and effectiveness metrics
    pub index_performance: MongoIndexPerformance,
    /// Resource utilization metrics (CPU, memory, disk)
    pub resource_utilization: MongoResourceUtilization,
    /// Operation latency and throughput metrics
    pub operation_metrics: MongoOperationMetrics,
    /// Lock contention and concurrency metrics
    pub concurrency_metrics: MongoConcurrencyMetrics,
    /// Storage engine performance metrics
    pub storage_performance: MongoStoragePerformance,
    /// Cache performance and memory efficiency
    pub cache_performance: MongoCachePerformance,
    /// Connection and network performance
    pub connection_performance: MongoConnectionPerformance,
    /// Overall performance health score (0.0 to 1.0)
    pub overall_performance_score: f64,
    /// Performance trend indicator (-1.0 to 1.0, negative = degrading)
    pub performance_trend: f64,
    /// Timestamp of performance measurement
    pub measurement_timestamp: DateTimeWrapper,
    /// Detailed metrics collected only when performance issues are detected
    pub detailed_metrics: Option<MongoPerformanceDetailedMetrics>,
}

/// Query performance analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoQueryPerformance {
    /// Average query execution time (milliseconds)
    pub avg_query_time_ms: f64,
    /// 95th percentile query time (milliseconds)
    pub p95_query_time_ms: f64,
    /// 99th percentile query time (milliseconds)
    pub p99_query_time_ms: f64,
    /// Number of slow queries (>100ms)
    pub slow_query_count: u64,
    /// Queries per second
    pub queries_per_second: f64,
    /// Collection scan ratio (percentage of queries using collection scans)
    pub collection_scan_ratio: f64,
    /// Query cache hit ratio
    pub query_cache_hit_ratio: f64,
    /// Average documents examined per query
    pub avg_docs_examined_per_query: f64,
    /// Average documents returned per query
    pub avg_docs_returned_per_query: f64,
    /// Query efficiency score (returned/examined ratio)
    pub query_efficiency_score: f64,
    /// Most expensive queries count
    pub expensive_queries_count: u64,
}

/// Index performance and effectiveness
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexPerformance {
    /// Total number of indexes across all collections
    pub total_index_count: u32,
    /// Index hit ratio (percentage of queries using indexes)
    pub index_hit_ratio: f64,
    /// Unused indexes count
    pub unused_indexes_count: u32,
    /// Average index size (bytes)
    pub avg_index_size_bytes: u64,
    /// Total index size (bytes)
    pub total_index_size_bytes: u64,
    /// Index-to-data size ratio
    pub index_to_data_ratio: f64,
    /// Index maintenance overhead percentage
    pub index_maintenance_overhead: f64,
    /// Duplicate/redundant indexes count
    pub redundant_indexes_count: u32,
    /// Index fragmentation percentage
    pub index_fragmentation_percentage: f64,
    /// Most accessed indexes
    pub most_used_indexes: Vec<String>,
    /// Index efficiency score
    pub index_efficiency_score: f64,
}

/// Resource utilization metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoResourceUtilization {
    /// CPU utilization percentage
    pub cpu_utilization_percentage: f64,
    /// Memory utilization percentage
    pub memory_utilization_percentage: f64,
    /// Disk I/O utilization percentage
    pub disk_io_utilization_percentage: f64,
    /// Network utilization percentage
    pub network_utilization_percentage: f64,
    /// Available memory (bytes)
    pub available_memory_bytes: u64,
    /// Total memory (bytes)
    pub total_memory_bytes: u64,
    /// Memory pressure indicators
    pub memory_pressure_score: f64,
    /// Disk space utilization percentage
    pub disk_space_utilization_percentage: f64,
    /// I/O wait time percentage
    pub io_wait_percentage: f64,
    /// Resource contention score
    pub resource_contention_score: f64,
}

/// Operation metrics and throughput
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOperationMetrics {
    /// Total operations per second
    pub total_ops_per_second: f64,
    /// Read operations per second
    pub read_ops_per_second: f64,
    /// Write operations per second
    pub write_ops_per_second: f64,
    /// Update operations per second
    pub update_ops_per_second: f64,
    /// Delete operations per second
    pub delete_ops_per_second: f64,
    /// Insert operations per second
    pub insert_ops_per_second: f64,
    /// Command operations per second
    pub command_ops_per_second: f64,
    /// Average operation latency (milliseconds)
    pub avg_operation_latency_ms: f64,
    /// Operation queue depth
    pub operation_queue_depth: u32,
    /// Failed operations per second
    pub failed_ops_per_second: f64,
    /// Operation success rate percentage
    pub operation_success_rate: f64,
}

/// Concurrency and lock metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConcurrencyMetrics {
    /// Active read operations
    pub active_read_operations: u32,
    /// Active write operations
    pub active_write_operations: u32,
    /// Lock contention events per second
    pub lock_contention_per_second: f64,
    /// Average lock wait time (milliseconds)
    pub avg_lock_wait_time_ms: f64,
    /// Read lock percentage
    pub read_lock_percentage: f64,
    /// Write lock percentage
    pub write_lock_percentage: f64,
    /// Lock escalation events
    pub lock_escalation_events: u64,
    /// Deadlock count
    pub deadlock_count: u64,
    /// Concurrent connections
    pub concurrent_connections: u32,
    /// Thread pool utilization percentage
    pub thread_pool_utilization: f64,
    /// Concurrency efficiency score
    pub concurrency_efficiency_score: f64,
}

/// Storage engine performance
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoStoragePerformance {
    /// Storage engine type
    pub storage_engine: String,
    /// Read throughput (bytes per second)
    pub read_throughput_bps: f64,
    /// Write throughput (bytes per second)
    pub write_throughput_bps: f64,
    /// Average read latency (milliseconds)
    pub avg_read_latency_ms: f64,
    /// Average write latency (milliseconds)
    pub avg_write_latency_ms: f64,
    /// Disk read IOPS
    pub disk_read_iops: f64,
    /// Disk write IOPS
    pub disk_write_iops: f64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Compaction overhead percentage
    pub compaction_overhead_percentage: f64,
    /// Journal flush time (milliseconds)
    pub journal_flush_time_ms: f64,
    /// Checkpoint frequency per hour
    pub checkpoint_frequency_per_hour: f64,
    /// Storage fragmentation percentage
    pub storage_fragmentation_percentage: f64,
}

/// Cache performance and memory efficiency
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCachePerformance {
    /// WiredTiger cache hit ratio
    pub cache_hit_ratio: f64,
    /// Cache size (bytes)
    pub cache_size_bytes: u64,
    /// Cache utilization percentage
    pub cache_utilization_percentage: f64,
    /// Cache eviction rate per second
    pub cache_eviction_rate_per_second: f64,
    /// Pages in cache
    pub pages_in_cache: u64,
    /// Dirty pages percentage
    pub dirty_pages_percentage: f64,
    /// Cache pressure score
    pub cache_pressure_score: f64,
    /// Working set size (bytes)
    pub working_set_size_bytes: u64,
    /// Cache efficiency score
    pub cache_efficiency_score: f64,
}

/// Connection and network performance
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConnectionPerformance {
    /// Current connections
    pub current_connections: u32,
    /// Available connections
    pub available_connections: u32,
    /// Connection utilization percentage
    pub connection_utilization_percentage: f64,
    /// Average connection duration (minutes)
    pub avg_connection_duration_minutes: f64,
    /// Connection establishment time (milliseconds)
    pub connection_establishment_time_ms: f64,
    /// Network latency (milliseconds)
    pub network_latency_ms: f64,
    /// Connection timeouts per hour
    pub connection_timeouts_per_hour: u64,
    /// SSL overhead percentage
    pub ssl_overhead_percentage: f64,
    /// Connection efficiency score
    pub connection_efficiency_score: f64,
}

/// Detailed performance metrics collected when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPerformanceDetailedMetrics {
    /// Slow query analysis
    pub slow_queries: Vec<MongoSlowQueryAnalysis>,
    /// Index optimization recommendations
    pub index_recommendations: Vec<MongoIndexRecommendation>,
    /// Resource bottleneck analysis
    pub resource_bottlenecks: Vec<MongoResourceBottleneck>,
    /// Performance alerts and issues
    pub performance_alerts: Vec<MongoPerformanceAlert>,
    /// Operation hotspots
    pub operation_hotspots: Vec<MongoOperationHotspot>,
    /// Concurrency issues analysis
    pub concurrency_issues: Option<Vec<MongoConcurrencyIssue>>,
    /// Cache optimization opportunities
    pub cache_optimizations: Option<Vec<MongoCacheOptimization>>,
    /// Performance tuning recommendations
    pub tuning_recommendations: Option<Vec<MongoPerformanceTuningRecommendation>>,
}

/// Slow query analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowQueryAnalysis {
    pub query_id: String,
    pub namespace: String,
    pub operation_type: String,
    pub execution_time_ms: f64,
    pub docs_examined: u64,
    pub docs_returned: u64,
    pub index_used: bool,
    pub query_pattern: String,
    pub frequency_per_hour: u64,
    pub cpu_impact_score: f64,
    pub optimization_suggestions: Vec<String>,
    pub recommended_indexes: Vec<String>,
    pub priority_level: String,
    pub business_impact: String,
}

/// Index optimization recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexRecommendation {
    pub recommendation_type: String, // Create, Drop, Modify, Optimize
    pub collection_namespace: String,
    pub index_specification: String,
    pub current_performance_impact: String,
    pub expected_improvement: String,
    pub query_patterns_affected: Vec<String>,
    pub estimated_space_impact_mb: f64,
    pub implementation_complexity: String,
    pub risk_assessment: String,
    pub rollback_strategy: String,
    pub success_metrics: Vec<String>,
}

/// Resource bottleneck analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoResourceBottleneck {
    pub resource_type: String,       // CPU, Memory, Disk, Network
    pub bottleneck_severity: String, // Critical, High, Medium, Low
    pub current_utilization_percentage: f64,
    pub threshold_exceeded: String,
    pub affected_operations: Vec<String>,
    pub performance_impact_description: String,
    pub root_cause_analysis: String,
    pub immediate_actions: Vec<String>,
    pub long_term_solutions: Vec<String>,
    pub monitoring_recommendations: Vec<String>,
    pub estimated_resolution_time: String,
}

/// Performance alert
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPerformanceAlert {
    pub alert_type: String,
    pub severity_level: String,
    pub metric_name: String,
    pub current_value: f64,
    pub threshold_value: f64,
    pub alert_description: String,
    pub detection_time: DateTimeWrapper,
    pub affected_components: Vec<String>,
    pub business_impact: String,
    pub recommended_action: String,
    pub alert_frequency: String,
    pub escalation_required: bool,
}

/// Operation hotspot analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOperationHotspot {
    pub hotspot_type: String, // Collection, Index, Operation, Client
    pub hotspot_identifier: String,
    pub resource_consumption_score: f64,
    pub operation_frequency: f64,
    pub performance_impact: String,
    pub contention_level: String,
    pub affected_operations: Vec<String>,
    pub optimization_opportunities: Vec<String>,
    pub load_balancing_suggestions: Vec<String>,
    pub scaling_recommendations: Vec<String>,
}

/// Concurrency issue analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConcurrencyIssue {
    pub issue_type: String,
    pub affected_collections: Vec<String>,
    pub lock_contention_level: String,
    pub blocking_operations: Vec<String>,
    pub wait_time_impact_ms: f64,
    pub throughput_reduction_percentage: f64,
    pub resolution_strategies: Vec<String>,
    pub prevention_measures: Vec<String>,
    pub monitoring_adjustments: Vec<String>,
}

/// Cache optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCacheOptimization {
    pub optimization_type: String,
    pub current_cache_efficiency: f64,
    pub potential_improvement_percentage: f64,
    pub memory_adjustment_recommendation: String,
    pub cache_policy_suggestions: Vec<String>,
    pub working_set_optimization: Vec<String>,
    pub implementation_steps: Vec<String>,
    pub expected_performance_gain: String,
    pub risk_factors: Vec<String>,
}

/// Performance tuning recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPerformanceTuningRecommendation {
    pub tuning_area: String,
    pub current_configuration: String,
    pub recommended_configuration: String,
    pub performance_impact_estimate: String,
    pub implementation_priority: String,
    pub testing_requirements: Vec<String>,
    pub rollback_plan: String,
    pub success_criteria: Vec<String>,
    pub monitoring_plan: Vec<String>,
    pub compatibility_notes: Vec<String>,
}

impl MetadataCollection for MongoPerformanceStats {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "server_status".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.serverStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(20)),
                ),
            ),
            (
                "slow_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 100 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(1)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(1000)),
                ),
            ),
            (
                "index_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.indexStats": { "$exists": true } },
                            { "command.collStats": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "lock_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.currentOp": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "wiredtiger_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.serverStatus": { "$exists": true },
                        "result.wiredTiger": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(10)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive MongoDB performance statistics and optimization recommendations"
    }

    fn category(&self) -> &'static str {
        "performance"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High // Performance metrics need frequent monitoring
    }
}

use function_name::named;
use std::time::Duration;

#[allow(dead_code)]
impl MongoPerformanceStats {
    const SLOW_QUERY_THRESHOLD_MS: f64 = 100.0;
    const HIGH_CPU_THRESHOLD: f64 = 80.0;
    const HIGH_MEMORY_THRESHOLD: f64 = 85.0;
    const LOW_CACHE_HIT_RATIO: f64 = 0.9; // 90%
    const HIGH_LOCK_CONTENTION_THRESHOLD: f64 = 50.0; // per second
    const POOR_QUERY_EFFICIENCY: f64 = 0.1; // 10% efficiency
    const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
    const MAX_DETAILED_RESULTS: usize = 150;
    const POOR_PERFORMANCE_THRESHOLD: f64 = 0.7; // 70%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut performance_stats = MongoPerformanceStats {
            measurement_timestamp: DateTimeWrapper::from(Utc::now()),
            ..Default::default()
        };

        // Execute serverStatus directly - contains all performance data
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;
        Self::parse_server_status(&mut performance_stats, &server_status_docs)?;
        Self::parse_wiredtiger_stats(&mut performance_stats, &server_status_docs)?;

        // Calculate derived metrics and scores
        Self::calculate_performance_scores(&mut performance_stats)?;

        // Detailed metrics temporarily disabled during refactor
        performance_stats.detailed_metrics = None;

        Ok(performance_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoPerformanceStats,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoPerformanceDetailedMetrics>> {
        let needs_slow_query_analysis = core_stats.query_performance.slow_query_count > 10;
        let needs_resource_analysis = core_stats.resource_utilization.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD
            || core_stats.resource_utilization.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD;
        let needs_cache_analysis = core_stats.cache_performance.cache_hit_ratio < Self::LOW_CACHE_HIT_RATIO;
        let needs_concurrency_analysis = core_stats.concurrency_metrics.lock_contention_per_second > Self::HIGH_LOCK_CONTENTION_THRESHOLD;
        let needs_index_analysis = core_stats.query_performance.collection_scan_ratio > 20.0;
        let needs_overall_analysis = core_stats.overall_performance_score < Self::POOR_PERFORMANCE_THRESHOLD;

        if !needs_slow_query_analysis
            && !needs_resource_analysis
            && !needs_cache_analysis
            && !needs_concurrency_analysis
            && !needs_index_analysis
            && !needs_overall_analysis
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoPerformanceDetailedMetrics {
            slow_queries: Vec::new(),
            index_recommendations: Vec::new(),
            resource_bottlenecks: Vec::new(),
            performance_alerts: Vec::new(),
            operation_hotspots: Vec::new(),
            concurrency_issues: None,
            cache_optimizations: None,
            tuning_recommendations: None,
        };

        // Collect slow query analysis if needed
        if needs_slow_query_analysis {
            let docs = fetch(requests, "slow_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.slow_queries = Self::analyze_slow_queries(docs)?;
        }

        // Generate index recommendations if needed
        if needs_index_analysis {
            detailed_metrics.index_recommendations = Self::generate_index_recommendations(core_stats)?;
        }

        // Analyze resource bottlenecks if needed
        if needs_resource_analysis {
            detailed_metrics.resource_bottlenecks = Self::analyze_resource_bottlenecks(core_stats)?;
        }

        // Generate performance alerts
        detailed_metrics.performance_alerts = Self::generate_performance_alerts(core_stats)?;

        // Analyze operation hotspots
        detailed_metrics.operation_hotspots = Self::analyze_operation_hotspots(core_stats)?;

        // Generate concurrency analysis if needed
        if needs_concurrency_analysis {
            detailed_metrics.concurrency_issues = Some(Self::analyze_concurrency_issues(core_stats)?);
        }

        // Generate cache optimizations if needed
        if needs_cache_analysis {
            detailed_metrics.cache_optimizations = Some(Self::generate_cache_optimizations(core_stats)?);
        }

        // Generate tuning recommendations
        detailed_metrics.tuning_recommendations = Some(Self::generate_tuning_recommendations(core_stats)?);

        Ok(Some(detailed_metrics))
    }
    fn parse_server_status(stats: &mut MongoPerformanceStats, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(opcounters) = result.child("opcounters") {
                    Self::parse_operation_counters(stats, opcounters.raw())?;
                }

                if let Some(mem) = result.child("mem") {
                    Self::parse_memory_stats(stats, mem.raw())?;
                }

                if let Some(connections) = result.child("connections") {
                    Self::parse_connection_stats(stats, connections.raw())?;
                }

                if let Some(locks) = result.child("locks") {
                    Self::parse_lock_statistics(stats, locks.raw())?;
                }

                if let Some(metrics) = result.child("metrics") {
                    Self::parse_metrics(stats, metrics.raw())?;
                }

                if let Some(network) = result.child("network") {
                    Self::parse_network_stats(stats, network.raw())?;
                }

                if let Some(extra_info) = result.child("extra_info") {
                    Self::parse_extra_info(stats, extra_info.raw())?;
                }
            }
        }

        Ok(())
    }

    fn parse_operation_counters(stats: &mut MongoPerformanceStats, opcounters: &Document) -> ResultEP<()> {
        let time_window = 300.0; // 5 minute window

        if let Ok(query) = opcounters.get_i64("query") {
            stats.operation_metrics.read_ops_per_second = query as f64 / time_window;
        }

        if let Ok(insert) = opcounters.get_i64("insert") {
            stats.operation_metrics.insert_ops_per_second = insert as f64 / time_window;
        }

        if let Ok(update) = opcounters.get_i64("update") {
            stats.operation_metrics.update_ops_per_second = update as f64 / time_window;
        }

        if let Ok(delete) = opcounters.get_i64("delete") {
            stats.operation_metrics.delete_ops_per_second = delete as f64 / time_window;
        }

        if let Ok(command) = opcounters.get_i64("command") {
            stats.operation_metrics.command_ops_per_second = command as f64 / time_window;
        }

        // Calculate total and write ops
        stats.operation_metrics.write_ops_per_second = stats.operation_metrics.insert_ops_per_second
            + stats.operation_metrics.update_ops_per_second
            + stats.operation_metrics.delete_ops_per_second;

        stats.operation_metrics.total_ops_per_second = stats.operation_metrics.read_ops_per_second
            + stats.operation_metrics.write_ops_per_second
            + stats.operation_metrics.command_ops_per_second;

        Ok(())
    }

    fn parse_memory_stats(stats: &mut MongoPerformanceStats, mem: &Document) -> ResultEP<()> {
        if let Ok(resident) = mem.get_i32("resident") {
            stats.resource_utilization.total_memory_bytes = (resident as u64) * 1024 * 1024;
            // MB to bytes
        }

        if let Ok(virtual_mem) = mem.get_i32("virtual") {
            // Estimate memory utilization
            if stats.resource_utilization.total_memory_bytes > 0 {
                let virtual_bytes = (virtual_mem as u64) * 1024 * 1024;
                stats.resource_utilization.memory_utilization_percentage =
                    (virtual_bytes as f64 / stats.resource_utilization.total_memory_bytes as f64) * 100.0;
            }
        }

        if let Ok(mapped) = mem.get_i32("mapped") {
            stats.resource_utilization.available_memory_bytes = (mapped as u64) * 1024 * 1024;
        }

        Ok(())
    }

    fn parse_connection_stats(stats: &mut MongoPerformanceStats, connections: &Document) -> ResultEP<()> {
        if let Ok(current) = connections.get_i32("current") {
            stats.connection_performance.current_connections = current as u32;
        }

        if let Ok(available) = connections.get_i32("available") {
            stats.connection_performance.available_connections = available as u32;
        }

        // Calculate connection utilization
        let total_connections = stats.connection_performance.current_connections + stats.connection_performance.available_connections;
        if total_connections > 0 {
            stats.connection_performance.connection_utilization_percentage =
                (stats.connection_performance.current_connections as f64 / total_connections as f64) * 100.0;
        }

        Ok(())
    }

    fn parse_lock_statistics(stats: &mut MongoPerformanceStats, locks: &Document) -> ResultEP<()> {
        let mut total_acquire_count = 0u64;
        let mut total_acquire_wait_count = 0u64;
        let mut total_time_acquiring = 0u64;

        // Parse global locks
        if let Ok(global) = locks.get_document("Global") {
            if let Ok(acquire_count) = global.get_document("acquireCount") {
                for (_, count) in acquire_count {
                    if let Some(val) = count.as_i64() {
                        total_acquire_count += val as u64;
                    }
                }
            }

            if let Ok(acquire_wait_count) = global.get_document("acquireWaitCount") {
                for (_, count) in acquire_wait_count {
                    if let Some(val) = count.as_i64() {
                        total_acquire_wait_count += val as u64;
                    }
                }
            }

            if let Ok(time_acquiring) = global.get_document("timeAcquiringMicros") {
                for (_, time) in time_acquiring {
                    if let Some(val) = time.as_i64() {
                        total_time_acquiring += val as u64;
                    }
                }
            }
        }

        // Calculate lock contention metrics
        if total_acquire_count > 0 {
            stats.concurrency_metrics.lock_contention_per_second = total_acquire_wait_count as f64 / 300.0; // 5 minute window

            if total_acquire_wait_count > 0 {
                stats.concurrency_metrics.avg_lock_wait_time_ms = (total_time_acquiring as f64 / total_acquire_wait_count as f64) / 1000.0;
                // microseconds to ms
            }
        }

        Ok(())
    }

    fn parse_metrics(stats: &mut MongoPerformanceStats, metrics: &Document) -> ResultEP<()> {
        // Parse query executor metrics
        if let Ok(query_executor) = metrics.get_document("queryExecutor")
            && let Ok(scanned) = query_executor.get_i64("scanned")
            && let Ok(scanned_objects) = query_executor.get_i64("scannedObjects")
            && scanned > 0
        {
            stats.query_performance.query_efficiency_score = scanned_objects as f64 / scanned as f64;
        }

        // Parse document metrics
        if let Ok(document) = metrics.get_document("document")
            && let Ok(returned) = document.get_i64("returned")
        {
            stats.query_performance.avg_docs_returned_per_query = returned as f64 / 100.0;
            // Estimate
        }

        // Parse operation metrics
        if let Ok(operation) = metrics.get_document("operation")
            && let Ok(scan_and_order) = operation.get_i64("scanAndOrder")
        {
            // Use as indicator of collection scans
            stats.query_performance.collection_scan_ratio = (scan_and_order as f64 / stats.operation_metrics.total_ops_per_second) * 100.0;
        }

        Ok(())
    }

    fn parse_network_stats(stats: &mut MongoPerformanceStats, network: &Document) -> ResultEP<()> {
        if let Ok(bytes_in) = network.get_i64("bytesIn")
            && let Ok(bytes_out) = network.get_i64("bytesOut")
        {
            let total_bytes = bytes_in + bytes_out;
            // Estimate network utilization (rough approximation)
            let bandwidth_estimate = 1024.0 * 1024.0 * 1024.0; // 1 Gbps
            stats.resource_utilization.network_utilization_percentage = (total_bytes as f64 / bandwidth_estimate) * 100.0;
        }

        Ok(())
    }

    fn parse_extra_info(stats: &mut MongoPerformanceStats, extra_info: &Document) -> ResultEP<()> {
        if let Ok(page_faults) = extra_info.get_i64("page_faults") {
            // Use page faults as an indicator of memory pressure
            stats.resource_utilization.memory_pressure_score = std::cmp::min(page_faults as u64, 100) as f64 / 100.0;
        }

        // Parse user and system time for CPU estimation
        if let Ok(user_time) = extra_info.get_i64("user_time_us")
            && let Ok(system_time) = extra_info.get_i64("system_time_us")
        {
            let total_time = user_time + system_time;
            // Rough CPU utilization estimate
            stats.resource_utilization.cpu_utilization_percentage = std::cmp::min(total_time / 10000, 100) as f64; // Normalized estimate
        }

        Ok(())
    }

    fn parse_slow_operations(stats: &mut MongoPerformanceStats, docs: &[Document]) -> ResultEP<()> {
        let mut total_query_time = 0.0;
        let mut query_times = Vec::new();
        let mut docs_examined_total = 0u64;
        let mut docs_returned_total = 0u64;
        let mut collection_scan_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis") {
                total_query_time += millis;
                query_times.push(millis);

                // Check for collection scans
                if let Some(execution_stats) = acc.child("executionStats") {
                    if let Some(total_docs_examined) = execution_stats.opt_i64("totalDocsExamined") {
                        docs_examined_total += total_docs_examined as u64;
                    }
                    if let Some(total_docs_returned) = execution_stats.opt_i64("totalDocsReturned") {
                        docs_returned_total += total_docs_returned as u64;
                    }

                    // Check if this was a collection scan
                    if let Some(inner_es) = execution_stats.child("executionStats")
                        && let Some(winning_plan) = inner_es.child("winningPlan")
                        && let Some(stage) = winning_plan.opt_string("stage")
                        && stage == "COLLSCAN"
                    {
                        collection_scan_count += 1;
                    }
                }
            }
        }

        let doc_count = docs.len();
        stats.query_performance.slow_query_count = doc_count as u64;

        if doc_count > 0 {
            stats.query_performance.avg_query_time_ms = total_query_time / doc_count as f64;

            // Calculate percentiles
            query_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            if !query_times.is_empty() {
                let p95_index = ((doc_count as f64 * 0.95) as usize).min(doc_count - 1);
                let p99_index = ((doc_count as f64 * 0.99) as usize).min(doc_count - 1);

                stats.query_performance.p95_query_time_ms = query_times[p95_index];
                stats.query_performance.p99_query_time_ms = query_times[p99_index];
            }

            stats.query_performance.avg_docs_examined_per_query = docs_examined_total as f64 / doc_count as f64;
            stats.query_performance.avg_docs_returned_per_query = docs_returned_total as f64 / doc_count as f64;

            if docs_examined_total > 0 {
                stats.query_performance.query_efficiency_score = docs_returned_total as f64 / docs_examined_total as f64;
            }

            stats.query_performance.collection_scan_ratio = (collection_scan_count as f64 / doc_count as f64) * 100.0;
        }

        // Estimate queries per second (from 1 hour of slow queries)
        stats.query_performance.queries_per_second = doc_count as f64 / 3600.0;

        Ok(())
    }

    fn parse_index_stats(stats: &mut MongoPerformanceStats, docs: &[Document]) -> ResultEP<()> {
        let mut total_index_size = 0u64;
        let mut total_data_size = 0u64;
        let mut index_count = 0u32;
        let mut _index_accesses = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result") {
                // Parse collection stats
                if let Some(index_sizes) = result.child("indexSizes") {
                    for (index_name, size) in index_sizes.raw() {
                        if let Some(size_val) = size.as_i64() {
                            total_index_size += size_val as u64;
                            index_count += 1;
                            _index_accesses.push((index_name.clone(), size_val as u64));
                        }
                    }
                }

                if let Some(size) = result.opt_i64("size") {
                    total_data_size += size as u64;
                }

                // Parse index stats
                if let Some(index_details) = result.array("indexDetails") {
                    for _index_doc in index_details {
                        // Additional index analysis would go here
                    }
                }
            }
        }

        stats.index_performance.total_index_count = index_count;
        stats.index_performance.total_index_size_bytes = total_index_size;

        if index_count > 0 {
            stats.index_performance.avg_index_size_bytes = total_index_size / index_count as u64;
        }

        if total_data_size > 0 {
            stats.index_performance.index_to_data_ratio = total_index_size as f64 / total_data_size as f64;
        }

        // Estimate index hit ratio (inverse of collection scan ratio)
        stats.index_performance.index_hit_ratio = 100.0 - stats.query_performance.collection_scan_ratio;

        Ok(())
    }

    fn parse_lock_stats(stats: &mut MongoPerformanceStats, docs: &[Document]) -> ResultEP<()> {
        let mut active_reads = 0u32;
        let mut active_writes = 0u32;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Some(ops) = result.array("inprog")
            {
                for operation in ops {
                    if let Some(op_type) = operation.opt_string("op") {
                        match op_type.as_str() {
                            "query" | "getmore" => active_reads += 1,
                            "insert" | "update" | "remove" => active_writes += 1,
                            _ => {}
                        }
                    }
                }
            }
        }

        stats.concurrency_metrics.active_read_operations = active_reads;
        stats.concurrency_metrics.active_write_operations = active_writes;
        stats.concurrency_metrics.concurrent_connections = stats.connection_performance.current_connections;

        Ok(())
    }

    fn parse_wiredtiger_stats(stats: &mut MongoPerformanceStats, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Some(wiredtiger) = result.child("wiredTiger")
            {
                Self::parse_wiredtiger_cache(stats, wiredtiger.raw())?;
                Self::parse_wiredtiger_block_manager(stats, wiredtiger.raw())?;
                Self::parse_wiredtiger_concurrency(stats, wiredtiger.raw())?;
            }
        }

        Ok(())
    }

    fn parse_wiredtiger_cache(stats: &mut MongoPerformanceStats, wiredtiger: &Document) -> ResultEP<()> {
        if let Ok(cache) = wiredtiger.get_document("cache") {
            if let Ok(bytes_in_cache) = cache.get_i64("bytes currently in the cache") {
                stats.cache_performance.cache_size_bytes = bytes_in_cache as u64;
            }

            if let Ok(max_bytes) = cache.get_i64("maximum bytes configured")
                && max_bytes > 0
            {
                stats.cache_performance.cache_utilization_percentage =
                    (stats.cache_performance.cache_size_bytes as f64 / max_bytes as f64) * 100.0;
            }

            if let Ok(pages_read) = cache.get_i64("pages read into cache")
                && let Ok(pages_requested) = cache.get_i64("pages requested from the cache")
                && pages_requested > 0
            {
                let cache_hits = pages_requested - pages_read;
                stats.cache_performance.cache_hit_ratio = cache_hits as f64 / pages_requested as f64;
            }

            if let Ok(evicted_pages) = cache.get_i64("pages evicted because they exceeded the in-memory maximum count") {
                stats.cache_performance.cache_eviction_rate_per_second = evicted_pages as f64 / 300.0; // 5 min window
            }

            if let Ok(dirty_pages) = cache.get_i64("tracked dirty pages in the cache")
                && let Ok(total_pages) = cache.get_i64("pages currently held in the cache")
            {
                if total_pages > 0 {
                    stats.cache_performance.dirty_pages_percentage = (dirty_pages as f64 / total_pages as f64) * 100.0;
                }
                stats.cache_performance.pages_in_cache = total_pages as u64;
            }
        }

        Ok(())
    }

    fn parse_wiredtiger_block_manager(stats: &mut MongoPerformanceStats, wiredtiger: &Document) -> ResultEP<()> {
        if let Ok(block_manager) = wiredtiger.get_document("block-manager") {
            if let Ok(blocks_read) = block_manager.get_i64("blocks read")
                && let Ok(bytes_read) = block_manager.get_i64("bytes read")
            {
                stats.storage_performance.read_throughput_bps = bytes_read as f64 / 300.0; // 5 min window
                if blocks_read > 0 {
                    stats.storage_performance.disk_read_iops = blocks_read as f64 / 300.0;
                }
            }

            if let Ok(blocks_written) = block_manager.get_i64("blocks written")
                && let Ok(bytes_written) = block_manager.get_i64("bytes written")
            {
                stats.storage_performance.write_throughput_bps = bytes_written as f64 / 300.0;
                if blocks_written > 0 {
                    stats.storage_performance.disk_write_iops = blocks_written as f64 / 300.0;
                }
            }
        }

        stats.storage_performance.storage_engine = "wiredTiger".to_string();

        Ok(())
    }

    fn parse_wiredtiger_concurrency(stats: &mut MongoPerformanceStats, wiredtiger: &Document) -> ResultEP<()> {
        if let Ok(concurrency) = wiredtiger.get_document("concurrentTransactions") {
            if let Ok(read) = concurrency.get_document("read")
                && let Ok(out) = read.get_i32("out")
            {
                stats.concurrency_metrics.active_read_operations = out as u32;
            }

            if let Ok(write) = concurrency.get_document("write")
                && let Ok(out) = write.get_i32("out")
            {
                stats.concurrency_metrics.active_write_operations = out as u32;
            }
        }

        Ok(())
    }

    fn calculate_performance_scores(stats: &mut MongoPerformanceStats) -> ResultEP<()> {
        // Calculate individual component scores
        let query_score = Self::calculate_query_performance_score(&stats.query_performance);
        let index_score = Self::calculate_index_performance_score(&stats.index_performance);
        let resource_score = Self::calculate_resource_utilization_score(&stats.resource_utilization);
        let cache_score = Self::calculate_cache_performance_score(&stats.cache_performance);
        let concurrency_score = Self::calculate_concurrency_score(&stats.concurrency_metrics);
        let connection_score = Self::calculate_connection_score(&stats.connection_performance);

        // Set individual efficiency scores
        stats.query_performance.query_efficiency_score = std::cmp::max_by(stats.query_performance.query_efficiency_score, 0.0, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });

        stats.index_performance.index_efficiency_score = index_score;
        stats.cache_performance.cache_efficiency_score = cache_score;
        stats.concurrency_metrics.concurrency_efficiency_score = concurrency_score;
        stats.connection_performance.connection_efficiency_score = connection_score;

        // Calculate overall performance score
        let scores = [
            query_score,
            index_score,
            resource_score,
            cache_score,
            concurrency_score,
            connection_score,
        ];
        stats.overall_performance_score = scores.iter().sum::<f64>() / scores.len() as f64;

        // Calculate performance trend (simplified - would need historical data)
        stats.performance_trend = if stats.overall_performance_score > 0.8 {
            0.1
        } else if stats.overall_performance_score > 0.6 {
            0.0
        } else {
            -0.1
        };

        // Calculate cache pressure score
        let cache_pressure = 1.0 - stats.cache_performance.cache_hit_ratio;
        let eviction_pressure = std::cmp::min_by(stats.cache_performance.cache_eviction_rate_per_second / 100.0, 1.0, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        stats.cache_performance.cache_pressure_score = (cache_pressure + eviction_pressure) / 2.0;

        // Calculate resource contention score
        let cpu_pressure = stats.resource_utilization.cpu_utilization_percentage / 100.0;
        let memory_pressure = stats.resource_utilization.memory_utilization_percentage / 100.0;
        let lock_pressure = std::cmp::min_by(stats.concurrency_metrics.lock_contention_per_second / 100.0, 1.0, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        stats.resource_utilization.resource_contention_score = (cpu_pressure + memory_pressure + lock_pressure) / 3.0;

        Ok(())
    }

    fn calculate_query_performance_score(query_perf: &MongoQueryPerformance) -> f64 {
        let mut score = 1.0;

        // Penalize high query times
        if query_perf.avg_query_time_ms > 1000.0 {
            score *= 0.3;
        } else if query_perf.avg_query_time_ms > 500.0 {
            score *= 0.6;
        } else if query_perf.avg_query_time_ms > 100.0 {
            score *= 0.8;
        }

        // Penalize collection scans
        if query_perf.collection_scan_ratio > 50.0 {
            score *= 0.3;
        } else if query_perf.collection_scan_ratio > 20.0 {
            score *= 0.7;
        }

        // Reward good efficiency
        if query_perf.query_efficiency_score > 0.8 {
            score *= 1.1;
        } else if query_perf.query_efficiency_score < 0.1 {
            score *= 0.5;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn calculate_index_performance_score(index_perf: &MongoIndexPerformance) -> f64 {
        let mut score = 1.0;

        // Reward high index hit ratio
        score *= index_perf.index_hit_ratio / 100.0;

        // Penalize too many unused indexes
        if index_perf.unused_indexes_count > 10 {
            score *= 0.7;
        } else if index_perf.unused_indexes_count > 5 {
            score *= 0.85;
        }

        // Penalize excessive index-to-data ratio
        if index_perf.index_to_data_ratio > 1.0 {
            score *= 0.6;
        } else if index_perf.index_to_data_ratio > 0.5 {
            score *= 0.8;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn calculate_resource_utilization_score(resource: &MongoResourceUtilization) -> f64 {
        let mut score = 1.0;

        // Penalize high CPU utilization
        if resource.cpu_utilization_percentage > 90.0 {
            score *= 0.2;
        } else if resource.cpu_utilization_percentage > 80.0 {
            score *= 0.5;
        } else if resource.cpu_utilization_percentage > 70.0 {
            score *= 0.8;
        }

        // Penalize high memory utilization
        if resource.memory_utilization_percentage > 95.0 {
            score *= 0.2;
        } else if resource.memory_utilization_percentage > 85.0 {
            score *= 0.6;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn calculate_cache_performance_score(cache: &MongoCachePerformance) -> f64 {
        let mut score = cache.cache_hit_ratio;

        // Penalize high eviction rates
        if cache.cache_eviction_rate_per_second > 100.0 {
            score *= 0.5;
        } else if cache.cache_eviction_rate_per_second > 50.0 {
            score *= 0.8;
        }

        // Penalize high dirty page percentage
        if cache.dirty_pages_percentage > 20.0 {
            score *= 0.7;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn calculate_concurrency_score(concurrency: &MongoConcurrencyMetrics) -> f64 {
        let mut score = 1.0;

        // Penalize high lock contention
        if concurrency.lock_contention_per_second > 100.0 {
            score *= 0.3;
        } else if concurrency.lock_contention_per_second > 50.0 {
            score *= 0.6;
        }

        // Penalize long lock wait times
        if concurrency.avg_lock_wait_time_ms > 100.0 {
            score *= 0.5;
        } else if concurrency.avg_lock_wait_time_ms > 50.0 {
            score *= 0.8;
        }

        // Penalize deadlocks
        if concurrency.deadlock_count > 0 {
            score *= 0.7;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn calculate_connection_score(connection: &MongoConnectionPerformance) -> f64 {
        let mut score = 1.0;

        // Penalize high connection utilization
        if connection.connection_utilization_percentage > 90.0 {
            score *= 0.4;
        } else if connection.connection_utilization_percentage > 80.0 {
            score *= 0.7;
        }

        // Penalize high connection timeouts
        if connection.connection_timeouts_per_hour > 10 {
            score *= 0.6;
        }

        std::cmp::max_by(score, 0.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    // Detailed analysis functions
    fn analyze_slow_queries(docs: Vec<Document>) -> ResultEP<Vec<MongoSlowQueryAnalysis>> {
        let mut slow_queries = Vec::new();
        let mut processed = 0;

        for doc in docs {
            if processed >= Self::MAX_DETAILED_RESULTS {
                break;
            }

            let acc = DocAccessor::new(&doc);
            if let Some(millis) = acc.opt_f64("millis")
                && millis > Self::SLOW_QUERY_THRESHOLD_MS
            {
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());
                let op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());

                let (docs_examined, docs_returned, index_used) = Self::extract_execution_stats(&doc);

                slow_queries.push(MongoSlowQueryAnalysis {
                    query_id: format!("slow_query_{}", processed),
                    namespace: ns.clone(),
                    operation_type: op_type.clone(),
                    execution_time_ms: millis,
                    docs_examined,
                    docs_returned,
                    index_used,
                    query_pattern: Self::extract_query_pattern(&doc),
                    frequency_per_hour: 1, // Would need aggregation for actual frequency
                    cpu_impact_score: Self::estimate_cpu_impact(millis, docs_examined),
                    optimization_suggestions: Self::suggest_query_optimizations(&doc, millis),
                    recommended_indexes: Self::recommend_indexes_for_query(&doc),
                    priority_level: if millis > 5000.0 { "High" } else { "Medium" }.to_string(),
                    business_impact: Self::assess_business_impact(&ns, millis),
                });

                processed += 1;
            }
        }

        Ok(slow_queries)
    }

    fn generate_index_recommendations(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoIndexRecommendation>> {
        let mut recommendations = Vec::new();

        // High collection scan ratio indicates missing indexes
        if stats.query_performance.collection_scan_ratio > 20.0 {
            recommendations.push(MongoIndexRecommendation {
                recommendation_type: "Create Missing Indexes".to_string(),
                collection_namespace: "Multiple collections".to_string(),
                index_specification: "Analyze query patterns to determine optimal indexes".to_string(),
                current_performance_impact: format!(
                    "{:.1}% of queries use collection scans",
                    stats.query_performance.collection_scan_ratio
                ),
                expected_improvement: "50-90% reduction in query execution time".to_string(),
                query_patterns_affected: vec![
                    "Queries with equality conditions".to_string(),
                    "Range queries".to_string(),
                    "Sort operations".to_string(),
                ],
                estimated_space_impact_mb: stats.index_performance.avg_index_size_bytes as f64 / (1024.0 * 1024.0),
                implementation_complexity: "Medium".to_string(),
                risk_assessment: "Low risk - indexes improve read performance".to_string(),
                rollback_strategy: "Drop indexes if performance degrades".to_string(),
                success_metrics: vec![
                    "Collection scan ratio < 10%".to_string(),
                    "Average query time reduction".to_string(),
                    "Index hit ratio > 90%".to_string(),
                ],
            });
        }

        // Too many unused indexes
        if stats.index_performance.unused_indexes_count > 5 {
            recommendations.push(MongoIndexRecommendation {
                recommendation_type: "Remove Unused Indexes".to_string(),
                collection_namespace: "Multiple collections".to_string(),
                index_specification: format!("{} unused indexes identified", stats.index_performance.unused_indexes_count),
                current_performance_impact: "Increased write latency and storage overhead".to_string(),
                expected_improvement: "10-30% improvement in write performance".to_string(),
                query_patterns_affected: vec!["Write operations".to_string()],
                estimated_space_impact_mb: -(stats.index_performance.avg_index_size_bytes as f64
                    * stats.index_performance.unused_indexes_count as f64)
                    / (1024.0 * 1024.0),
                implementation_complexity: "Low".to_string(),
                risk_assessment: "Medium - ensure indexes are truly unused".to_string(),
                rollback_strategy: "Recreate indexes if needed".to_string(),
                success_metrics: vec![
                    "Reduced storage usage".to_string(),
                    "Improved write performance".to_string(),
                    "Lower index maintenance overhead".to_string(),
                ],
            });
        }

        Ok(recommendations)
    }

    fn analyze_resource_bottlenecks(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoResourceBottleneck>> {
        let mut bottlenecks = Vec::new();

        // CPU bottleneck
        if stats.resource_utilization.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
            bottlenecks.push(MongoResourceBottleneck {
                resource_type: "CPU".to_string(),
                bottleneck_severity: if stats.resource_utilization.cpu_utilization_percentage > 95.0 {
                    "Critical"
                } else {
                    "High"
                }
                .to_string(),
                current_utilization_percentage: stats.resource_utilization.cpu_utilization_percentage,
                threshold_exceeded: format!(
                    "{:.1}% > {:.1}%",
                    stats.resource_utilization.cpu_utilization_percentage,
                    Self::HIGH_CPU_THRESHOLD
                ),
                affected_operations: vec![
                    "Query processing".to_string(),
                    "Index operations".to_string(),
                    "Aggregation pipeline".to_string(),
                ],
                performance_impact_description: "Increased query latency and reduced throughput".to_string(),
                root_cause_analysis: "High computational load from queries or insufficient CPU resources".to_string(),
                immediate_actions: vec![
                    "Identify expensive queries".to_string(),
                    "Review query patterns".to_string(),
                    "Consider query optimization".to_string(),
                ],
                long_term_solutions: vec![
                    "Scale CPU resources vertically".to_string(),
                    "Optimize query performance".to_string(),
                    "Implement read replicas".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Set CPU alerts at 80%".to_string(),
                    "Monitor query execution patterns".to_string(),
                    "Track slow query trends".to_string(),
                ],
                estimated_resolution_time: "2-8 hours depending on approach".to_string(),
            });
        }

        // Memory bottleneck
        if stats.resource_utilization.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
            bottlenecks.push(MongoResourceBottleneck {
                resource_type: "Memory".to_string(),
                bottleneck_severity: if stats.resource_utilization.memory_utilization_percentage > 95.0 {
                    "Critical"
                } else {
                    "High"
                }
                .to_string(),
                current_utilization_percentage: stats.resource_utilization.memory_utilization_percentage,
                threshold_exceeded: format!(
                    "{:.1}% > {:.1}%",
                    stats.resource_utilization.memory_utilization_percentage,
                    Self::HIGH_MEMORY_THRESHOLD
                ),
                affected_operations: vec![
                    "Cache performance".to_string(),
                    "Working set operations".to_string(),
                    "Index loading".to_string(),
                ],
                performance_impact_description: "Increased cache misses and slower data access".to_string(),
                root_cause_analysis: "Working set exceeds available memory or memory leak".to_string(),
                immediate_actions: vec![
                    "Review memory usage patterns".to_string(),
                    "Check for memory leaks".to_string(),
                    "Optimize cache settings".to_string(),
                ],
                long_term_solutions: vec![
                    "Increase available memory".to_string(),
                    "Optimize data model".to_string(),
                    "Implement data archiving".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Monitor working set size".to_string(),
                    "Track cache hit ratios".to_string(),
                    "Set memory pressure alerts".to_string(),
                ],
                estimated_resolution_time: "4-12 hours for optimization".to_string(),
            });
        }

        Ok(bottlenecks)
    }

    fn generate_performance_alerts(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoPerformanceAlert>> {
        let mut alerts = Vec::new();

        // Slow query alert
        if stats.query_performance.slow_query_count > 100 {
            alerts.push(MongoPerformanceAlert {
                alert_type: "High Slow Query Count".to_string(),
                severity_level: "High".to_string(),
                metric_name: "slow_query_count".to_string(),
                current_value: stats.query_performance.slow_query_count as f64,
                threshold_value: 100.0,
                alert_description: format!("{} slow queries detected in the last hour", stats.query_performance.slow_query_count),
                detection_time: DateTimeWrapper::from(Utc::now()),
                affected_components: vec!["Query performance".to_string(), "User experience".to_string()],
                business_impact: "Degraded application response times affecting user satisfaction".to_string(),
                recommended_action: "Review and optimize slow queries, consider adding indexes".to_string(),
                alert_frequency: "Hourly".to_string(),
                escalation_required: stats.query_performance.slow_query_count > 500,
            });
        }

        // Low cache hit ratio alert
        if stats.cache_performance.cache_hit_ratio < Self::LOW_CACHE_HIT_RATIO {
            alerts.push(MongoPerformanceAlert {
                alert_type: "Low Cache Hit Ratio".to_string(),
                severity_level: "Medium".to_string(),
                metric_name: "cache_hit_ratio".to_string(),
                current_value: stats.cache_performance.cache_hit_ratio,
                threshold_value: Self::LOW_CACHE_HIT_RATIO,
                alert_description: format!(
                    "Cache hit ratio at {:.1}% below optimal threshold",
                    stats.cache_performance.cache_hit_ratio * 100.0
                ),
                detection_time: DateTimeWrapper::from(Utc::now()),
                affected_components: vec!["Cache performance".to_string(), "I/O performance".to_string()],
                business_impact: "Increased I/O load and slower query response times".to_string(),
                recommended_action: "Review cache configuration and working set size".to_string(),
                alert_frequency: "Every 15 minutes".to_string(),
                escalation_required: stats.cache_performance.cache_hit_ratio < 0.8,
            });
        }

        // High lock contention alert
        if stats.concurrency_metrics.lock_contention_per_second > Self::HIGH_LOCK_CONTENTION_THRESHOLD {
            alerts.push(MongoPerformanceAlert {
                alert_type: "High Lock Contention".to_string(),
                severity_level: "High".to_string(),
                metric_name: "lock_contention_per_second".to_string(),
                current_value: stats.concurrency_metrics.lock_contention_per_second,
                threshold_value: Self::HIGH_LOCK_CONTENTION_THRESHOLD,
                alert_description: format!(
                    "{:.1} lock contentions per second detected",
                    stats.concurrency_metrics.lock_contention_per_second
                ),
                detection_time: DateTimeWrapper::from(Utc::now()),
                affected_components: vec!["Concurrency".to_string(), "Write performance".to_string()],
                business_impact: "Reduced throughput and increased operation latency".to_string(),
                recommended_action: "Analyze locking patterns and optimize query concurrency".to_string(),
                alert_frequency: "Real-time".to_string(),
                escalation_required: stats.concurrency_metrics.lock_contention_per_second > 100.0,
            });
        }

        Ok(alerts)
    }

    fn analyze_operation_hotspots(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoOperationHotspot>> {
        let mut hotspots = Vec::new();

        // High write operation hotspot
        if stats.operation_metrics.write_ops_per_second > 500.0 {
            hotspots.push(MongoOperationHotspot {
                hotspot_type: "Write Operations".to_string(),
                hotspot_identifier: "write_intensive_workload".to_string(),
                resource_consumption_score: 8.0,
                operation_frequency: stats.operation_metrics.write_ops_per_second,
                performance_impact: "High write volume may cause replication lag".to_string(),
                contention_level: "Medium".to_string(),
                affected_operations: vec![
                    "Insert operations".to_string(),
                    "Update operations".to_string(),
                    "Delete operations".to_string(),
                ],
                optimization_opportunities: vec![
                    "Batch write operations".to_string(),
                    "Optimize write patterns".to_string(),
                    "Consider write concern adjustment".to_string(),
                ],
                load_balancing_suggestions: vec![
                    "Distribute writes across time".to_string(),
                    "Use multiple application instances".to_string(),
                    "Implement write queuing".to_string(),
                ],
                scaling_recommendations: vec![
                    "Scale write capacity".to_string(),
                    "Consider sharding for write distribution".to_string(),
                    "Implement read replicas".to_string(),
                ],
            });
        }

        // Collection scan hotspot
        if stats.query_performance.collection_scan_ratio > 30.0 {
            hotspots.push(MongoOperationHotspot {
                hotspot_type: "Collection Scans".to_string(),
                hotspot_identifier: "missing_indexes".to_string(),
                resource_consumption_score: 9.0,
                operation_frequency: stats.query_performance.queries_per_second,
                performance_impact: "High CPU and I/O usage from full collection scans".to_string(),
                contention_level: "High".to_string(),
                affected_operations: vec![
                    "Query operations".to_string(),
                    "Aggregation pipelines".to_string(),
                    "Sort operations".to_string(),
                ],
                optimization_opportunities: vec![
                    "Create appropriate indexes".to_string(),
                    "Optimize query filters".to_string(),
                    "Review query patterns".to_string(),
                ],
                load_balancing_suggestions: vec![
                    "Use read replicas for queries".to_string(),
                    "Implement query result caching".to_string(),
                    "Optimize data access patterns".to_string(),
                ],
                scaling_recommendations: vec![
                    "Add indexes immediately".to_string(),
                    "Increase read replica capacity".to_string(),
                    "Consider query optimization".to_string(),
                ],
            });
        }

        Ok(hotspots)
    }

    fn analyze_concurrency_issues(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoConcurrencyIssue>> {
        let mut issues = Vec::new();

        if stats.concurrency_metrics.lock_contention_per_second > Self::HIGH_LOCK_CONTENTION_THRESHOLD {
            issues.push(MongoConcurrencyIssue {
                issue_type: "High Lock Contention".to_string(),
                affected_collections: vec!["Multiple collections".to_string()],
                lock_contention_level: "High".to_string(),
                blocking_operations: vec![
                    "Long-running queries".to_string(),
                    "Large update operations".to_string(),
                    "Index building".to_string(),
                ],
                wait_time_impact_ms: stats.concurrency_metrics.avg_lock_wait_time_ms,
                throughput_reduction_percentage: 25.0, // Estimate
                resolution_strategies: vec![
                    "Optimize query performance".to_string(),
                    "Break large operations into smaller chunks".to_string(),
                    "Schedule maintenance operations during low traffic".to_string(),
                ],
                prevention_measures: vec![
                    "Implement query timeouts".to_string(),
                    "Monitor long-running operations".to_string(),
                    "Use appropriate read concerns".to_string(),
                ],
                monitoring_adjustments: vec![
                    "Set lock contention alerts".to_string(),
                    "Monitor operation queue depth".to_string(),
                    "Track lock wait times".to_string(),
                ],
            });
        }

        Ok(issues)
    }

    fn generate_cache_optimizations(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoCacheOptimization>> {
        let mut optimizations = Vec::new();

        if stats.cache_performance.cache_hit_ratio < Self::LOW_CACHE_HIT_RATIO {
            optimizations.push(MongoCacheOptimization {
                optimization_type: "Increase Cache Size".to_string(),
                current_cache_efficiency: stats.cache_performance.cache_hit_ratio,
                potential_improvement_percentage: (Self::LOW_CACHE_HIT_RATIO - stats.cache_performance.cache_hit_ratio) * 100.0,
                memory_adjustment_recommendation: "Increase cache size by 50%".to_string(),
                cache_policy_suggestions: vec![
                    "Review cache eviction policies".to_string(),
                    "Optimize working set management".to_string(),
                    "Consider cache warming strategies".to_string(),
                ],
                working_set_optimization: vec![
                    "Analyze data access patterns".to_string(),
                    "Identify frequently accessed data".to_string(),
                    "Optimize data locality".to_string(),
                ],
                implementation_steps: vec![
                    "Monitor current memory usage".to_string(),
                    "Calculate optimal cache size".to_string(),
                    "Implement gradual cache size increase".to_string(),
                    "Monitor performance impact".to_string(),
                ],
                expected_performance_gain: "20-40% improvement in query response times".to_string(),
                risk_factors: vec![
                    "Increased memory usage".to_string(),
                    "Potential for memory pressure".to_string(),
                    "May require infrastructure scaling".to_string(),
                ],
            });
        }

        Ok(optimizations)
    }

    fn generate_tuning_recommendations(stats: &MongoPerformanceStats) -> ResultEP<Vec<MongoPerformanceTuningRecommendation>> {
        let mut recommendations = Vec::new();

        // Query optimization recommendation
        if stats.query_performance.avg_query_time_ms > 200.0 {
            recommendations.push(MongoPerformanceTuningRecommendation {
                tuning_area: "Query Performance".to_string(),
                current_configuration: format!("Average query time: {:.1}ms", stats.query_performance.avg_query_time_ms),
                recommended_configuration: "Optimize queries to < 100ms average".to_string(),
                performance_impact_estimate: "50-70% improvement in query response times".to_string(),
                implementation_priority: "High".to_string(),
                testing_requirements: vec![
                    "Query performance benchmarking".to_string(),
                    "Index impact analysis".to_string(),
                    "Load testing with optimized queries".to_string(),
                ],
                rollback_plan: "Revert query changes and index modifications".to_string(),
                success_criteria: vec![
                    "Average query time < 100ms".to_string(),
                    "95th percentile < 500ms".to_string(),
                    "Collection scan ratio < 10%".to_string(),
                ],
                monitoring_plan: vec![
                    "Continuous query performance monitoring".to_string(),
                    "Slow query analysis".to_string(),
                    "Index usage tracking".to_string(),
                ],
                compatibility_notes: vec![
                    "Ensure application compatibility with query changes".to_string(),
                    "Test with production data volumes".to_string(),
                ],
            });
        }

        // Connection pool optimization
        if stats.connection_performance.connection_utilization_percentage > 80.0 {
            recommendations.push(MongoPerformanceTuningRecommendation {
                tuning_area: "Connection Pool".to_string(),
                current_configuration: format!(
                    "Connection utilization: {:.1}%",
                    stats.connection_performance.connection_utilization_percentage
                ),
                recommended_configuration: "Increase connection pool size by 50%".to_string(),
                performance_impact_estimate: "Reduced connection timeouts and improved concurrency".to_string(),
                implementation_priority: "Medium".to_string(),
                testing_requirements: vec![
                    "Connection pool stress testing".to_string(),
                    "Resource usage monitoring".to_string(),
                    "Application concurrency testing".to_string(),
                ],
                rollback_plan: "Revert connection pool settings to previous values".to_string(),
                success_criteria: vec![
                    "Connection utilization < 70%".to_string(),
                    "Connection timeouts < 5 per hour".to_string(),
                    "No connection pool exhaustion".to_string(),
                ],
                monitoring_plan: vec![
                    "Connection pool metrics monitoring".to_string(),
                    "Connection timeout tracking".to_string(),
                    "Resource utilization monitoring".to_string(),
                ],
                compatibility_notes: vec![
                    "Consider server resource limits".to_string(),
                    "Test with peak load scenarios".to_string(),
                ],
            });
        }

        Ok(recommendations)
    }

    // Helper functions for detailed analysis
    fn extract_execution_stats(doc: &Document) -> (u64, u64, bool) {
        let acc = DocAccessor::new(doc);
        let mut docs_examined = 0u64;
        let mut docs_returned = 0u64;
        let mut index_used = false;

        if let Some(execution_stats) = acc.child("executionStats") {
            if let Some(examined) = execution_stats.opt_i64("totalDocsExamined") {
                docs_examined = examined as u64;
            }
            if let Some(returned) = execution_stats.opt_i64("totalDocsReturned") {
                docs_returned = returned as u64;
            }
            if let Some(winning_plan) = execution_stats.child("winningPlan")
                && let Some(stage) = winning_plan.opt_string("stage")
            {
                index_used = stage != "COLLSCAN";
            }
        }

        (docs_examined, docs_returned, index_used)
    }

    fn extract_query_pattern(doc: &Document) -> String {
        let acc = DocAccessor::new(doc);
        if let Some(command) = acc.child("command") {
            if let Some(filter) = command.child("filter") {
                format!("Filter: {}", Self::summarize_query_filter(filter.raw()))
            } else {
                "No filter specified".to_string()
            }
        } else {
            "Unknown query pattern".to_string()
        }
    }

    fn summarize_query_filter(filter: &Document) -> String {
        let mut summary = Vec::new();
        for (key, _) in filter {
            summary.push(key.clone());
        }
        if summary.len() > 3 {
            format!("{}, ... ({} fields)", summary[..3].join(", "), summary.len())
        } else {
            summary.join(", ")
        }
    }

    fn estimate_cpu_impact(execution_time_ms: f64, docs_examined: u64) -> f64 {
        let base_impact = execution_time_ms / 1000.0; // Convert to seconds
        let examination_factor = (docs_examined as f64).log10().max(1.0);
        std::cmp::min_by(base_impact * examination_factor, 10.0, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn suggest_query_optimizations(doc: &Document, execution_time_ms: f64) -> Vec<String> {
        let acc = DocAccessor::new(doc);
        let mut suggestions = Vec::new();

        if execution_time_ms > 5000.0 {
            suggestions.push("Consider breaking query into smaller operations".to_string());
        }

        if let Some(execution_stats) = acc.child("executionStats") {
            if let Some(winning_plan) = execution_stats.child("winningPlan")
                && let Some(stage) = winning_plan.opt_string("stage")
            {
                if stage == "COLLSCAN" {
                    suggestions.push("Add appropriate index to avoid collection scan".to_string());
                }
                if stage == "SORT" {
                    suggestions.push("Consider compound index with sort fields".to_string());
                }
            }

            if let (Some(examined), Some(returned)) =
                (execution_stats.opt_i64("totalDocsExamined"), execution_stats.opt_i64("totalDocsReturned"))
                && examined > returned * 10
            {
                suggestions.push("Query examines too many documents - refine filters".to_string());
            }
        }

        if suggestions.is_empty() {
            suggestions.push("Review query execution plan for optimization opportunities".to_string());
        }

        suggestions
    }

    fn recommend_indexes_for_query(doc: &Document) -> Vec<String> {
        let acc = DocAccessor::new(doc);
        let mut recommendations = Vec::new();

        if let Some(command) = acc.child("command") {
            if let Some(filter) = command.child("filter") {
                let filter_fields: Vec<String> = filter.raw().keys().map(|k| k.to_string()).collect();
                if !filter_fields.is_empty() {
                    recommendations.push(format!("Index on: {}", filter_fields.join(", ")));
                }
            }

            if let Some(sort) = command.child("sort") {
                let sort_fields: Vec<String> = sort.raw().keys().map(|k| k.to_string()).collect();
                if !sort_fields.is_empty() {
                    recommendations.push(format!("Compound index including sort fields: {}", sort_fields.join(", ")));
                }
            }
        }

        if recommendations.is_empty() {
            recommendations.push("Analyze query pattern for appropriate indexing strategy".to_string());
        }

        recommendations
    }

    fn assess_business_impact(namespace: &str, execution_time_ms: f64) -> String {
        let collection_name = namespace.split('.').next_back().unwrap_or("unknown");
        let severity = if execution_time_ms > 5000.0 {
            "High"
        } else if execution_time_ms > 1000.0 {
            "Medium"
        } else {
            "Low"
        };

        match collection_name {
            s if s.contains("user") => format!("{} impact on user experience", severity),
            s if s.contains("order") => format!("{} impact on order processing", severity),
            s if s.contains("product") => {
                format!("{} impact on product catalog performance", severity)
            }
            s if s.contains("payment") => format!("{} impact on payment processing", severity),
            _ => format!("{} impact on application performance", severity),
        }
    }
}
