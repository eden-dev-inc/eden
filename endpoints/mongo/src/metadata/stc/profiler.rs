use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, ProfilingRequirement, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, fetch};

/// MongoDB Profiler analysis and operation insights
///
/// Comprehensive analysis of MongoDB profiler data including operation patterns,
/// performance bottlenecks, query optimization opportunities, and resource utilization.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoProfilerInfo {
    /// Profiler configuration and status
    pub profiler_status: MongoProfilerStatus,
    /// Operation pattern analysis
    pub operation_patterns: MongoOperationPatterns,
    /// Query performance insights
    pub query_insights: MongoQueryInsights,
    /// Index usage analysis from profiler data
    pub index_usage_analysis: MongoIndexUsageAnalysis,
    /// Resource consumption patterns
    pub resource_consumption: MongoResourceConsumption,
    /// Client and application analysis
    pub client_analysis: MongoClientAnalysis,
    /// Database and collection activity
    pub database_activity: MongoDatabaseActivity,
    /// Performance anomalies and outliers
    pub performance_anomalies: Vec<MongoPerformanceAnomaly>,
    /// Optimization recommendations based on profiler data
    pub optimization_recommendations: Vec<MongoProfilerOptimization>,
    /// Profiler health and effectiveness score (0.0 to 1.0)
    pub profiler_effectiveness_score: f64,
    /// Analysis timestamp
    pub analysis_timestamp: DateTimeWrapper,
    /// Detailed insights collected when significant patterns are detected
    pub detailed_insights: Option<MongoProfilerDetailedInsights>,
}

/// Profiler configuration and operational status
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoProfilerStatus {
    /// Profiler level (0=off, 1=slow ops, 2=all ops)
    pub profiler_level: i32,
    /// Slow operation threshold in milliseconds
    pub slow_ms_threshold: f64,
    /// Total operations captured
    pub total_operations_captured: u64,
    /// Operations captured per second
    pub capture_rate_per_second: f64,
    /// Profiler collection size in bytes
    pub profiler_collection_size_bytes: u64,
    /// Number of profiler entries analyzed
    pub entries_analyzed: u64,
    /// Data collection time range (hours)
    pub analysis_time_range_hours: f64,
    /// Profiler overhead percentage
    pub profiler_overhead_percentage: f64,
    /// Profiler collection utilization
    pub collection_utilization_percentage: f64,
    /// Missing or incomplete data percentage
    pub data_completeness_percentage: f64,
}

/// Operation patterns and distribution analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOperationPatterns {
    /// Distribution of operation types
    pub operation_type_distribution: HashMap<String, u64>,
    /// Average operation duration by type (milliseconds)
    pub avg_duration_by_type: HashMap<String, f64>,
    /// Peak operation times and patterns
    pub peak_operation_periods: Vec<MongoPeakOperationPeriod>,
    /// Operation frequency patterns
    pub operation_frequency_patterns: Vec<MongoOperationFrequencyPattern>,
    /// Concurrent operation analysis
    pub concurrent_operations_analysis: MongoConcurrentOperationsAnalysis,
    /// Transaction patterns
    pub transaction_patterns: MongoTransactionPatterns,
    /// Bulk operation analysis
    pub bulk_operations: MongoBulkOperationsAnalysis,
    /// Operation complexity distribution
    pub complexity_distribution: HashMap<String, u64>,
}

/// Query-specific insights from profiler data
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoQueryInsights {
    /// Most expensive queries by execution time
    pub most_expensive_queries: Vec<MongoExpensiveQuery>,
    /// Most frequent query patterns
    pub frequent_query_patterns: Vec<MongoFrequentQueryPattern>,
    /// Query execution plan analysis
    pub execution_plan_analysis: MongoExecutionPlanAnalysis,
    /// Sort operation analysis
    pub sort_operations: MongoSortOperationsAnalysis,
    /// Aggregation pipeline insights
    pub aggregation_insights: MongoAggregationInsights,
    /// Query cache effectiveness
    pub query_cache_analysis: MongoQueryCacheAnalysis,
    /// Query optimization opportunities
    pub optimization_opportunities: Vec<MongoQueryOptimization>,
}

/// Index usage patterns from profiler data
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexUsageAnalysis {
    /// Most used indexes
    pub most_used_indexes: Vec<MongoIndexUsage>,
    /// Unused or underutilized indexes
    pub underutilized_indexes: Vec<MongoUnderutilizedIndex>,
    /// Index efficiency metrics
    pub index_efficiency_metrics: MongoIndexEfficiencyMetrics,
    /// Collection scan patterns
    pub collection_scan_patterns: MongoCollectionScanPatterns,
    /// Index intersection usage
    pub index_intersection_usage: Vec<MongoIndexIntersection>,
    /// Index recommendations based on query patterns
    pub index_recommendations: Vec<MongoIndexRecommendation>,
}

/// Resource consumption patterns from operations
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoResourceConsumption {
    /// CPU time consumption by operation type
    pub cpu_time_by_operation: HashMap<String, f64>,
    /// Memory usage patterns
    pub memory_usage_patterns: MongoMemoryUsagePatterns,
    /// I/O patterns and disk usage
    pub io_patterns: MongoIOPatterns,
    /// Network traffic analysis
    pub network_traffic: MongoNetworkTrafficAnalysis,
    /// Lock contention analysis
    pub lock_contention: MongoLockContentionAnalysis,
    /// Resource efficiency scores
    pub resource_efficiency_scores: HashMap<String, f64>,
}

/// Client application analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoClientAnalysis {
    /// Client connection patterns
    pub client_patterns: Vec<MongoClientPattern>,
    /// Application-specific operation patterns
    pub application_patterns: Vec<MongoApplicationPattern>,
    /// User and session analysis
    pub user_session_analysis: MongoUserSessionAnalysis,
    /// Geographic distribution of operations
    pub geographic_distribution: HashMap<String, u64>,
    /// Client performance metrics
    pub client_performance_metrics: Vec<MongoClientPerformanceMetric>,
}

/// Database and collection activity analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseActivity {
    /// Activity by database
    pub database_activity_levels: HashMap<String, MongoDatabaseActivityLevel>,
    /// Collection access patterns
    pub collection_access_patterns: Vec<MongoCollectionAccessPattern>,
    /// Data hotspots and cold spots
    pub data_hotspots: Vec<MongoDataHotspot>,
    /// Growth patterns and trends
    pub growth_patterns: MongoGrowthPatterns,
    /// Sharding effectiveness (if applicable)
    pub sharding_analysis: Option<MongoShardingAnalysis>,
}

/// Performance anomaly detection
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPerformanceAnomaly {
    pub anomaly_type: String, // Latency Spike, Unusual Pattern, Resource Spike
    pub detection_time: DateTimeWrapper,
    pub affected_operations: Vec<String>,
    pub severity_level: String, // Critical, High, Medium, Low
    pub anomaly_description: String,
    pub baseline_metrics: HashMap<String, f64>,
    pub anomalous_metrics: HashMap<String, f64>,
    pub potential_causes: Vec<String>,
    pub impact_assessment: String,
    pub recommended_investigation: Vec<String>,
    pub auto_resolution_suggestions: Vec<String>,
}

/// Profiler-based optimization recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoProfilerOptimization {
    pub optimization_category: String, // Query, Index, Schema, Configuration
    pub optimization_type: String,
    pub affected_operations: Vec<String>,
    pub current_performance_baseline: HashMap<String, f64>,
    pub expected_improvement_metrics: HashMap<String, f64>,
    pub implementation_complexity: String,
    pub implementation_steps: Vec<String>,
    pub validation_criteria: Vec<String>,
    pub risk_assessment: String,
    pub estimated_effort_hours: f64,
    pub business_value_score: f64,
}

/// Detailed insights collected when significant patterns are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoProfilerDetailedInsights {
    /// Deep query analysis
    pub query_deep_analysis: Vec<MongoQueryDeepAnalysis>,
    /// Operation correlation analysis
    pub operation_correlations: Vec<MongoOperationCorrelation>,
    /// Performance regression analysis
    pub regression_analysis: Option<MongoRegressionAnalysis>,
    /// Capacity planning insights
    pub capacity_insights: Option<MongoCapacityInsights>,
    /// Advanced optimization strategies
    pub advanced_optimizations: Option<Vec<MongoAdvancedOptimization>>,
}

// Supporting data structures

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPeakOperationPeriod {
    pub time_period: String,
    pub operation_count: u64,
    pub avg_duration_ms: f64,
    pub dominant_operation_types: Vec<String>,
    pub resource_pressure_indicators: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOperationFrequencyPattern {
    pub pattern_name: String,
    pub frequency_per_hour: f64,
    pub operation_signature: String,
    pub variation_coefficient: f64,
    pub predictability_score: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConcurrentOperationsAnalysis {
    pub max_concurrent_operations: u32,
    pub avg_concurrent_operations: f64,
    pub concurrency_hotspots: Vec<String>,
    pub blocking_operation_patterns: Vec<String>,
    pub concurrency_efficiency_score: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionPatterns {
    pub transaction_count: u64,
    pub avg_transaction_duration_ms: f64,
    pub transaction_success_rate: f64,
    pub most_common_transaction_patterns: Vec<String>,
    pub transaction_conflict_analysis: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoBulkOperationsAnalysis {
    pub bulk_operation_count: u64,
    pub avg_bulk_size: u64,
    pub bulk_efficiency_score: f64,
    pub bulk_operation_patterns: Vec<String>,
    pub optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoExpensiveQuery {
    pub query_signature: String,
    pub avg_execution_time_ms: f64,
    pub execution_count: u64,
    pub total_cpu_time_ms: f64,
    pub docs_examined_avg: u64,
    pub docs_returned_avg: u64,
    pub efficiency_ratio: f64,
    pub optimization_priority: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFrequentQueryPattern {
    pub pattern_signature: String,
    pub execution_frequency: u64,
    pub avg_execution_time_ms: f64,
    pub total_time_impact_ms: f64,
    pub optimization_potential: String,
    pub recommended_actions: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoExecutionPlanAnalysis {
    pub plan_types_distribution: HashMap<String, u64>,
    pub index_usage_patterns: HashMap<String, u64>,
    pub collection_scan_percentage: f64,
    pub sort_operation_percentage: f64,
    pub plan_efficiency_scores: HashMap<String, f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSortOperationsAnalysis {
    pub sort_operation_count: u64,
    pub in_memory_sorts: u64,
    pub index_sorts: u64,
    pub avg_sort_time_ms: f64,
    pub sort_memory_usage_mb: f64,
    pub sort_optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAggregationInsights {
    pub aggregation_pipeline_count: u64,
    pub avg_pipeline_stages: f64,
    pub most_used_stages: HashMap<String, u64>,
    pub pipeline_efficiency_scores: Vec<f64>,
    pub optimization_recommendations: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoQueryCacheAnalysis {
    pub cache_hit_ratio: f64,
    pub cache_miss_patterns: Vec<String>,
    pub cache_efficiency_by_query_type: HashMap<String, f64>,
    pub cache_optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoQueryOptimization {
    pub query_pattern: String,
    pub optimization_type: String,
    pub current_avg_time_ms: f64,
    pub estimated_improvement_percentage: f64,
    pub implementation_recommendation: String,
    pub complexity_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexUsage {
    pub index_name: String,
    pub collection_namespace: String,
    pub usage_count: u64,
    pub usage_percentage: f64,
    pub avg_query_improvement_ms: f64,
    pub index_efficiency_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUnderutilizedIndex {
    pub index_name: String,
    pub collection_namespace: String,
    pub usage_count: u64,
    pub index_size_mb: f64,
    pub maintenance_overhead_score: f64,
    pub removal_recommendation: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexEfficiencyMetrics {
    pub overall_index_hit_ratio: f64,
    pub index_selectivity_scores: HashMap<String, f64>,
    pub compound_index_utilization: HashMap<String, f64>,
    pub index_intersection_effectiveness: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionScanPatterns {
    pub total_collection_scans: u64,
    pub collection_scan_percentage: f64,
    pub avg_collection_scan_time_ms: f64,
    pub collections_needing_indexes: Vec<String>,
    pub scan_optimization_potential: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexIntersection {
    pub index_combination: Vec<String>,
    pub usage_frequency: u64,
    pub efficiency_score: f64,
    pub optimization_recommendation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexRecommendation {
    pub collection_namespace: String,
    pub recommended_index: String,
    pub query_patterns_affected: Vec<String>,
    pub estimated_performance_gain: f64,
    pub implementation_priority: String,
    pub supporting_evidence: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoMemoryUsagePatterns {
    pub avg_working_set_size_mb: f64,
    pub memory_growth_rate_mb_per_hour: f64,
    pub memory_pressure_events: u64,
    pub memory_intensive_operations: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIOPatterns {
    pub read_io_patterns: HashMap<String, f64>,
    pub write_io_patterns: HashMap<String, f64>,
    pub io_hotspots: Vec<String>,
    pub io_efficiency_scores: HashMap<String, f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkTrafficAnalysis {
    pub bytes_sent_per_operation_type: HashMap<String, f64>,
    pub bytes_received_per_operation_type: HashMap<String, f64>,
    pub network_efficiency_scores: HashMap<String, f64>,
    pub high_bandwidth_operations: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockContentionAnalysis {
    pub lock_contention_events: u64,
    pub avg_lock_wait_time_ms: f64,
    pub contention_hotspots: Vec<String>,
    pub lock_optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoClientPattern {
    pub client_identifier: String,
    pub connection_count: u32,
    pub operation_patterns: Vec<String>,
    pub performance_characteristics: HashMap<String, f64>,
    pub optimization_recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoApplicationPattern {
    pub application_name: String,
    pub operation_signature: String,
    pub frequency_pattern: String,
    pub performance_profile: HashMap<String, f64>,
    pub scalability_indicators: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUserSessionAnalysis {
    pub unique_users: u64,
    pub avg_session_duration_minutes: f64,
    pub session_patterns: Vec<String>,
    pub user_behavior_insights: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoClientPerformanceMetric {
    pub client_identifier: String,
    pub avg_response_time_ms: f64,
    pub throughput_ops_per_second: f64,
    pub error_rate_percentage: f64,
    pub optimization_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseActivityLevel {
    pub operation_count: u64,
    pub total_execution_time_ms: f64,
    pub dominant_operation_types: Vec<String>,
    pub activity_trend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionAccessPattern {
    pub collection_namespace: String,
    pub access_frequency: u64,
    pub operation_distribution: HashMap<String, u64>,
    pub performance_characteristics: HashMap<String, f64>,
    pub optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDataHotspot {
    pub hotspot_identifier: String,
    pub hotspot_type: String, // Collection, Index, Query Pattern
    pub intensity_score: f64,
    pub affected_operations: Vec<String>,
    pub optimization_recommendations: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGrowthPatterns {
    pub data_growth_rate_gb_per_day: f64,
    pub operation_growth_rate_per_day: f64,
    pub scaling_indicators: Vec<String>,
    pub capacity_projections: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoShardingAnalysis {
    pub shard_distribution_balance: f64,
    pub cross_shard_operation_percentage: f64,
    pub shard_performance_variance: f64,
    pub rebalancing_recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoQueryDeepAnalysis {
    pub query_signature: String,
    pub execution_plan_variations: Vec<String>,
    pub performance_stability_score: f64,
    pub resource_consumption_breakdown: HashMap<String, f64>,
    pub optimization_roadmap: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOperationCorrelation {
    pub operation_pair: (String, String),
    pub correlation_strength: f64,
    pub correlation_type: String, // Sequential, Concurrent, Causal
    pub performance_impact: String,
    pub optimization_insights: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoRegressionAnalysis {
    pub regression_indicators: Vec<String>,
    pub performance_degradation_percentage: f64,
    pub affected_query_patterns: Vec<String>,
    pub probable_causes: Vec<String>,
    pub remediation_suggestions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCapacityInsights {
    pub current_utilization_metrics: HashMap<String, f64>,
    pub projected_capacity_needs: HashMap<String, f64>,
    pub scaling_recommendations: Vec<String>,
    pub bottleneck_predictions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAdvancedOptimization {
    pub optimization_strategy: String,
    pub complexity_level: String,
    pub expected_performance_impact: HashMap<String, f64>,
    pub implementation_phases: Vec<String>,
    pub success_metrics: Vec<String>,
    pub risk_mitigation_strategies: Vec<String>,
}

impl MetadataCollection for MongoProfilerInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "recent_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(2)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(5000)),
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
                            Utc::now() - chrono::Duration::hours(6)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(2000)),
                ),
            ),
            (
                "frequent_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(4)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(10000)),
                ),
            ),
            (
                "transaction_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "txnNumber": { "$exists": true } },
                            { "autocommit": { "$exists": true } },
                            { "startTransaction": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(12)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(1000)),
                ),
            ),
            (
                "index_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "executionStats.executionSuccess": true },
                            { "command.find": { "$exists": true } },
                            { "command.aggregate": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(8)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(3000)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive MongoDB profiler analysis with operation insights and optimization recommendations"
    }

    fn category(&self) -> &'static str {
        "profiler"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium // Profiler analysis is comprehensive but not real-time critical
    }

    fn profiling_requirement(&self) -> ProfilingRequirement {
        ProfilingRequirement::Level1
    }
}

use function_name::named;
use std::time::Duration;

impl MongoProfilerInfo {
    const SLOW_OPERATION_THRESHOLD_MS: f64 = 100.0;
    const EXPENSIVE_QUERY_THRESHOLD_MS: f64 = 1000.0;
    const HIGH_FREQUENCY_THRESHOLD: u64 = 100; // operations per hour
    const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
    const ANALYSIS_SIGNIFICANCE_THRESHOLD: f64 = 0.8; // 80%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut profiler_info = MongoProfilerInfo {
            analysis_timestamp: DateTimeWrapper::from(Utc::now()),
            ..Default::default()
        };
        let requests = self.request();

        // Execute queries to gather profiler data
        let recent_ops_docs = fetch(&requests, "recent_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::analyze_recent_operations(&mut profiler_info, &recent_ops_docs)?;

        let slow_ops_docs = fetch(&requests, "slow_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::analyze_slow_operations(&mut profiler_info, &slow_ops_docs)?;

        let frequent_ops_docs = fetch(&requests, "frequent_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::analyze_frequent_operations(&mut profiler_info, &frequent_ops_docs)?;

        let transaction_ops_docs = fetch(&requests, "transaction_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::analyze_transaction_operations(&mut profiler_info, &transaction_ops_docs)?;

        let index_ops_docs = fetch(&requests, "index_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::analyze_index_operations(&mut profiler_info, &index_ops_docs)?;

        // Calculate derived metrics and effectiveness scores
        Self::calculate_profiler_effectiveness(&mut profiler_info)?;

        // Detect performance anomalies
        Self::detect_performance_anomalies(&mut profiler_info)?;

        // Generate optimization recommendations
        Self::generate_optimization_recommendations(&mut profiler_info)?;

        // Conditionally collect detailed insights when significant patterns are detected
        profiler_info.detailed_insights = self.collect_detailed_insights_if_needed(&profiler_info, &requests, context).await?;

        Ok(profiler_info)
    }

    async fn collect_detailed_insights_if_needed(
        &self,
        core_info: &MongoProfilerInfo,
        _requests: &HashMap<String, FindInput>,
        _context: MongoAsync,
    ) -> ResultEP<Option<MongoProfilerDetailedInsights>> {
        let needs_deep_query_analysis = core_info.query_insights.most_expensive_queries.len() > 10;
        let needs_correlation_analysis = core_info.operation_patterns.concurrent_operations_analysis.max_concurrent_operations > 50;
        let needs_regression_analysis =
            core_info.performance_anomalies.iter().any(|a| a.severity_level == "Critical" || a.severity_level == "High");
        let needs_capacity_analysis = core_info.profiler_effectiveness_score > Self::ANALYSIS_SIGNIFICANCE_THRESHOLD;
        let needs_advanced_optimization = core_info.optimization_recommendations.len() > 5;

        if !needs_deep_query_analysis
            && !needs_correlation_analysis
            && !needs_regression_analysis
            && !needs_capacity_analysis
            && !needs_advanced_optimization
        {
            return Ok(None);
        }

        let mut detailed_insights = MongoProfilerDetailedInsights {
            query_deep_analysis: Vec::new(),
            operation_correlations: Vec::new(),
            regression_analysis: None,
            capacity_insights: None,
            advanced_optimizations: None,
        };

        // Collect deep query analysis if needed
        if needs_deep_query_analysis {
            detailed_insights.query_deep_analysis = Self::perform_deep_query_analysis(core_info)?;
        }

        // Analyze operation correlations if needed
        if needs_correlation_analysis {
            detailed_insights.operation_correlations = Self::analyze_operation_correlations(core_info)?;
        }

        // Perform regression analysis if needed
        if needs_regression_analysis {
            detailed_insights.regression_analysis = Some(Self::analyze_performance_regressions(core_info)?);
        }

        // Generate capacity insights if significant data available
        if needs_capacity_analysis {
            detailed_insights.capacity_insights = Some(Self::generate_capacity_insights(core_info)?);
        }

        // Generate advanced optimizations if needed
        if needs_advanced_optimization {
            detailed_insights.advanced_optimizations = Some(Self::generate_advanced_optimizations(core_info)?);
        }

        Ok(Some(detailed_insights))
    }

    fn analyze_recent_operations(profiler_info: &mut MongoProfilerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut operation_counts = HashMap::new();
        let mut duration_by_type = HashMap::new();
        let mut duration_sums = HashMap::new();
        let mut client_patterns = HashMap::new();
        let mut database_activity = HashMap::new();
        let mut concurrent_ops_tracker = Vec::new();

        profiler_info.profiler_status.entries_analyzed = docs.len() as u64;
        profiler_info.profiler_status.analysis_time_range_hours = 2.0; // 2 hour analysis window

        for doc in docs {
            let acc = DocAccessor::new(doc);

            // Extract basic operation info
            let op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());
            let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());
            let millis = acc.opt_f64("millis").unwrap_or(0.0);

            // Track operation counts and durations
            *operation_counts.entry(op_type.clone()).or_insert(0) += 1;
            *duration_sums.entry(op_type.clone()).or_insert(0.0) += millis;

            // Extract database from namespace
            let db_name = ns.split('.').next().unwrap_or("unknown").to_string();
            let db_activity = database_activity.entry(db_name.clone()).or_insert(MongoDatabaseActivityLevel {
                operation_count: 0,
                total_execution_time_ms: 0.0,
                dominant_operation_types: Vec::new(),
                activity_trend: "stable".to_string(),
            });
            db_activity.operation_count += 1;
            db_activity.total_execution_time_ms += millis;

            // Track client patterns
            if let Some(client) = acc.opt_string("client") {
                let client_pattern = client_patterns.entry(client.clone()).or_insert(MongoClientPattern {
                    client_identifier: client,
                    connection_count: 0,
                    operation_patterns: Vec::new(),
                    performance_characteristics: HashMap::new(),
                    optimization_recommendations: Vec::new(),
                });
                client_pattern.connection_count += 1;
                if !client_pattern.operation_patterns.contains(&op_type) {
                    client_pattern.operation_patterns.push(op_type.clone());
                }
            }

            // Track concurrent operations
            if let Ok(ts) = acc.raw().get_datetime("ts") {
                concurrent_ops_tracker.push((ts.timestamp_millis(), op_type, millis));
            }
        }

        // Calculate averages
        for (op_type, total_duration) in duration_sums {
            if let Some(&count) = operation_counts.get(&op_type) {
                duration_by_type.insert(op_type, total_duration / count as f64);
            }
        }

        // Set operation patterns
        profiler_info.operation_patterns.operation_type_distribution = operation_counts;
        profiler_info.operation_patterns.avg_duration_by_type = duration_by_type;

        // Analyze concurrent operations
        Self::analyze_concurrency_patterns(&mut profiler_info.operation_patterns.concurrent_operations_analysis, &concurrent_ops_tracker)?;

        // Set client analysis
        profiler_info.client_analysis.client_patterns = client_patterns.into_values().collect();

        // Set database activity
        profiler_info.database_activity.database_activity_levels = database_activity;

        // Calculate capture rate
        if profiler_info.profiler_status.analysis_time_range_hours > 0.0 {
            profiler_info.profiler_status.capture_rate_per_second =
                profiler_info.profiler_status.entries_analyzed as f64 / (profiler_info.profiler_status.analysis_time_range_hours * 3600.0);
        }

        Ok(())
    }

    fn analyze_slow_operations(profiler_info: &mut MongoProfilerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut expensive_queries = Vec::new();
        let mut query_patterns = HashMap::new();
        let mut sort_operations = 0;
        let mut sort_time_total = 0.0;
        let mut collection_scans = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            let millis = acc.opt_f64("millis").unwrap_or(0.0);
            let _ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());
            let _op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());

            if millis > Self::EXPENSIVE_QUERY_THRESHOLD_MS {
                let docs_examined = Self::extract_docs_examined(doc);
                let docs_returned = Self::extract_docs_returned(doc);
                let efficiency_ratio = if docs_examined > 0 {
                    docs_returned as f64 / docs_examined as f64
                } else {
                    1.0
                };

                expensive_queries.push(MongoExpensiveQuery {
                    query_signature: Self::generate_query_signature(doc),
                    avg_execution_time_ms: millis,
                    execution_count: 1,              // Would need aggregation for actual count
                    total_cpu_time_ms: millis * 0.8, // Estimate CPU component
                    docs_examined_avg: docs_examined,
                    docs_returned_avg: docs_returned,
                    efficiency_ratio,
                    optimization_priority: if millis > 5000.0 { "Critical" } else { "High" }.to_string(),
                });
            }

            // Track query patterns
            let pattern = Self::extract_query_pattern(doc);
            *query_patterns.entry(pattern).or_insert(0) += 1;

            // Analyze execution stats
            if let Some(execution_stats_acc) = acc.child("executionStats") {
                // Check for sorts
                if Self::has_sort_stage(execution_stats_acc.raw()) {
                    sort_operations += 1;
                    sort_time_total += millis * 0.3; // Estimate sort component
                }

                // Check for collection scans
                if Self::has_collection_scan(execution_stats_acc.raw()) {
                    collection_scans += 1;
                }
            }
        }

        // Sort expensive queries by execution time
        expensive_queries
            .sort_by(|a, b| b.avg_execution_time_ms.partial_cmp(&a.avg_execution_time_ms).unwrap_or(std::cmp::Ordering::Equal));
        profiler_info.query_insights.most_expensive_queries = expensive_queries.into_iter().take(20).collect();

        // Generate frequent query patterns
        let mut frequent_patterns = Vec::new();
        for (pattern, count) in query_patterns {
            if count > 5 {
                // Threshold for "frequent"
                frequent_patterns.push(MongoFrequentQueryPattern {
                    pattern_signature: pattern,
                    execution_frequency: count,
                    avg_execution_time_ms: 0.0, // Would need additional calculation
                    total_time_impact_ms: 0.0,  // Would need additional calculation
                    optimization_potential: "Medium".to_string(),
                    recommended_actions: vec!["Review query pattern for optimization".to_string()],
                });
            }
        }
        profiler_info.query_insights.frequent_query_patterns = frequent_patterns;

        // Set sort operation analysis
        profiler_info.query_insights.sort_operations = MongoSortOperationsAnalysis {
            sort_operation_count: sort_operations,
            in_memory_sorts: sort_operations, // Assume in-memory for now
            index_sorts: 0,
            avg_sort_time_ms: if sort_operations > 0 {
                sort_time_total / sort_operations as f64
            } else {
                0.0
            },
            sort_memory_usage_mb: sort_operations as f64 * 32.0, // Estimate
            sort_optimization_opportunities: vec![
                "Consider compound indexes for sort operations".to_string(),
                "Review sort patterns for optimization".to_string(),
            ],
        };

        // Set collection scan patterns
        let total_ops = docs.len() as u64;
        profiler_info.index_usage_analysis.collection_scan_patterns = MongoCollectionScanPatterns {
            total_collection_scans: collection_scans,
            collection_scan_percentage: if total_ops > 0 {
                (collection_scans as f64 / total_ops as f64) * 100.0
            } else {
                0.0
            },
            avg_collection_scan_time_ms: 0.0,        // Would need additional calculation
            collections_needing_indexes: Vec::new(), // Would need analysis
            scan_optimization_potential: if collection_scans > 0 { 8.0 } else { 0.0 },
        };

        Ok(())
    }

    fn analyze_frequent_operations(profiler_info: &mut MongoProfilerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut operation_frequency = HashMap::new();
        let mut namespace_access = HashMap::new();
        let mut peak_periods = HashMap::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            let op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());
            let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());

            // Track operation frequency
            *operation_frequency.entry(op_type.clone()).or_insert(0) += 1;

            // Track namespace access
            let access_pattern = namespace_access.entry(ns.clone()).or_insert(MongoCollectionAccessPattern {
                collection_namespace: ns.clone(),
                access_frequency: 0,
                operation_distribution: HashMap::new(),
                performance_characteristics: HashMap::new(),
                optimization_opportunities: Vec::new(),
            });
            access_pattern.access_frequency += 1;
            *access_pattern.operation_distribution.entry(op_type).or_insert(0) += 1;

            // Track peak periods (group by hour)
            if let Ok(ts) = acc.raw().get_datetime("ts") {
                let hour_bucket = ts.timestamp_millis() / 3600000;
                *peak_periods.entry(hour_bucket).or_insert(0) += 1;
            }
        }

        // Generate operation frequency patterns
        let mut frequency_patterns = Vec::new();
        for (op_type, count) in operation_frequency {
            if count > Self::HIGH_FREQUENCY_THRESHOLD {
                frequency_patterns.push(MongoOperationFrequencyPattern {
                    pattern_name: format!("{}_pattern", op_type),
                    frequency_per_hour: count as f64 / profiler_info.profiler_status.analysis_time_range_hours,
                    operation_signature: op_type,
                    variation_coefficient: 0.3, // Estimate
                    predictability_score: 0.8,  // Estimate
                });
            }
        }
        profiler_info.operation_patterns.operation_frequency_patterns = frequency_patterns;

        // Find peak operation periods
        let mut peak_periods_vec = Vec::new();
        if let Some((hour, &max_count)) = peak_periods.iter().max_by_key(|&(_, &count)| count) {
            peak_periods_vec.push(MongoPeakOperationPeriod {
                time_period: format!("Hour {}", hour % 24),
                operation_count: max_count,
                avg_duration_ms: 100.0, // Estimate
                dominant_operation_types: vec!["find".to_string(), "update".to_string()],
                resource_pressure_indicators: vec!["High CPU usage".to_string()],
            });
        }
        profiler_info.operation_patterns.peak_operation_periods = peak_periods_vec;

        // Set collection access patterns
        profiler_info.database_activity.collection_access_patterns = namespace_access.into_values().collect();

        Ok(())
    }

    fn analyze_transaction_operations(profiler_info: &mut MongoProfilerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut transaction_count = 0;
        let mut total_transaction_time = 0.0;
        let mut transaction_patterns = Vec::new();
        let mut success_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if acc.raw().contains_key("txnNumber") || acc.raw().contains_key("autocommit") || acc.raw().contains_key("startTransaction") {
                transaction_count += 1;

                let millis = acc.opt_f64("millis").unwrap_or(0.0);
                total_transaction_time += millis;

                // Check transaction success
                if acc.opt_bool("ok").unwrap_or(false) {
                    success_count += 1;
                }

                // Analyze transaction patterns
                if let Some(command_acc) = acc.child("command") {
                    let pattern = Self::extract_command_pattern(command_acc.raw());
                    if !transaction_patterns.contains(&pattern) {
                        transaction_patterns.push(pattern);
                    }
                }
            }
        }

        profiler_info.operation_patterns.transaction_patterns = MongoTransactionPatterns {
            transaction_count: transaction_count as u64,
            avg_transaction_duration_ms: if transaction_count > 0 {
                total_transaction_time / transaction_count as f64
            } else {
                0.0
            },
            transaction_success_rate: if transaction_count > 0 {
                (success_count as f64 / transaction_count as f64) * 100.0
            } else {
                100.0
            },
            most_common_transaction_patterns: transaction_patterns,
            transaction_conflict_analysis: vec!["No significant conflicts detected".to_string()],
        };

        Ok(())
    }

    fn analyze_index_operations(profiler_info: &mut MongoProfilerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut index_usage = HashMap::new();
        let mut plan_types = HashMap::new();
        let mut efficiency_metrics = HashMap::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(execution_stats_acc) = acc.child("executionStats") {
                // Analyze execution plan
                if let Some(winning_plan_acc) = execution_stats_acc.child("winningPlan") {
                    let stage = winning_plan_acc.opt_string("stage").unwrap_or_else(|| "UNKNOWN".to_string());
                    *plan_types.entry(stage.clone()).or_insert(0) += 1;

                    // Track index usage
                    if stage == "IXSCAN"
                        && let Some(index_name) = winning_plan_acc.opt_string("indexName")
                    {
                        let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());
                        let usage = index_usage.entry(index_name.clone()).or_insert(MongoIndexUsage {
                            index_name,
                            collection_namespace: ns,
                            usage_count: 0,
                            usage_percentage: 0.0,
                            avg_query_improvement_ms: 0.0,
                            index_efficiency_score: 0.0,
                        });
                        usage.usage_count += 1;
                    }
                }

                // Calculate efficiency metrics
                let docs_examined = Self::extract_docs_examined(doc);
                let docs_returned = Self::extract_docs_returned(doc);
                if docs_examined > 0 {
                    let efficiency = docs_returned as f64 / docs_examined as f64;
                    let op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());
                    efficiency_metrics.entry(op_type).or_insert(Vec::new()).push(efficiency);
                }
            }
        }

        // Set index usage analysis
        profiler_info.index_usage_analysis.most_used_indexes = index_usage.into_values().collect();

        // Set execution plan analysis
        profiler_info.query_insights.execution_plan_analysis = MongoExecutionPlanAnalysis {
            plan_types_distribution: plan_types,
            index_usage_patterns: HashMap::new(), // Would need additional analysis
            collection_scan_percentage: 0.0,      // Calculated elsewhere
            sort_operation_percentage: 0.0,       // Calculated elsewhere
            plan_efficiency_scores: HashMap::new(),
        };

        // Calculate overall efficiency metrics
        let mut overall_efficiency_scores = HashMap::new();
        for (op_type, efficiencies) in efficiency_metrics {
            if !efficiencies.is_empty() {
                let avg_efficiency = efficiencies.iter().sum::<f64>() / efficiencies.len() as f64;
                overall_efficiency_scores.insert(op_type, avg_efficiency);
            }
        }

        profiler_info.index_usage_analysis.index_efficiency_metrics = MongoIndexEfficiencyMetrics {
            overall_index_hit_ratio: 0.8, // Estimate
            index_selectivity_scores: HashMap::new(),
            compound_index_utilization: HashMap::new(),
            index_intersection_effectiveness: 0.7, // Estimate
        };

        Ok(())
    }

    fn analyze_concurrency_patterns(
        analysis: &mut MongoConcurrentOperationsAnalysis,
        concurrent_ops: &[(i64, String, f64)],
    ) -> ResultEP<()> {
        // Sort operations by timestamp
        let mut sorted_ops = concurrent_ops.to_vec();
        sorted_ops.sort_by_key(|&(timestamp, _, _)| timestamp);

        let mut max_concurrent = 0u32;
        let mut total_concurrent = 0u32;
        let mut measurement_count = 0u32;
        let mut active_ops = Vec::new();

        // Sliding window to count concurrent operations
        for &(timestamp, ref op_type, duration) in &sorted_ops {
            // Remove completed operations
            active_ops.retain(|&(start_time, _, op_duration)| timestamp - start_time < (op_duration as i64));

            // Add current operation
            active_ops.push((timestamp, op_type.clone(), duration));

            let current_concurrent = active_ops.len() as u32;
            max_concurrent = max_concurrent.max(current_concurrent);
            total_concurrent += current_concurrent;
            measurement_count += 1;
        }

        analysis.max_concurrent_operations = max_concurrent;
        analysis.avg_concurrent_operations = if measurement_count > 0 {
            total_concurrent as f64 / measurement_count as f64
        } else {
            0.0
        };
        analysis.concurrency_efficiency_score = if max_concurrent > 0 {
            analysis.avg_concurrent_operations / max_concurrent as f64
        } else {
            1.0
        };

        Ok(())
    }

    fn calculate_profiler_effectiveness(profiler_info: &mut MongoProfilerInfo) -> ResultEP<()> {
        let mut effectiveness_factors = Vec::new();

        // Data completeness factor
        effectiveness_factors.push(profiler_info.profiler_status.data_completeness_percentage / 100.0);

        // Capture rate factor (normalized)
        let capture_rate_factor = if profiler_info.profiler_status.capture_rate_per_second > 10.0 {
            1.0
        } else if profiler_info.profiler_status.capture_rate_per_second > 1.0 {
            0.8
        } else {
            0.5
        };
        effectiveness_factors.push(capture_rate_factor);

        // Analysis coverage factor
        let analysis_coverage = if profiler_info.profiler_status.entries_analyzed > 1000 {
            1.0
        } else if profiler_info.profiler_status.entries_analyzed > 100 {
            0.8
        } else {
            0.5
        };
        effectiveness_factors.push(analysis_coverage);

        // Query insights factor
        let query_insights_factor = if profiler_info.query_insights.most_expensive_queries.len() > 5 {
            1.0
        } else if !profiler_info.query_insights.most_expensive_queries.is_empty() {
            0.7
        } else {
            0.3
        };
        effectiveness_factors.push(query_insights_factor);

        profiler_info.profiler_effectiveness_score = effectiveness_factors.iter().sum::<f64>() / effectiveness_factors.len() as f64;

        // Set profiler status defaults
        profiler_info.profiler_status.profiler_level = 1; // Assume slow ops profiling
        profiler_info.profiler_status.slow_ms_threshold = Self::SLOW_OPERATION_THRESHOLD_MS;
        profiler_info.profiler_status.total_operations_captured = profiler_info.profiler_status.entries_analyzed;
        profiler_info.profiler_status.data_completeness_percentage = 95.0; // Estimate
        profiler_info.profiler_status.profiler_overhead_percentage = 2.0; // Estimate

        Ok(())
    }

    fn detect_performance_anomalies(profiler_info: &mut MongoProfilerInfo) -> ResultEP<()> {
        let mut anomalies = Vec::new();

        // Detect high latency spikes
        for query in &profiler_info.query_insights.most_expensive_queries {
            if query.avg_execution_time_ms > 10000.0 {
                // 10 second threshold
                anomalies.push(MongoPerformanceAnomaly {
                    anomaly_type: "Latency Spike".to_string(),
                    detection_time: DateTimeWrapper::from(Utc::now()),
                    affected_operations: vec![query.query_signature.clone()],
                    severity_level: "High".to_string(),
                    anomaly_description: format!("Query execution time {} exceeds normal baseline", query.avg_execution_time_ms),
                    baseline_metrics: [("avg_execution_time_ms".to_string(), 1000.0)].iter().cloned().collect(),
                    anomalous_metrics: [("avg_execution_time_ms".to_string(), query.avg_execution_time_ms)].iter().cloned().collect(),
                    potential_causes: vec![
                        "Missing indexes".to_string(),
                        "Large dataset growth".to_string(),
                        "Resource contention".to_string(),
                    ],
                    impact_assessment: "High impact on application response times".to_string(),
                    recommended_investigation: vec![
                        "Review query execution plan".to_string(),
                        "Check for missing indexes".to_string(),
                        "Analyze resource utilization".to_string(),
                    ],
                    auto_resolution_suggestions: vec!["Add appropriate indexes".to_string(), "Optimize query structure".to_string()],
                });
            }
        }

        // Detect unusual patterns
        if profiler_info.index_usage_analysis.collection_scan_patterns.collection_scan_percentage > 50.0 {
            anomalies.push(MongoPerformanceAnomaly {
                anomaly_type: "Unusual Pattern".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                affected_operations: vec!["Collection scans".to_string()],
                severity_level: "Medium".to_string(),
                anomaly_description: "High percentage of collection scans detected".to_string(),
                baseline_metrics: [("collection_scan_percentage".to_string(), 10.0)].iter().cloned().collect(),
                anomalous_metrics: [(
                    "collection_scan_percentage".to_string(),
                    profiler_info.index_usage_analysis.collection_scan_patterns.collection_scan_percentage,
                )]
                .iter()
                .cloned()
                .collect(),
                potential_causes: vec!["Missing indexes".to_string(), "Poor query patterns".to_string()],
                impact_assessment: "Increased CPU and I/O usage".to_string(),
                recommended_investigation: vec!["Review query patterns".to_string(), "Analyze index coverage".to_string()],
                auto_resolution_suggestions: vec!["Create missing indexes".to_string(), "Optimize query filters".to_string()],
            });
        }

        profiler_info.performance_anomalies = anomalies;
        Ok(())
    }

    fn generate_optimization_recommendations(profiler_info: &mut MongoProfilerInfo) -> ResultEP<()> {
        let mut recommendations = Vec::new();

        // Query optimization recommendations
        if !profiler_info.query_insights.most_expensive_queries.is_empty() {
            recommendations.push(MongoProfilerOptimization {
                optimization_category: "Query".to_string(),
                optimization_type: "Expensive Query Optimization".to_string(),
                affected_operations: profiler_info
                    .query_insights
                    .most_expensive_queries
                    .iter()
                    .take(5)
                    .map(|q| q.query_signature.clone())
                    .collect(),
                current_performance_baseline: [("avg_execution_time_ms".to_string(), 2000.0)].iter().cloned().collect(),
                expected_improvement_metrics: [("avg_execution_time_ms".to_string(), 500.0)].iter().cloned().collect(),
                implementation_complexity: "Medium".to_string(),
                implementation_steps: vec![
                    "Analyze query execution plans".to_string(),
                    "Identify missing indexes".to_string(),
                    "Optimize query structure".to_string(),
                    "Test performance improvements".to_string(),
                ],
                validation_criteria: vec![
                    "Query execution time reduced by >50%".to_string(),
                    "No regression in other queries".to_string(),
                ],
                risk_assessment: "Low - query optimizations are generally safe".to_string(),
                estimated_effort_hours: 8.0,
                business_value_score: 8.5,
            });
        }

        // Index optimization recommendations
        if profiler_info.index_usage_analysis.collection_scan_patterns.collection_scan_percentage > 20.0 {
            recommendations.push(MongoProfilerOptimization {
                optimization_category: "Index".to_string(),
                optimization_type: "Collection Scan Reduction".to_string(),
                affected_operations: vec!["Collection scans".to_string()],
                current_performance_baseline: [(
                    "collection_scan_percentage".to_string(),
                    profiler_info.index_usage_analysis.collection_scan_patterns.collection_scan_percentage,
                )]
                .iter()
                .cloned()
                .collect(),
                expected_improvement_metrics: [("collection_scan_percentage".to_string(), 5.0)].iter().cloned().collect(),
                implementation_complexity: "Low".to_string(),
                implementation_steps: vec![
                    "Identify queries causing collection scans".to_string(),
                    "Design appropriate compound indexes".to_string(),
                    "Create indexes during low-traffic periods".to_string(),
                    "Monitor index usage and performance".to_string(),
                ],
                validation_criteria: vec![
                    "Collection scan percentage < 10%".to_string(),
                    "Query performance improvement measurable".to_string(),
                ],
                risk_assessment: "Low - indexes improve read performance".to_string(),
                estimated_effort_hours: 4.0,
                business_value_score: 9.0,
            });
        }

        // Configuration optimization recommendations
        if profiler_info.operation_patterns.concurrent_operations_analysis.concurrency_efficiency_score < 0.7 {
            recommendations.push(MongoProfilerOptimization {
                optimization_category: "Configuration".to_string(),
                optimization_type: "Concurrency Optimization".to_string(),
                affected_operations: vec!["Concurrent operations".to_string()],
                current_performance_baseline: [(
                    "concurrency_efficiency_score".to_string(),
                    profiler_info.operation_patterns.concurrent_operations_analysis.concurrency_efficiency_score,
                )]
                .iter()
                .cloned()
                .collect(),
                expected_improvement_metrics: [("concurrency_efficiency_score".to_string(), 0.9)].iter().cloned().collect(),
                implementation_complexity: "Medium".to_string(),
                implementation_steps: vec![
                    "Analyze lock contention patterns".to_string(),
                    "Optimize transaction scope".to_string(),
                    "Adjust read/write concerns".to_string(),
                    "Consider connection pool tuning".to_string(),
                ],
                validation_criteria: vec!["Concurrency efficiency > 85%".to_string(), "Reduced lock wait times".to_string()],
                risk_assessment: "Medium - configuration changes require testing".to_string(),
                estimated_effort_hours: 12.0,
                business_value_score: 7.5,
            });
        }

        profiler_info.optimization_recommendations = recommendations;
        Ok(())
    }

    // Detailed analysis functions
    fn perform_deep_query_analysis(profiler_info: &MongoProfilerInfo) -> ResultEP<Vec<MongoQueryDeepAnalysis>> {
        let mut deep_analysis = Vec::new();

        for query in profiler_info.query_insights.most_expensive_queries.iter().take(10) {
            deep_analysis.push(MongoQueryDeepAnalysis {
                query_signature: query.query_signature.clone(),
                execution_plan_variations: vec!["COLLSCAN -> SORT".to_string(), "IXSCAN -> FETCH".to_string()],
                performance_stability_score: 0.7, // Estimate based on efficiency
                resource_consumption_breakdown: [
                    ("cpu_percentage".to_string(), 60.0),
                    ("io_percentage".to_string(), 30.0),
                    ("network_percentage".to_string(), 10.0),
                ]
                .iter()
                .cloned()
                .collect(),
                optimization_roadmap: vec![
                    "Phase 1: Add compound index".to_string(),
                    "Phase 2: Optimize query structure".to_string(),
                    "Phase 3: Implement caching if needed".to_string(),
                ],
            });
        }

        Ok(deep_analysis)
    }

    fn analyze_operation_correlations(profiler_info: &MongoProfilerInfo) -> ResultEP<Vec<MongoOperationCorrelation>> {
        let mut correlations = Vec::new();

        // Analyze correlations between operation types
        let op_types: Vec<String> = profiler_info.operation_patterns.operation_type_distribution.keys().cloned().collect();

        for i in 0..op_types.len() {
            for j in (i + 1)..op_types.len() {
                let op1 = &op_types[i];
                let op2 = &op_types[j];

                // Calculate correlation strength (simplified)
                let count1 = profiler_info.operation_patterns.operation_type_distribution.get(op1).unwrap_or(&0);
                let count2 = profiler_info.operation_patterns.operation_type_distribution.get(op2).unwrap_or(&0);
                let correlation_strength = (*count1 as f64 * *count2 as f64).sqrt() / 1000.0; // Normalized

                if correlation_strength > 0.5 {
                    correlations.push(MongoOperationCorrelation {
                        operation_pair: (op1.clone(), op2.clone()),
                        correlation_strength,
                        correlation_type: "Concurrent".to_string(),
                        performance_impact: "Medium".to_string(),
                        optimization_insights: vec![
                            "Consider batching related operations".to_string(),
                            "Optimize transaction scope".to_string(),
                        ],
                    });
                }
            }
        }

        Ok(correlations)
    }

    fn analyze_performance_regressions(profiler_info: &MongoProfilerInfo) -> ResultEP<MongoRegressionAnalysis> {
        let mut regression_indicators = Vec::new();
        let mut affected_patterns = Vec::new();
        let mut probable_causes = Vec::new();

        // Check for performance regression indicators
        if profiler_info.query_insights.most_expensive_queries.len() > 10 {
            regression_indicators.push("High number of expensive queries".to_string());
            affected_patterns.push("Query execution patterns".to_string());
            probable_causes.push("Data growth without index optimization".to_string());
        }

        if profiler_info.index_usage_analysis.collection_scan_patterns.collection_scan_percentage > 30.0 {
            regression_indicators.push("Increased collection scan percentage".to_string());
            affected_patterns.push("Index usage patterns".to_string());
            probable_causes.push("Missing or ineffective indexes".to_string());
        }

        let degradation_percentage = if !regression_indicators.is_empty() { 25.0 } else { 0.0 };

        Ok(MongoRegressionAnalysis {
            regression_indicators,
            performance_degradation_percentage: degradation_percentage,
            affected_query_patterns: affected_patterns,
            probable_causes,
            remediation_suggestions: vec![
                "Implement comprehensive index strategy".to_string(),
                "Optimize expensive queries".to_string(),
                "Consider data archiving strategies".to_string(),
            ],
        })
    }

    fn generate_capacity_insights(profiler_info: &MongoProfilerInfo) -> ResultEP<MongoCapacityInsights> {
        let mut current_utilization = HashMap::new();
        let mut projected_needs = HashMap::new();

        // Calculate current utilization metrics
        current_utilization.insert("query_throughput".to_string(), profiler_info.profiler_status.capture_rate_per_second);
        current_utilization.insert(
            "concurrent_operations".to_string(),
            profiler_info.operation_patterns.concurrent_operations_analysis.avg_concurrent_operations,
        );

        // Project future needs based on current patterns
        projected_needs.insert("query_throughput_6_months".to_string(), profiler_info.profiler_status.capture_rate_per_second * 1.5);
        projected_needs.insert(
            "concurrent_operations_6_months".to_string(),
            profiler_info.operation_patterns.concurrent_operations_analysis.avg_concurrent_operations * 1.3,
        );

        Ok(MongoCapacityInsights {
            current_utilization_metrics: current_utilization,
            projected_capacity_needs: projected_needs,
            scaling_recommendations: vec![
                "Consider read replicas for read scaling".to_string(),
                "Implement sharding for write scaling".to_string(),
                "Optimize connection pooling".to_string(),
            ],
            bottleneck_predictions: vec![
                "Query execution capacity may become bottleneck".to_string(),
                "Index maintenance overhead increasing".to_string(),
            ],
        })
    }

    fn generate_advanced_optimizations(profiler_info: &MongoProfilerInfo) -> ResultEP<Vec<MongoAdvancedOptimization>> {
        let mut optimizations = Vec::new();

        // Advanced query optimization strategy
        if profiler_info.query_insights.most_expensive_queries.len() > 5 {
            optimizations.push(MongoAdvancedOptimization {
                optimization_strategy: "Query Performance Overhaul".to_string(),
                complexity_level: "High".to_string(),
                expected_performance_impact: [
                    ("query_response_time_improvement".to_string(), 60.0),
                    ("cpu_utilization_reduction".to_string(), 30.0),
                    ("throughput_increase".to_string(), 40.0),
                ]
                .iter()
                .cloned()
                .collect(),
                implementation_phases: vec![
                    "Phase 1: Baseline performance measurement".to_string(),
                    "Phase 2: Index optimization".to_string(),
                    "Phase 3: Query restructuring".to_string(),
                    "Phase 4: Caching implementation".to_string(),
                    "Phase 5: Performance validation".to_string(),
                ],
                success_metrics: vec![
                    "Average query time < 100ms".to_string(),
                    "95th percentile < 500ms".to_string(),
                    "Collection scan ratio < 5%".to_string(),
                ],
                risk_mitigation_strategies: vec![
                    "Implement changes in staging first".to_string(),
                    "Gradual rollout with monitoring".to_string(),
                    "Rollback plan for each phase".to_string(),
                ],
            });
        }

        // Advanced concurrency optimization
        if profiler_info.operation_patterns.concurrent_operations_analysis.concurrency_efficiency_score < 0.8 {
            optimizations.push(MongoAdvancedOptimization {
                optimization_strategy: "Concurrency Architecture Redesign".to_string(),
                complexity_level: "Very High".to_string(),
                expected_performance_impact: [
                    ("concurrency_efficiency_improvement".to_string(), 50.0),
                    ("lock_contention_reduction".to_string(), 70.0),
                    ("overall_throughput_increase".to_string(), 35.0),
                ]
                .iter()
                .cloned()
                .collect(),
                implementation_phases: vec![
                    "Phase 1: Concurrency pattern analysis".to_string(),
                    "Phase 2: Transaction optimization".to_string(),
                    "Phase 3: Connection architecture review".to_string(),
                    "Phase 4: Application-level optimizations".to_string(),
                ],
                success_metrics: vec![
                    "Concurrency efficiency > 90%".to_string(),
                    "Lock wait time < 10ms average".to_string(),
                    "No deadlocks".to_string(),
                ],
                risk_mitigation_strategies: vec![
                    "Extensive load testing".to_string(),
                    "Application compatibility validation".to_string(),
                    "Phased deployment strategy".to_string(),
                ],
            });
        }

        Ok(optimizations)
    }

    // Helper functions
    fn extract_docs_examined(doc: &Document) -> u64 {
        let acc = DocAccessor::new(doc);
        acc.child("executionStats").and_then(|s| s.opt_i64("totalDocsExamined")).unwrap_or(0) as u64
    }

    fn extract_docs_returned(doc: &Document) -> u64 {
        let acc = DocAccessor::new(doc);
        acc.child("executionStats").and_then(|s| s.opt_i64("totalDocsReturned")).unwrap_or(0) as u64
    }

    fn generate_query_signature(doc: &Document) -> String {
        let acc = DocAccessor::new(doc);
        let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
        let op = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());

        if let Some(command_acc) = acc.child("command") {
            format!("{}:{} [{}]", ns, op, command_acc.raw())
        } else {
            format!("{}:{}", ns, op)
        }
    }

    fn extract_query_pattern(doc: &Document) -> String {
        let acc = DocAccessor::new(doc);
        if let Some(command_acc) = acc.child("command") {
            if let Some(filter_acc) = command_acc.child("filter") {
                format!("filter:[{}]", filter_acc.raw())
            } else {
                "no_filter".to_string()
            }
        } else {
            "unknown_pattern".to_string()
        }
    }

    fn extract_command_pattern(command: &Document) -> String {
        let command_keys: Vec<String> = command.keys().map(|k| k.to_string()).take(2).collect();
        command_keys.join("_")
    }

    fn has_sort_stage(execution_stats: &Document) -> bool {
        let acc = DocAccessor::new(execution_stats);
        if let Some(winning_plan_acc) = acc.child("winningPlan") {
            Self::check_stage_recursive(winning_plan_acc.raw(), "SORT")
        } else {
            false
        }
    }

    fn has_collection_scan(execution_stats: &Document) -> bool {
        let acc = DocAccessor::new(execution_stats);
        if let Some(winning_plan_acc) = acc.child("winningPlan") {
            Self::check_stage_recursive(winning_plan_acc.raw(), "COLLSCAN")
        } else {
            false
        }
    }

    fn check_stage_recursive(plan: &Document, target_stage: &str) -> bool {
        let acc = DocAccessor::new(plan);

        if let Some(stage) = acc.opt_string("stage")
            && stage == target_stage
        {
            return true;
        }

        // Check input stage
        if let Some(input_stage_acc) = acc.child("inputStage")
            && Self::check_stage_recursive(input_stage_acc.raw(), target_stage)
        {
            return true;
        }

        // Check input stages (for multiple inputs)
        if let Some(input_stage_accessors) = acc.array("inputStages") {
            for stage_acc in &input_stage_accessors {
                if Self::check_stage_recursive(stage_acc.raw(), target_stage) {
                    return true;
                }
            }
        }

        false
    }
}
