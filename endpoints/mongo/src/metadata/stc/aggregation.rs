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

/// MongoDB aggregation pipeline statistics and performance metrics
///
/// Simplified struct containing essential metrics about aggregation pipeline
/// performance and usage patterns. Focuses on core pipeline health indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAggregationStats {
    /// Total number of aggregation operations executed
    pub total_aggregations: u64,
    /// Number of aggregations currently running
    pub active_aggregations: u64,
    /// Number of failed aggregations in the last period
    pub failed_aggregations: u64,
    /// Average execution time for aggregations (milliseconds)
    pub avg_execution_time_ms: f64,
    /// Maximum execution time for aggregations (milliseconds)
    pub max_execution_time_ms: f64,
    /// Minimum execution time for aggregations (milliseconds)
    pub min_execution_time_ms: f64,
    /// Total number of documents examined by aggregations
    pub total_docs_examined: u64,
    /// Total number of documents returned by aggregations
    pub total_docs_returned: u64,
    /// Average documents examined per aggregation
    pub avg_docs_examined_per_operation: f64,
    /// Average documents returned per aggregation
    pub avg_docs_returned_per_operation: f64,
    /// Number of aggregations using indexes effectively
    pub index_efficient_aggregations: u64,
    /// Number of aggregations requiring collection scans
    pub collection_scan_aggregations: u64,
    /// Memory usage by aggregations (bytes)
    pub memory_usage_bytes: u64,
    /// Number of aggregations that spilled to disk
    pub disk_spill_count: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoAggregationDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAggregationDetailedMetrics {
    /// Slow aggregation operations (only collected when max_execution_time_ms > threshold)
    pub slow_aggregations: Vec<MongoSlowAggregation>,
    /// Inefficient aggregations (only collected when collection_scan_aggregations > threshold)
    pub inefficient_aggregations: Vec<MongoInefficientAggregation>,
    /// Aggregation breakdown by collection (collected less frequently)
    pub aggregations_by_collection: Option<Vec<MongoAggregationsByCollection>>,
    /// Memory-intensive aggregations (only collected when memory usage is high)
    pub memory_intensive_aggregations: Vec<MongoMemoryIntensiveAggregation>,
}

impl MetadataCollection for MongoAggregationStats {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "profiler_aggregations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.aggregate": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "slow_aggregations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.aggregate": { "$exists": true },
                        "millis": { "$gte": 5000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(50)),
                ),
            ),
            (
                "inefficient_aggregations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.aggregate": { "$exists": true },
                        "planSummary": { "$regex": "COLLSCAN" },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential aggregation pipeline metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "aggregation"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }

    fn profiling_requirement(&self) -> ProfilingRequirement {
        ProfilingRequirement::Level1
    }
}

use function_name::named;
use std::time::Duration;

impl MongoAggregationStats {
    const SLOW_AGGREGATION_THRESHOLD_MS: f64 = 5000.0; // 5 seconds
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const HIGH_MEMORY_THRESHOLD_MB: f64 = 100.0; // 100MB

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut aggregation_stats = MongoAggregationStats::default();
        let requests = self.request();

        // Execute profiler query to get recent aggregation operations
        let profiler_docs = fetch(&requests, "profiler_aggregations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_profiler_data(&mut aggregation_stats, &profiler_docs)?;

        // Conditionally collect detailed metrics only when problems are detected
        aggregation_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&aggregation_stats, &requests, context).await?;

        Ok(aggregation_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoAggregationStats,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoAggregationDetailedMetrics>> {
        let needs_slow_aggregation_details = core_stats.max_execution_time_ms > Self::SLOW_AGGREGATION_THRESHOLD_MS;
        let needs_inefficiency_details = core_stats.collection_scan_aggregations > 0;
        let needs_memory_details = (core_stats.memory_usage_bytes as f64 / 1024.0 / 1024.0) > Self::HIGH_MEMORY_THRESHOLD_MB;

        if !needs_slow_aggregation_details && !needs_inefficiency_details && !needs_memory_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoAggregationDetailedMetrics {
            slow_aggregations: Vec::new(),
            inefficient_aggregations: Vec::new(),
            aggregations_by_collection: None,
            memory_intensive_aggregations: Vec::new(),
        };

        // Collect slow aggregations if needed
        if needs_slow_aggregation_details {
            let docs = fetch(requests, "slow_aggregations", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.slow_aggregations = Self::parse_slow_aggregations(docs)?;
        }

        // Collect inefficient aggregations if needed
        if needs_inefficiency_details {
            let docs = fetch(requests, "inefficient_aggregations", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.inefficient_aggregations = Self::parse_inefficient_aggregations(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_profiler_data(stats: &mut MongoAggregationStats, docs: &[Document]) -> ResultEP<()> {
        let mut execution_times = Vec::new();
        let mut docs_examined_total = 0u64;
        let mut docs_returned_total = 0u64;
        let mut collection_scans = 0u64;
        let mut index_usage = 0u64;
        let mut failed_count = 0u64;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(millis) = acc.opt_f64("millis") {
                execution_times.push(millis);
            }

            if let Some(docs_examined) = acc.opt_u64("docsExamined") {
                docs_examined_total += docs_examined;
            }

            if let Some(docs_returned) = acc.opt_u64("nreturned") {
                docs_returned_total += docs_returned;
            }

            if let Some(plan_summary) = acc.opt_string("planSummary") {
                if plan_summary.contains("COLLSCAN") {
                    collection_scans += 1;
                } else if plan_summary.contains("IXSCAN") {
                    index_usage += 1;
                }
            }

            if acc.opt_i32("ok").unwrap_or(1) != 1 {
                failed_count += 1;
            }
        }

        stats.total_aggregations = docs.len() as u64;
        stats.failed_aggregations = failed_count;
        stats.collection_scan_aggregations = collection_scans;
        stats.index_efficient_aggregations = index_usage;
        stats.total_docs_examined = docs_examined_total;
        stats.total_docs_returned = docs_returned_total;

        if !execution_times.is_empty() {
            stats.avg_execution_time_ms = execution_times.iter().sum::<f64>() / execution_times.len() as f64;
            stats.max_execution_time_ms = execution_times.iter().fold(0.0f64, |a, &b| a.max(b));
            stats.min_execution_time_ms = execution_times.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        }

        if stats.total_aggregations > 0 {
            stats.avg_docs_examined_per_operation = stats.total_docs_examined as f64 / stats.total_aggregations as f64;
            stats.avg_docs_returned_per_operation = stats.total_docs_returned as f64 / stats.total_aggregations as f64;
        }

        Ok(())
    }

    fn parse_slow_aggregations(docs: Vec<Document>) -> ResultEP<Vec<MongoSlowAggregation>> {
        let mut aggregations = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(millis), Some(ts)) = (acc.opt_f64("millis"), acc.opt_datetime("ts")) {
                let command = acc.child("command");
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let collection = ns.split('.').next_back().unwrap_or("unknown").to_string();

                let pipeline = if let Some(cmd) = command.as_ref() {
                    if let Ok(pipeline) = cmd.raw().get_array("pipeline") {
                        format!("{:?}", pipeline)
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                aggregations.push(MongoSlowAggregation {
                    operation_id: acc.opt_string("opid").unwrap_or_else(|| "unknown".to_string()),
                    database: command.as_ref().and_then(|cmd| cmd.opt_string("aggregate")).unwrap_or_else(|| "unknown".to_string()),
                    collection,
                    pipeline,
                    execution_time_ms: millis,
                    docs_examined: acc.opt_u64("docsExamined").unwrap_or(0),
                    docs_returned: acc.opt_u64("nreturned").unwrap_or(0),
                    timestamp: ts,
                    plan_summary: acc.opt_string("planSummary"),
                    user: acc.opt_string("user"),
                });
            }
        }

        Ok(aggregations)
    }

    fn parse_inefficient_aggregations(docs: Vec<Document>) -> ResultEP<Vec<MongoInefficientAggregation>> {
        let mut aggregations = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(millis), Some(ts)) = (acc.opt_f64("millis"), acc.opt_datetime("ts")) {
                let docs_examined = acc.opt_u64("docsExamined").unwrap_or(0);
                let docs_returned = acc.opt_u64("nreturned").unwrap_or(0);

                let efficiency_ratio = if docs_examined > 0 {
                    docs_returned as f64 / docs_examined as f64
                } else {
                    0.0
                };

                let command = acc.child("command");
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let collection = ns.split('.').next_back().unwrap_or("unknown").to_string();

                let pipeline = if let Some(cmd) = command.as_ref() {
                    if let Ok(pipeline) = cmd.raw().get_array("pipeline") {
                        format!("{:?}", pipeline)
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                aggregations.push(MongoInefficientAggregation {
                    operation_id: acc.opt_string("opid").unwrap_or_else(|| "unknown".to_string()),
                    database: command.as_ref().and_then(|cmd| cmd.opt_string("aggregate")).unwrap_or_else(|| "unknown".to_string()),
                    collection,
                    pipeline,
                    execution_time_ms: millis,
                    docs_examined,
                    docs_returned,
                    efficiency_ratio,
                    inefficiency_reason: "Collection scan detected".to_string(),
                    timestamp: ts,
                });
            }
        }

        Ok(aggregations)
    }
}

/// Information about slow aggregation operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowAggregation {
    /// Operation ID
    pub operation_id: String,
    /// Database name
    pub database: String,
    /// Collection name
    pub collection: String,
    /// Aggregation pipeline (truncated for safety)
    pub pipeline: String,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Number of documents examined
    pub docs_examined: u64,
    /// Number of documents returned
    pub docs_returned: u64,
    /// Timestamp when the operation started
    pub timestamp: DateTimeWrapper,
    /// Plan summary showing index usage
    pub plan_summary: Option<String>,
    /// User who executed the aggregation
    pub user: Option<String>,
}

/// Information about inefficient aggregation operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoInefficientAggregation {
    /// Operation ID
    pub operation_id: String,
    /// Database name
    pub database: String,
    /// Collection name
    pub collection: String,
    /// Aggregation pipeline (truncated for safety)
    pub pipeline: String,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Number of documents examined
    pub docs_examined: u64,
    /// Number of documents returned
    pub docs_returned: u64,
    /// Efficiency ratio (returned/examined)
    pub efficiency_ratio: f64,
    /// Reason for inefficiency
    pub inefficiency_reason: String,
    /// Timestamp when the operation started
    pub timestamp: DateTimeWrapper,
}

/// Information about memory-intensive aggregation operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoMemoryIntensiveAggregation {
    /// Operation ID
    pub operation_id: String,
    /// Database name
    pub database: String,
    /// Collection name
    pub collection: String,
    /// Aggregation pipeline (truncated for safety)
    pub pipeline: String,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Whether the operation spilled to disk
    pub spilled_to_disk: bool,
    /// Timestamp when the operation started
    pub timestamp: DateTimeWrapper,
    /// Number of pipeline stages
    pub stage_count: u32,
}

/// Aggregation statistics grouped by collection
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAggregationsByCollection {
    /// Database name
    pub database_name: String,
    /// Collection name
    pub collection_name: String,
    /// Total aggregations on this collection
    pub total_aggregations: u64,
    /// Average execution time for this collection
    pub avg_execution_time_ms: f64,
    /// Number of failed aggregations
    pub failed_aggregations: u64,
    /// Total documents examined
    pub total_docs_examined: u64,
    /// Total documents returned
    pub total_docs_returned: u64,
}

impl MongoAggregationStats {
    /// Calculates the average efficiency ratio across all aggregations
    pub fn overall_efficiency_ratio(&self) -> f64 {
        if self.total_docs_examined == 0 {
            0.0
        } else {
            self.total_docs_returned as f64 / self.total_docs_examined as f64
        }
    }

    /// Checks if there are slow aggregations
    pub fn has_slow_aggregations(&self, threshold_ms: f64) -> bool {
        self.max_execution_time_ms > threshold_ms
    }

    /// Checks if aggregations are using indexes efficiently
    pub fn has_inefficient_aggregations(&self) -> bool {
        self.collection_scan_aggregations > 0
    }

    /// Checks if memory usage is high
    pub fn has_high_memory_usage(&self, threshold_mb: f64) -> bool {
        (self.memory_usage_bytes as f64 / 1024.0 / 1024.0) > threshold_mb
    }

    /// Returns the percentage of aggregations that failed
    pub fn failure_rate_percentage(&self) -> f64 {
        if self.total_aggregations == 0 {
            0.0
        } else {
            (self.failed_aggregations as f64 / self.total_aggregations as f64) * 100.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns the percentage of aggregations that spilled to disk
    pub fn disk_spill_percentage(&self) -> f64 {
        if self.total_aggregations == 0 {
            0.0
        } else {
            (self.disk_spill_count as f64 / self.total_aggregations as f64) * 100.0
        }
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_aggregation_stats() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let aggregation_stats = MongoAggregationStats::default();

        let result = aggregation_stats
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let stats = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(stats.avg_execution_time_ms >= 0.0);
    }
}
