use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, ProfilingRequirement, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, fetch};
use crate::metadata::capabilities::MONGO_REPLICA_SET;

/// MongoDB Oplog (Operations Log) statistics and performance metrics
///
/// Comprehensive struct containing essential metrics about oplog health,
/// replication lag, write patterns, and replica set synchronization.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogInfo {
    /// Total oplog size in bytes
    pub oplog_size_bytes: u64,
    /// Current oplog usage in bytes
    pub oplog_used_bytes: u64,
    /// Oplog utilization percentage (0.0 to 100.0)
    pub oplog_utilization_percentage: f64,
    /// Estimated time until oplog is full (hours)
    pub oplog_time_remaining_hours: f64,
    /// Average operations per second being logged
    pub avg_ops_per_second: f64,
    /// Peak operations per second observed
    pub peak_ops_per_second: f64,
    /// Current replication lag in milliseconds (max across secondaries)
    pub max_replication_lag_ms: f64,
    /// Average replication lag across all secondaries
    pub avg_replication_lag_ms: f64,
    /// Number of replica set members
    pub replica_set_member_count: u32,
    /// Number of healthy replica members
    pub healthy_replica_members: u32,
    /// Oplog window duration in hours (time span of operations in oplog)
    pub oplog_window_hours: f64,
    /// Average document size in oplog entries (bytes)
    pub avg_oplog_entry_size_bytes: f64,
    /// Write operations count in current window
    pub write_operations_count: u64,
    /// Insert operations percentage
    pub insert_operations_percentage: f64,
    /// Update operations percentage
    pub update_operations_percentage: f64,
    /// Delete operations percentage
    pub delete_operations_percentage: f64,
    /// Command operations percentage (DDL, admin commands)
    pub command_operations_percentage: f64,
    /// Number of large transactions in oplog
    pub large_transaction_count: u64,
    /// Average transaction size in bytes
    pub avg_transaction_size_bytes: f64,
    /// Oplog entry growth rate (entries per hour)
    pub oplog_growth_rate_per_hour: f64,
    /// Oplog size growth rate (bytes per hour)
    pub oplog_size_growth_rate_bytes_per_hour: f64,
    /// Number of oplog entries with high impact (large updates, bulk operations)
    pub high_impact_operations_count: u64,
    /// Percentage of operations that are multi-document transactions
    pub transaction_operations_percentage: f64,
    /// Collections with highest write activity
    pub most_active_collections: Vec<String>,
    /// Oplog health score (0.0 to 1.0, higher is better)
    pub oplog_health_score: f64,
    /// Number of oplog entries analyzed
    pub analyzed_entries_count: u64,
    /// Timestamp of oldest entry in oplog
    pub oldest_oplog_entry: Option<DateTimeWrapper>,
    /// Timestamp of newest entry in oplog
    pub newest_oplog_entry: Option<DateTimeWrapper>,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoOplogDetailedMetrics>,
}

/// Detailed oplog metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogDetailedMetrics {
    /// Replication lag issues and analysis
    pub replication_issues: Vec<MongoReplicationIssue>,
    /// Large operations that may cause problems
    pub large_operations: Vec<MongoLargeOperation>,
    /// Oplog capacity and sizing recommendations
    pub capacity_recommendations: Vec<MongoOplogCapacityRecommendation>,
    /// Write pattern analysis and optimization suggestions
    pub write_pattern_analysis: Vec<MongoWritePatternAnalysis>,
    /// Transaction impact on oplog
    pub transaction_analysis: Option<MongoTransactionAnalysis>,
    /// Collection-specific oplog impact
    pub collection_impact_analysis: Option<Vec<MongoCollectionOplogImpact>>,
    /// Performance bottlenecks in replication
    pub performance_issues: Option<Vec<MongoOplogPerformanceIssue>>,
    /// Oplog configuration recommendations
    pub configuration_recommendations: Option<Vec<MongoOplogConfigRecommendation>>,
}

/// Information about replication lag and synchronization issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoReplicationIssue {
    pub issue_type: String,      // Lag, Stale Secondary, Network Issues, Initial Sync
    pub affected_member: String, // hostname:port or member ID
    pub member_state: String,    // PRIMARY, SECONDARY, RECOVERING, etc.
    pub lag_duration_ms: f64,
    pub lag_severity: String, // Critical, High, Medium, Low
    pub operations_behind: u64,
    pub sync_source: String,
    pub last_heartbeat: DateTimeWrapper,
    pub health_status: String,
    pub estimated_catch_up_time_minutes: f64,
    pub root_cause_indicators: Vec<String>,
    pub recommended_actions: Vec<String>,
    pub impact_assessment: String,
    pub monitoring_suggestions: Vec<String>,
}

/// Information about large operations that impact oplog
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLargeOperation {
    pub operation_id: String,
    pub operation_type: String, // insert, update, delete, command
    pub namespace: String,      // database.collection
    pub operation_size_bytes: u64,
    pub timestamp: DateTimeWrapper,
    pub execution_time_ms: f64,
    pub oplog_entries_count: u32, // For transactions, number of oplog entries generated
    pub affected_documents: u64,
    pub operation_complexity: String,  // Simple, Complex, Bulk, Transaction
    pub impact_on_secondaries: String, // Low, Medium, High
    pub optimization_suggestions: Vec<String>,
    pub potential_issues: Vec<String>,
    pub business_context: String,
}

/// Oplog capacity and sizing recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogCapacityRecommendation {
    pub recommendation_type: String, // Size Increase, Cleanup, Monitoring
    pub current_capacity_gb: f64,
    pub recommended_capacity_gb: f64,
    pub rationale: String,
    pub urgency_level: String, // Critical, High, Medium, Low
    pub estimated_window_improvement_hours: f64,
    pub implementation_steps: Vec<String>,
    pub risks_and_considerations: Vec<String>,
    pub cost_implications: String,
    pub timeline_estimate: String,
    pub success_metrics: Vec<String>,
    pub monitoring_requirements: Vec<String>,
}

/// Write pattern analysis and optimization suggestions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWritePatternAnalysis {
    pub pattern_type: String, // Bulk Writes, High Frequency, Large Documents, Transactions
    pub pattern_description: String,
    pub frequency_analysis: String,
    pub collections_affected: Vec<String>,
    pub oplog_impact_score: f64, // 0.0 to 10.0
    pub performance_implications: Vec<String>,
    pub optimization_opportunities: Vec<String>,
    pub recommended_strategies: Vec<String>,
    pub implementation_complexity: String,
    pub expected_benefits: Vec<String>,
    pub monitoring_metrics: Vec<String>,
}

/// Transaction impact analysis on oplog
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionAnalysis {
    pub total_transactions: u64,
    pub avg_transaction_duration_ms: f64,
    pub avg_transaction_oplog_entries: f64,
    pub largest_transaction_entries: u64,
    pub largest_transaction_size_bytes: u64,
    pub cross_shard_transactions: u64,
    pub failed_transactions: u64,
    pub transaction_oplog_overhead_percentage: f64,
    pub peak_concurrent_transactions: u32,
    pub transaction_patterns: Vec<String>,
    pub performance_recommendations: Vec<String>,
    pub optimization_strategies: Vec<String>,
}

/// Collection-specific oplog impact analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionOplogImpact {
    pub collection_namespace: String,
    pub operations_count: u64,
    pub total_oplog_bytes: u64,
    pub avg_operation_size_bytes: f64,
    pub operation_type_distribution: HashMap<String, f64>, // insert: 50%, update: 30%, etc.
    pub impact_percentage: f64,                            // Percentage of total oplog usage
    pub write_frequency_pattern: String,                   // Steady, Bursty, Periodic
    pub optimization_potential: String,                    // High, Medium, Low
    pub recommended_indexes: Vec<String>,
    pub suggested_optimizations: Vec<String>,
    pub monitoring_recommendations: Vec<String>,
}

/// Oplog performance issues and bottlenecks
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogPerformanceIssue {
    pub issue_type: String,
    pub severity: String, // Critical, High, Medium, Low
    pub affected_operations: u64,
    pub performance_impact_description: String,
    pub root_cause_analysis: String,
    pub detection_time: DateTimeWrapper,
    pub estimated_resolution_time: String,
    pub business_impact: String,
    pub technical_details: String,
    pub recommended_solution: String,
    pub prevention_strategies: Vec<String>,
    pub monitoring_improvements: Vec<String>,
}

/// Oplog configuration recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOplogConfigRecommendation {
    pub configuration_area: String,
    pub current_setting: String,
    pub recommended_setting: String,
    pub justification: String,
    pub expected_improvement: String,
    pub implementation_risk: String,
    pub testing_requirements: Vec<String>,
    pub rollback_procedure: String,
    pub monitoring_after_change: Vec<String>,
    pub compatibility_considerations: Vec<String>,
}

impl MetadataCollection for MongoOplogInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "oplog_status".to_string(),
                FindInput::new(
                    "local".to_string(),
                    "oplog.rs".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {})),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(1000)),
                ),
            ),
            (
                "replica_set_status".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.replSetGetStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "oplog_entries".to_string(),
                FindInput::new(
                    "local".to_string(),
                    "oplog.rs".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ts": { "$gte": mongodb::bson::Timestamp {
                            time: (Utc::now() - chrono::Duration::hours(1)).timestamp() as u32,
                            increment: 0
                        }}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(5000)),
                ),
            ),
            (
                "recent_large_ops".to_string(),
                FindInput::new(
                    "local".to_string(),
                    "oplog.rs".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ts": { "$gte": mongodb::bson::Timestamp {
                            time: (Utc::now() - chrono::Duration::hours(6)).timestamp() as u32,
                            increment: 0
                        }},
                        "$or": [
                            { "o.size": { "$gte": 16 * 1024 * 1024 } }, // 16MB+ operations
                            { "txnNumber": { "$exists": true } }, // Transactions
                            { "prevOpTime": { "$exists": true } } // Multi-oplog operations
                        ]
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(1000)),
                ),
            ),
            (
                "oplog_collection_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.collStats": "oplog.rs",
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
        "Return comprehensive oplog health, replication lag, and write pattern metrics"
    }

    fn category(&self) -> &'static str {
        "replication"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium // Oplog analysis is important but not as time-sensitive as network
    }

    fn profiling_requirement(&self) -> ProfilingRequirement {
        ProfilingRequirement::Level1
    }
}

use function_name::named;
use mongodb::bson;
use std::time::Duration;

impl MongoOplogInfo {
    const HIGH_REPLICATION_LAG_THRESHOLD_MS: f64 = 5000.0; // 5 seconds
    const CRITICAL_REPLICATION_LAG_THRESHOLD_MS: f64 = 30000.0; // 30 seconds
    const HIGH_OPLOG_UTILIZATION_THRESHOLD: f64 = 85.0; // 85%
    const LOW_OPLOG_WINDOW_THRESHOLD_HOURS: f64 = 4.0; // 4 hours
    const LARGE_OPERATION_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024; // 16MB
    const QUERY_TIMEOUT: Duration = Duration::from_secs(20);
    const MAX_DETAILED_RESULTS: usize = 200;
    const HIGH_OPS_PER_SECOND_THRESHOLD: f64 = 1000.0;
    const POOR_OPLOG_HEALTH_THRESHOLD: f64 = 0.7; // 70%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        if !capabilities.has(&MONGO_REPLICA_SET) {
            return Ok(MongoOplogInfo::default());
        }

        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut oplog_stats = MongoOplogInfo::default();
        let requests = self.request();

        // Execute queries to get oplog information
        let oplog_docs = fetch(&requests, "oplog_status", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_oplog_entries(&mut oplog_stats, &oplog_docs)?;

        let replica_status_docs = fetch(&requests, "replica_set_status", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_replica_set_status(&mut oplog_stats, &replica_status_docs)?;

        let recent_entries_docs = fetch(&requests, "oplog_entries", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_recent_oplog_entries(&mut oplog_stats, &recent_entries_docs)?;

        let large_ops_docs = fetch(&requests, "recent_large_ops", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_large_operations(&mut oplog_stats, &large_ops_docs)?;

        let collection_stats_docs = fetch(&requests, "oplog_collection_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_oplog_collection_stats(&mut oplog_stats, &collection_stats_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut oplog_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        oplog_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&oplog_stats, &requests, context).await?;

        Ok(oplog_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoOplogInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoOplogDetailedMetrics>> {
        let needs_replication_analysis = core_stats.max_replication_lag_ms > Self::HIGH_REPLICATION_LAG_THRESHOLD_MS;
        let needs_capacity_analysis = core_stats.oplog_utilization_percentage > Self::HIGH_OPLOG_UTILIZATION_THRESHOLD;
        let needs_large_ops_analysis = core_stats.large_transaction_count > 10;
        let needs_performance_analysis = core_stats.avg_ops_per_second > Self::HIGH_OPS_PER_SECOND_THRESHOLD;
        let needs_window_analysis = core_stats.oplog_window_hours < Self::LOW_OPLOG_WINDOW_THRESHOLD_HOURS;
        let needs_health_analysis = core_stats.oplog_health_score < Self::POOR_OPLOG_HEALTH_THRESHOLD;

        if !needs_replication_analysis
            && !needs_capacity_analysis
            && !needs_large_ops_analysis
            && !needs_performance_analysis
            && !needs_window_analysis
            && !needs_health_analysis
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoOplogDetailedMetrics {
            replication_issues: Vec::new(),
            large_operations: Vec::new(),
            capacity_recommendations: Vec::new(),
            write_pattern_analysis: Vec::new(),
            transaction_analysis: None,
            collection_impact_analysis: None,
            performance_issues: None,
            configuration_recommendations: None,
        };

        // Collect replication issues if needed
        if needs_replication_analysis {
            detailed_metrics.replication_issues = Self::analyze_replication_issues(core_stats)?;
        }

        // Collect large operations analysis if needed
        if needs_large_ops_analysis {
            let docs = fetch(requests, "recent_large_ops", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.large_operations = Self::analyze_large_operations(docs)?;
        }

        // Generate capacity recommendations
        if needs_capacity_analysis || needs_window_analysis {
            detailed_metrics.capacity_recommendations = Self::generate_capacity_recommendations(core_stats)?;
        }

        // Analyze write patterns
        detailed_metrics.write_pattern_analysis = Self::analyze_write_patterns(core_stats)?;

        // Generate transaction analysis if there are significant transactions
        if core_stats.transaction_operations_percentage > 5.0 {
            detailed_metrics.transaction_analysis = Some(Self::analyze_transaction_impact(core_stats)?);
        }

        // Generate collection impact analysis
        detailed_metrics.collection_impact_analysis = Some(Self::analyze_collection_impact(core_stats)?);

        // Generate performance issues analysis
        if needs_performance_analysis {
            detailed_metrics.performance_issues = Some(Self::analyze_performance_issues(core_stats)?);
        }

        // Generate configuration recommendations
        detailed_metrics.configuration_recommendations = Some(Self::generate_config_recommendations(core_stats)?);

        Ok(Some(detailed_metrics))
    }
    fn parse_oplog_entries(stats: &mut MongoOplogInfo, docs: &[Document]) -> ResultEP<()> {
        let mut total_entries = 0;
        let mut total_size = 0u64;
        let mut operation_counts = HashMap::new();
        let mut collection_activity = HashMap::new();
        let mut transaction_count = 0;
        let mut oldest_ts: Option<mongodb::bson::Timestamp> = None;
        let mut newest_ts: Option<mongodb::bson::Timestamp> = None;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            total_entries += 1;
            total_size += Self::estimate_document_size(doc);

            if let Some(op_type) = acc.opt_string("op") {
                *operation_counts.entry(op_type).or_insert(0) += 1;
            }

            if let Some(ns) = acc.opt_string("ns") {
                *collection_activity.entry(ns).or_insert(0) += 1;
            }

            if acc.raw().contains_key("txnNumber") || acc.raw().contains_key("prevOpTime") {
                transaction_count += 1;
            }

            // Track timestamp range
            if let Some(ts) = acc.raw().get("ts").and_then(|v| v.as_timestamp()) {
                if oldest_ts.is_none() || Some(ts) < oldest_ts {
                    oldest_ts = Some(ts);
                }
                if newest_ts.is_none() || Some(ts) > newest_ts {
                    newest_ts = Some(ts);
                }
            }
        }

        stats.analyzed_entries_count = total_entries;

        if total_entries > 0 {
            stats.avg_oplog_entry_size_bytes = total_size as f64 / total_entries as f64;

            // Calculate operation percentages
            let total_ops = operation_counts.values().sum::<u64>() as f64;
            if total_ops > 0.0 {
                stats.insert_operations_percentage = (*operation_counts.get("i").unwrap_or(&0) as f64 / total_ops) * 100.0;
                stats.update_operations_percentage = (*operation_counts.get("u").unwrap_or(&0) as f64 / total_ops) * 100.0;
                stats.delete_operations_percentage = (*operation_counts.get("d").unwrap_or(&0) as f64 / total_ops) * 100.0;
                stats.command_operations_percentage = (*operation_counts.get("c").unwrap_or(&0) as f64 / total_ops) * 100.0;
            }

            stats.transaction_operations_percentage = (transaction_count as f64 / total_entries as f64) * 100.0;

            // Most active collections
            let mut sorted_collections: Vec<_> = collection_activity.iter().collect();
            sorted_collections.sort_by(|a, b| b.1.cmp(a.1));
            stats.most_active_collections = sorted_collections.into_iter().take(10).map(|(name, _)| name.clone()).collect();
        }

        // Set timestamp range
        if let Some(oldest) = oldest_ts {
            stats.oldest_oplog_entry = Some(DateTimeWrapper::from(DateTime::from_timestamp(oldest.time as i64, 0).unwrap_or_default()));
        }
        if let Some(newest) = newest_ts {
            stats.newest_oplog_entry = Some(DateTimeWrapper::from(DateTime::from_timestamp(newest.time as i64, 0).unwrap_or_default()));
        }

        Ok(())
    }

    fn parse_replica_set_status(stats: &mut MongoOplogInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Ok(members) = result.raw().get_array("members")
            {
                let mut total_lag = 0.0;
                let mut max_lag = 0.0;
                let mut healthy_count = 0;
                let total_members = members.len();

                for member_val in members {
                    if let Some(member_doc) = member_val.as_document() {
                        let member = DocAccessor::new(member_doc);
                        if member.opt_f64("health").unwrap_or(0.0) == 1.0 {
                            healthy_count += 1;
                        }

                        if let (Ok(optime_date), Ok(last_heartbeat)) =
                            (member_doc.get_datetime("optimeDate"), member_doc.get_datetime("lastHeartbeat"))
                        {
                            let lag_ms = (last_heartbeat.timestamp_millis() - optime_date.timestamp_millis()) as f64;
                            total_lag += lag_ms;
                            max_lag = f64::max(max_lag, lag_ms);
                        }
                    }
                }

                stats.replica_set_member_count = total_members as u32;
                stats.healthy_replica_members = healthy_count;
                stats.max_replication_lag_ms = max_lag;

                if total_members > 0 {
                    stats.avg_replication_lag_ms = total_lag / total_members as f64;
                }
            }
        }

        Ok(())
    }

    fn parse_recent_oplog_entries(stats: &mut MongoOplogInfo, docs: &[Document]) -> ResultEP<()> {
        if docs.is_empty() {
            return Ok(());
        }

        let time_window_hours = 1.0; // Analyzing 1 hour of data
        stats.write_operations_count = docs.len() as u64;
        stats.avg_ops_per_second = docs.len() as f64 / (time_window_hours * 3600.0);

        // Calculate growth rates
        let total_size = docs.iter().map(Self::estimate_document_size).sum::<u64>();

        stats.oplog_growth_rate_per_hour = docs.len() as f64 / time_window_hours;
        stats.oplog_size_growth_rate_bytes_per_hour = total_size as f64 / time_window_hours;

        // Calculate peak ops per second (using 5-minute windows)
        let mut ops_per_minute = HashMap::new();
        for doc in docs {
            if let Some(ts) = doc.get("ts").and_then(|v| v.as_timestamp()) {
                let minute_bucket = ts.time / 60; // Group by minute
                *ops_per_minute.entry(minute_bucket).or_insert(0) += 1;
            }
        }

        if let Some(&max_ops_per_minute) = ops_per_minute.values().max() {
            stats.peak_ops_per_second = max_ops_per_minute as f64 / 60.0;
        }

        Ok(())
    }

    fn parse_large_operations(stats: &mut MongoOplogInfo, docs: &[Document]) -> ResultEP<()> {
        let mut large_ops_count = 0;
        let mut transaction_sizes = Vec::new();
        let mut high_impact_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            let doc_size = Self::estimate_document_size(doc);

            if doc_size > Self::LARGE_OPERATION_THRESHOLD_BYTES {
                large_ops_count += 1;
                high_impact_count += 1;
            }

            if acc.raw().contains_key("txnNumber") {
                transaction_sizes.push(doc_size);
            }

            if let Some(op_type) = acc.opt_string("op") {
                match op_type.as_str() {
                    "u" => {
                        if let Some(o_doc) = acc.child("o")
                            && Self::estimate_document_size(o_doc.raw()) > 1024 * 1024
                        {
                            high_impact_count += 1;
                        }
                    }
                    "d" => {
                        high_impact_count += 1;
                    }
                    _ => {}
                }
            }
        }

        stats.large_transaction_count = large_ops_count;
        stats.high_impact_operations_count = high_impact_count;

        if !transaction_sizes.is_empty() {
            stats.avg_transaction_size_bytes = transaction_sizes.iter().sum::<u64>() as f64 / transaction_sizes.len() as f64;
        }

        Ok(())
    }

    fn parse_oplog_collection_stats(stats: &mut MongoOplogInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(size) = result.opt_i64("size") {
                    stats.oplog_used_bytes = size as u64;
                }

                if let Some(storage_size) = result.opt_i64("storageSize") {
                    stats.oplog_size_bytes = storage_size as u64;
                }

                if let Some(max_size) = result.opt_i64("maxSize") {
                    stats.oplog_size_bytes = std::cmp::max(stats.oplog_size_bytes, max_size as u64);
                }

                if let Some(count) = result.opt_i64("count")
                    && count > 0
                    && stats.analyzed_entries_count == 0
                {
                    stats.analyzed_entries_count = count as u64;
                }
            }
        }

        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoOplogInfo) -> ResultEP<()> {
        // Calculate oplog utilization
        if stats.oplog_size_bytes > 0 {
            stats.oplog_utilization_percentage = (stats.oplog_used_bytes as f64 / stats.oplog_size_bytes as f64) * 100.0;
        }

        // Calculate oplog window (time span of operations)
        if let (Some(oldest), Some(newest)) = (&stats.oldest_oplog_entry, &stats.newest_oplog_entry) {
            let duration = newest.as_datetime().timestamp() - oldest.as_datetime().timestamp();
            stats.oplog_window_hours = duration as f64 / 3600.0;
        }

        // Estimate time remaining until oplog is full
        if stats.oplog_size_growth_rate_bytes_per_hour > 0.0 && stats.oplog_size_bytes > stats.oplog_used_bytes {
            let remaining_bytes = stats.oplog_size_bytes - stats.oplog_used_bytes;
            stats.oplog_time_remaining_hours = remaining_bytes as f64 / stats.oplog_size_growth_rate_bytes_per_hour;
        } else {
            stats.oplog_time_remaining_hours = f64::INFINITY;
        }

        // Calculate oplog health score
        let mut health_factors = Vec::new();

        // Utilization factor (lower utilization is better)
        let utilization_factor = if stats.oplog_utilization_percentage < 70.0 {
            1.0
        } else if stats.oplog_utilization_percentage < 85.0 {
            0.8
        } else if stats.oplog_utilization_percentage < 95.0 {
            0.5
        } else {
            0.2
        };
        health_factors.push(utilization_factor);

        // Window factor (longer window is better)
        let window_factor = if stats.oplog_window_hours > 24.0 {
            1.0
        } else if stats.oplog_window_hours > 12.0 {
            0.8
        } else if stats.oplog_window_hours > 4.0 {
            0.6
        } else {
            0.3
        };
        health_factors.push(window_factor);

        // Replication lag factor (lower lag is better)
        let lag_factor = if stats.max_replication_lag_ms < 1000.0 {
            1.0
        } else if stats.max_replication_lag_ms < 5000.0 {
            0.8
        } else if stats.max_replication_lag_ms < 30000.0 {
            0.5
        } else {
            0.2
        };
        health_factors.push(lag_factor);

        // Replica health factor
        let replica_health_factor = if stats.replica_set_member_count > 0 {
            stats.healthy_replica_members as f64 / stats.replica_set_member_count as f64
        } else {
            1.0
        };
        health_factors.push(replica_health_factor);

        stats.oplog_health_score = health_factors.iter().sum::<f64>() / health_factors.len() as f64;

        Ok(())
    }

    fn analyze_replication_issues(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoReplicationIssue>> {
        let mut issues = Vec::new();

        // High replication lag issue
        if stats.max_replication_lag_ms > Self::HIGH_REPLICATION_LAG_THRESHOLD_MS {
            let severity = if stats.max_replication_lag_ms > Self::CRITICAL_REPLICATION_LAG_THRESHOLD_MS {
                "Critical"
            } else {
                "High"
            };

            issues.push(MongoReplicationIssue {
                issue_type: "High Replication Lag".to_string(),
                affected_member: "secondary_member".to_string(), // Would need replica set status to get actual member
                member_state: "SECONDARY".to_string(),
                lag_duration_ms: stats.max_replication_lag_ms,
                lag_severity: severity.to_string(),
                operations_behind: (stats.max_replication_lag_ms / 10.0) as u64, // Estimate based on avg op time
                sync_source: "primary".to_string(),
                last_heartbeat: DateTimeWrapper::from(Utc::now()),
                health_status: if severity == "Critical" {
                    "Poor".to_string()
                } else {
                    "Degraded".to_string()
                },
                estimated_catch_up_time_minutes: stats.max_replication_lag_ms / 60000.0,
                root_cause_indicators: vec![
                    "Network latency".to_string(),
                    "Secondary overload".to_string(),
                    "Large operations".to_string(),
                ],
                recommended_actions: vec![
                    "Check network connectivity".to_string(),
                    "Monitor secondary resource usage".to_string(),
                    "Consider read preference optimization".to_string(),
                ],
                impact_assessment: "Read consistency and failover capability affected".to_string(),
                monitoring_suggestions: vec![
                    "Set up lag monitoring alerts".to_string(),
                    "Track secondary performance metrics".to_string(),
                ],
            });
        }

        // Unhealthy replica members
        if stats.healthy_replica_members < stats.replica_set_member_count {
            let unhealthy_count = stats.replica_set_member_count - stats.healthy_replica_members;

            issues.push(MongoReplicationIssue {
                issue_type: "Unhealthy Replica Members".to_string(),
                affected_member: format!("{} members", unhealthy_count),
                member_state: "UNKNOWN".to_string(),
                lag_duration_ms: 0.0,
                lag_severity: "High".to_string(),
                operations_behind: 0,
                sync_source: "N/A".to_string(),
                last_heartbeat: DateTimeWrapper::from(Utc::now()),
                health_status: "Unhealthy".to_string(),
                estimated_catch_up_time_minutes: 0.0,
                root_cause_indicators: vec![
                    "Node failure".to_string(),
                    "Network partition".to_string(),
                    "Resource exhaustion".to_string(),
                ],
                recommended_actions: vec![
                    "Check node status and logs".to_string(),
                    "Verify network connectivity".to_string(),
                    "Restart unhealthy nodes if necessary".to_string(),
                ],
                impact_assessment: "Reduced fault tolerance and read capacity".to_string(),
                monitoring_suggestions: vec![
                    "Monitor node health continuously".to_string(),
                    "Set up alerts for member state changes".to_string(),
                ],
            });
        }

        Ok(issues)
    }

    fn analyze_large_operations(docs: Vec<Document>) -> ResultEP<Vec<MongoLargeOperation>> {
        let mut large_ops = Vec::new();
        let mut processed = 0;

        for doc in docs {
            if processed >= Self::MAX_DETAILED_RESULTS {
                break;
            }

            let acc = DocAccessor::new(&doc);
            let doc_size = Self::estimate_document_size(&doc);

            if doc_size > Self::LARGE_OPERATION_THRESHOLD_BYTES {
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown.unknown".to_string());
                let op_type = acc.opt_string("op").unwrap_or_else(|| "unknown".to_string());
                let ts = acc
                    .raw()
                    .get("ts")
                    .and_then(|v| v.as_timestamp())
                    .map(|ts| DateTime::from_timestamp(ts.time as i64, 0).unwrap_or_default())
                    .unwrap_or_default();

                // Estimate affected documents
                let affected_docs = acc
                    .child("o")
                    .map(|o| {
                        if op_type == "u" {
                            std::cmp::max(1, Self::estimate_document_size(o.raw()) / 1024)
                        } else {
                            1
                        }
                    })
                    .unwrap_or(1);

                let complexity = if acc.raw().contains_key("txnNumber") {
                    "Transaction"
                } else if doc_size > 64 * 1024 * 1024 {
                    // 64MB
                    "Bulk"
                } else if op_type == "u" && acc.raw().contains_key("o2") {
                    "Complex"
                } else {
                    "Simple"
                };

                large_ops.push(MongoLargeOperation {
                    operation_id: format!("large_op_{}", processed),
                    operation_type: op_type.clone(),
                    namespace: ns.clone(),
                    operation_size_bytes: doc_size,
                    timestamp: DateTimeWrapper::from(ts),
                    execution_time_ms: 0.0, // Would need profiler data
                    oplog_entries_count: if acc.raw().contains_key("prevOpTime") { 2 } else { 1 },
                    affected_documents: affected_docs,
                    operation_complexity: complexity.to_string(),
                    impact_on_secondaries: if doc_size > 32 * 1024 * 1024 { "High" } else { "Medium" }.to_string(),
                    optimization_suggestions: Self::suggest_large_op_optimization(&op_type, doc_size),
                    potential_issues: Self::identify_large_op_issues(&op_type, doc_size, complexity),
                    business_context: Self::infer_business_context(&ns),
                });

                processed += 1;
            }
        }

        Ok(large_ops)
    }

    fn generate_capacity_recommendations(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoOplogCapacityRecommendation>> {
        let mut recommendations = Vec::new();

        // High utilization recommendation
        if stats.oplog_utilization_percentage > Self::HIGH_OPLOG_UTILIZATION_THRESHOLD {
            let current_gb = stats.oplog_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let recommended_gb = current_gb * 2.0; // Double the size

            recommendations.push(MongoOplogCapacityRecommendation {
                recommendation_type: "Size Increase".to_string(),
                current_capacity_gb: current_gb,
                recommended_capacity_gb: recommended_gb,
                rationale: format!("Oplog utilization at {:.1}% exceeds recommended threshold", stats.oplog_utilization_percentage),
                urgency_level: if stats.oplog_utilization_percentage > 95.0 {
                    "Critical"
                } else {
                    "High"
                }
                .to_string(),
                estimated_window_improvement_hours: stats.oplog_window_hours * 2.0,
                implementation_steps: vec![
                    "Plan maintenance window".to_string(),
                    "Resize oplog using db.runCommand({replSetResizeOplog: 1, size: newSizeInMB})".to_string(),
                    "Monitor impact on performance".to_string(),
                    "Verify replication continues normally".to_string(),
                ],
                risks_and_considerations: vec![
                    "Increased disk usage".to_string(),
                    "Temporary performance impact during resize".to_string(),
                    "Ensure sufficient disk space".to_string(),
                ],
                cost_implications: format!("Additional {:.1}GB disk space required", recommended_gb - current_gb),
                timeline_estimate: "1-2 hours including validation".to_string(),
                success_metrics: vec![
                    format!("Oplog utilization below 80%"),
                    format!("Oplog window increased to {:.1} hours", stats.oplog_window_hours * 2.0),
                    "No replication lag increase".to_string(),
                ],
                monitoring_requirements: vec![
                    "Monitor oplog utilization trends".to_string(),
                    "Set alerts for 80% utilization".to_string(),
                    "Track oplog window duration".to_string(),
                ],
            });
        }

        // Short window recommendation
        if stats.oplog_window_hours < Self::LOW_OPLOG_WINDOW_THRESHOLD_HOURS {
            recommendations.push(MongoOplogCapacityRecommendation {
                recommendation_type: "Window Extension".to_string(),
                current_capacity_gb: stats.oplog_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                recommended_capacity_gb: (stats.oplog_size_bytes as f64 * 3.0) / (1024.0 * 1024.0 * 1024.0),
                rationale: format!("Oplog window of {:.1} hours is too short for safe operations", stats.oplog_window_hours),
                urgency_level: "Medium".to_string(),
                estimated_window_improvement_hours: stats.oplog_window_hours * 3.0,
                implementation_steps: vec![
                    "Analyze write patterns to determine optimal size".to_string(),
                    "Increase oplog size to accommodate 24-48 hour window".to_string(),
                    "Update monitoring thresholds".to_string(),
                ],
                risks_and_considerations: vec![
                    "Higher disk usage".to_string(),
                    "Longer initial sync times for new members".to_string(),
                ],
                cost_implications: "Increased storage costs but improved operational safety".to_string(),
                timeline_estimate: "2-4 hours including planning".to_string(),
                success_metrics: vec![
                    "Oplog window > 24 hours".to_string(),
                    "Sufficient time for maintenance operations".to_string(),
                ],
                monitoring_requirements: vec![
                    "Daily oplog window checks".to_string(),
                    "Trend analysis of oplog consumption".to_string(),
                ],
            });
        }

        Ok(recommendations)
    }

    fn analyze_write_patterns(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoWritePatternAnalysis>> {
        let mut patterns = Vec::new();

        // High frequency writes pattern
        if stats.avg_ops_per_second > Self::HIGH_OPS_PER_SECOND_THRESHOLD {
            patterns.push(MongoWritePatternAnalysis {
                pattern_type: "High Frequency Writes".to_string(),
                pattern_description: format!("Average {:.1} operations per second detected", stats.avg_ops_per_second),
                frequency_analysis: "Sustained high write volume".to_string(),
                collections_affected: stats.most_active_collections.clone(),
                oplog_impact_score: 8.0,
                performance_implications: vec![
                    "Increased replication lag risk".to_string(),
                    "Higher secondary load".to_string(),
                    "Faster oplog consumption".to_string(),
                ],
                optimization_opportunities: vec![
                    "Batch write operations".to_string(),
                    "Optimize write patterns".to_string(),
                    "Consider write concern adjustments".to_string(),
                ],
                recommended_strategies: vec![
                    "Implement write batching".to_string(),
                    "Review indexing strategy".to_string(),
                    "Monitor secondary performance".to_string(),
                ],
                implementation_complexity: "Medium".to_string(),
                expected_benefits: vec![
                    "Reduced oplog pressure".to_string(),
                    "Lower replication lag".to_string(),
                    "Improved secondary performance".to_string(),
                ],
                monitoring_metrics: vec![
                    "Operations per second".to_string(),
                    "Replication lag".to_string(),
                    "Secondary resource usage".to_string(),
                ],
            });
        }

        // Large document pattern
        if stats.avg_oplog_entry_size_bytes > 100.0 * 1024.0 {
            // 100KB average
            patterns.push(MongoWritePatternAnalysis {
                pattern_type: "Large Documents".to_string(),
                pattern_description: format!("Average oplog entry size: {:.1}KB", stats.avg_oplog_entry_size_bytes / 1024.0),
                frequency_analysis: "Consistent large document operations".to_string(),
                collections_affected: stats.most_active_collections.clone(),
                oplog_impact_score: 7.0,
                performance_implications: vec![
                    "High network bandwidth usage".to_string(),
                    "Slower replication".to_string(),
                    "Memory pressure on secondaries".to_string(),
                ],
                optimization_opportunities: vec![
                    "Document schema optimization".to_string(),
                    "Field selection in updates".to_string(),
                    "Consider document splitting".to_string(),
                ],
                recommended_strategies: vec![
                    "Review document structure".to_string(),
                    "Implement incremental updates".to_string(),
                    "Use projection in queries".to_string(),
                ],
                implementation_complexity: "High".to_string(),
                expected_benefits: vec![
                    "Reduced oplog consumption".to_string(),
                    "Faster replication".to_string(),
                    "Lower bandwidth usage".to_string(),
                ],
                monitoring_metrics: vec![
                    "Average document size".to_string(),
                    "Oplog growth rate".to_string(),
                    "Network throughput".to_string(),
                ],
            });
        }

        // Transaction pattern
        if stats.transaction_operations_percentage > 20.0 {
            patterns.push(MongoWritePatternAnalysis {
                pattern_type: "High Transaction Usage".to_string(),
                pattern_description: format!("{:.1}% of operations are transactions", stats.transaction_operations_percentage),
                frequency_analysis: "Frequent multi-document transactions".to_string(),
                collections_affected: stats.most_active_collections.clone(),
                oplog_impact_score: 6.0,
                performance_implications: vec![
                    "Multiple oplog entries per transaction".to_string(),
                    "Increased complexity for secondaries".to_string(),
                    "Potential for large transaction conflicts".to_string(),
                ],
                optimization_opportunities: vec![
                    "Transaction size optimization".to_string(),
                    "Reduce transaction scope".to_string(),
                    "Batch related operations".to_string(),
                ],
                recommended_strategies: vec![
                    "Review transaction boundaries".to_string(),
                    "Optimize transaction duration".to_string(),
                    "Monitor transaction conflicts".to_string(),
                ],
                implementation_complexity: "Medium".to_string(),
                expected_benefits: vec![
                    "Reduced oplog overhead".to_string(),
                    "Better transaction performance".to_string(),
                    "Lower conflict rates".to_string(),
                ],
                monitoring_metrics: vec![
                    "Transaction duration".to_string(),
                    "Transaction conflict rate".to_string(),
                    "Oplog entries per transaction".to_string(),
                ],
            });
        }

        Ok(patterns)
    }

    fn analyze_transaction_impact(stats: &MongoOplogInfo) -> ResultEP<MongoTransactionAnalysis> {
        // Only populate what we can actually derive from oplog stats.
        let total_transactions = (stats.analyzed_entries_count as f64 * stats.transaction_operations_percentage / 100.0) as u64;
        Ok(MongoTransactionAnalysis {
            total_transactions,
            avg_transaction_duration_ms: 0.0,
            avg_transaction_oplog_entries: 0.0,
            largest_transaction_entries: 0,
            largest_transaction_size_bytes: 0,
            cross_shard_transactions: 0,
            failed_transactions: 0,
            transaction_oplog_overhead_percentage: stats.transaction_operations_percentage,
            peak_concurrent_transactions: 0,
            transaction_patterns: Vec::new(),
            performance_recommendations: Vec::new(),
            optimization_strategies: Vec::new(),
        })
    }

    fn analyze_collection_impact(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoCollectionOplogImpact>> {
        let mut collection_impacts = Vec::new();

        // Analyze top active collections
        for (index, collection) in stats.most_active_collections.iter().enumerate() {
            if index >= 10 {
                break;
            } // Limit to top 10

            let estimated_ops = stats.write_operations_count / (index as u64 + 1); // Rough distribution
            let estimated_bytes = estimated_ops * stats.avg_oplog_entry_size_bytes as u64;

            let mut op_distribution = HashMap::new();
            op_distribution.insert("insert".to_string(), stats.insert_operations_percentage);
            op_distribution.insert("update".to_string(), stats.update_operations_percentage);
            op_distribution.insert("delete".to_string(), stats.delete_operations_percentage);

            collection_impacts.push(MongoCollectionOplogImpact {
                collection_namespace: collection.clone(),
                operations_count: estimated_ops,
                total_oplog_bytes: estimated_bytes,
                avg_operation_size_bytes: stats.avg_oplog_entry_size_bytes,
                operation_type_distribution: op_distribution,
                impact_percentage: (estimated_bytes as f64 / (stats.oplog_used_bytes as f64)) * 100.0,
                write_frequency_pattern: if index < 3 {
                    "High"
                } else if index < 7 {
                    "Medium"
                } else {
                    "Low"
                }
                .to_string(),
                optimization_potential: if index < 2 { "High" } else { "Medium" }.to_string(),
                recommended_indexes: vec!["Review query patterns for indexing opportunities".to_string()],
                suggested_optimizations: vec![
                    "Analyze write patterns".to_string(),
                    "Consider schema optimization".to_string(),
                    "Review update strategies".to_string(),
                ],
                monitoring_recommendations: vec!["Track collection-specific metrics".to_string(), "Monitor index usage".to_string()],
            });
        }

        Ok(collection_impacts)
    }

    fn analyze_performance_issues(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoOplogPerformanceIssue>> {
        let mut issues = Vec::new();

        // High operations per second
        if stats.avg_ops_per_second > Self::HIGH_OPS_PER_SECOND_THRESHOLD {
            issues.push(MongoOplogPerformanceIssue {
                issue_type: "High Write Volume".to_string(),
                severity: "High".to_string(),
                affected_operations: stats.write_operations_count,
                performance_impact_description: format!("Sustained {:.1} ops/sec may cause replication lag", stats.avg_ops_per_second),
                root_cause_analysis: "Application generating high write volume".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                estimated_resolution_time: "2-4 hours for optimization".to_string(),
                business_impact: "Risk of replication lag affecting read consistency".to_string(),
                technical_details: format!(
                    "Peak: {:.1} ops/sec, Average: {:.1} ops/sec",
                    stats.peak_ops_per_second, stats.avg_ops_per_second
                ),
                recommended_solution: "Implement write batching and optimize application patterns".to_string(),
                prevention_strategies: vec![
                    "Monitor write patterns continuously".to_string(),
                    "Set up proactive alerting".to_string(),
                    "Regular performance reviews".to_string(),
                ],
                monitoring_improvements: vec![
                    "Real-time ops/sec monitoring".to_string(),
                    "Write pattern analysis".to_string(),
                    "Capacity planning alerts".to_string(),
                ],
            });
        }

        // Poor oplog health
        if stats.oplog_health_score < Self::POOR_OPLOG_HEALTH_THRESHOLD {
            issues.push(MongoOplogPerformanceIssue {
                issue_type: "Poor Oplog Health".to_string(),
                severity: "Medium".to_string(),
                affected_operations: 0,
                performance_impact_description: format!("Oplog health score: {:.1}%", stats.oplog_health_score * 100.0),
                root_cause_analysis: "Multiple factors affecting oplog performance".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                estimated_resolution_time: "4-8 hours for comprehensive fixes".to_string(),
                business_impact: "Reduced system reliability and operational flexibility".to_string(),
                technical_details: format!(
                    "Utilization: {:.1}%, Window: {:.1}h, Lag: {:.1}ms",
                    stats.oplog_utilization_percentage, stats.oplog_window_hours, stats.max_replication_lag_ms
                ),
                recommended_solution: "Address utilization, extend window, reduce lag".to_string(),
                prevention_strategies: vec![
                    "Proactive capacity management".to_string(),
                    "Regular health assessments".to_string(),
                    "Trend monitoring".to_string(),
                ],
                monitoring_improvements: vec![
                    "Comprehensive health scoring".to_string(),
                    "Multi-metric dashboards".to_string(),
                    "Predictive alerting".to_string(),
                ],
            });
        }

        Ok(issues)
    }

    fn generate_config_recommendations(stats: &MongoOplogInfo) -> ResultEP<Vec<MongoOplogConfigRecommendation>> {
        let mut recommendations = Vec::new();

        // Oplog size recommendation
        if stats.oplog_utilization_percentage > Self::HIGH_OPLOG_UTILIZATION_THRESHOLD {
            recommendations.push(MongoOplogConfigRecommendation {
                configuration_area: "Oplog Size".to_string(),
                current_setting: format!("{:.1}GB", stats.oplog_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)),
                recommended_setting: format!("{:.1}GB", (stats.oplog_size_bytes as f64 * 2.0) / (1024.0 * 1024.0 * 1024.0)),
                justification: format!("Current utilization at {:.1}% requires larger oplog", stats.oplog_utilization_percentage),
                expected_improvement: "Extended oplog window and reduced pressure".to_string(),
                implementation_risk: "Low - online operation".to_string(),
                testing_requirements: vec![
                    "Monitor replication during resize".to_string(),
                    "Verify disk space availability".to_string(),
                    "Test in staging environment first".to_string(),
                ],
                rollback_procedure: "Cannot shrink oplog easily - plan carefully".to_string(),
                monitoring_after_change: vec![
                    "Oplog utilization percentage".to_string(),
                    "Oplog window duration".to_string(),
                    "Replication lag impact".to_string(),
                ],
                compatibility_considerations: vec![
                    "All replica set members supported".to_string(),
                    "No application changes required".to_string(),
                ],
            });
        }

        // Write concern optimization
        if stats.avg_ops_per_second > Self::HIGH_OPS_PER_SECOND_THRESHOLD {
            recommendations.push(MongoOplogConfigRecommendation {
                configuration_area: "Write Concern".to_string(),
                current_setting: "w: majority (assumed)".to_string(),
                recommended_setting: "w: 1 with appropriate j: true for critical operations".to_string(),
                justification: "High write volume may benefit from optimized write concern".to_string(),
                expected_improvement: "Improved write throughput with managed durability".to_string(),
                implementation_risk: "Medium - affects data durability guarantees".to_string(),
                testing_requirements: vec![
                    "Thorough testing of failure scenarios".to_string(),
                    "Application-level write concern review".to_string(),
                    "Performance benchmarking".to_string(),
                ],
                rollback_procedure: "Revert to previous write concern settings".to_string(),
                monitoring_after_change: vec![
                    "Write performance metrics".to_string(),
                    "Replication lag changes".to_string(),
                    "Data consistency validation".to_string(),
                ],
                compatibility_considerations: vec![
                    "Application must handle write concern appropriately".to_string(),
                    "Consider per-operation write concern".to_string(),
                ],
            });
        }

        // Replication lag optimization
        if stats.max_replication_lag_ms > Self::HIGH_REPLICATION_LAG_THRESHOLD_MS {
            recommendations.push(MongoOplogConfigRecommendation {
                configuration_area: "Secondary Read Preference".to_string(),
                current_setting: "primary (assumed)".to_string(),
                recommended_setting: "primaryPreferred with maxStalenessSeconds".to_string(),
                justification: format!(
                    "High replication lag ({:.1}ms) suggests secondary optimization needed",
                    stats.max_replication_lag_ms
                ),
                expected_improvement: "Reduced primary load and better read distribution".to_string(),
                implementation_risk: "Medium - may affect read consistency".to_string(),
                testing_requirements: vec![
                    "Application tolerance for eventual consistency".to_string(),
                    "Read preference impact testing".to_string(),
                    "Staleness threshold optimization".to_string(),
                ],
                rollback_procedure: "Revert read preference to primary".to_string(),
                monitoring_after_change: vec![
                    "Read distribution across replicas".to_string(),
                    "Application consistency requirements".to_string(),
                    "Primary server load reduction".to_string(),
                ],
                compatibility_considerations: vec![
                    "Application must handle stale reads appropriately".to_string(),
                    "Critical reads should specify primary".to_string(),
                ],
            });
        }

        // Journal optimization for high write volumes
        if stats.avg_ops_per_second > Self::HIGH_OPS_PER_SECOND_THRESHOLD * 2.0 {
            recommendations.push(MongoOplogConfigRecommendation {
                configuration_area: "Journal Commit Interval".to_string(),
                current_setting: "100ms (default)".to_string(),
                recommended_setting: "30-50ms for high write volumes".to_string(),
                justification: "Very high write volume may benefit from more frequent journal commits".to_string(),
                expected_improvement: "Better write durability with minimal performance impact".to_string(),
                implementation_risk: "Low - minor performance impact".to_string(),
                testing_requirements: vec![
                    "Write performance benchmarking".to_string(),
                    "Durability testing".to_string(),
                    "Storage I/O impact analysis".to_string(),
                ],
                rollback_procedure: "Revert journalCommitInterval to default".to_string(),
                monitoring_after_change: vec![
                    "Write latency changes".to_string(),
                    "Journal flush frequency".to_string(),
                    "Storage I/O patterns".to_string(),
                ],
                compatibility_considerations: vec![
                    "All storage engines support this setting".to_string(),
                    "Consider storage performance characteristics".to_string(),
                ],
            });
        }

        Ok(recommendations)
    }

    // Helper functions
    fn estimate_document_size(doc: &Document) -> u64 {
        // Rough estimation based on BSON serialization
        match bson::to_vec(doc) {
            Ok(bytes) => bytes.len() as u64,
            Err(_) => 1024, // Default estimate
        }
    }

    fn suggest_large_op_optimization(op_type: &str, size_bytes: u64) -> Vec<String> {
        let mut suggestions = Vec::new();

        match op_type {
            "i" => {
                // Insert
                suggestions.push("Consider bulk insert operations".to_string());
                if size_bytes > 32 * 1024 * 1024 {
                    suggestions.push("Split large documents into smaller chunks".to_string());
                }
                suggestions.push("Use ordered: false for better performance".to_string());
            }
            "u" => {
                // Update
                suggestions.push("Use targeted field updates instead of document replacement".to_string());
                suggestions.push("Consider using $inc, $set operators efficiently".to_string());
                if size_bytes > 16 * 1024 * 1024 {
                    suggestions.push("Break large updates into smaller operations".to_string());
                }
            }
            "d" => {
                // Delete
                suggestions.push("Consider bulk delete operations".to_string());
                suggestions.push("Use deleteMany with appropriate filters".to_string());
            }
            _ => {
                suggestions.push("Review operation complexity".to_string());
            }
        }

        suggestions.push("Monitor replication lag after large operations".to_string());
        suggestions
    }

    fn identify_large_op_issues(op_type: &str, size_bytes: u64, complexity: &str) -> Vec<String> {
        let mut issues = Vec::new();

        if size_bytes > 64 * 1024 * 1024 {
            // 64MB
            issues.push("Operation exceeds recommended size limits".to_string());
            issues.push("May cause significant replication lag".to_string());
            issues.push("Risk of memory pressure on secondaries".to_string());
        }

        if complexity == "Transaction" {
            issues.push("Large transactions can cause conflicts".to_string());
            issues.push("May impact concurrent operations".to_string());
        }

        match op_type {
            "u" => {
                issues.push("Large updates can block other operations".to_string());
                issues.push("May cause index maintenance overhead".to_string());
            }
            "d" => {
                issues.push("Large deletes can cause fragmentation".to_string());
                issues.push("May impact query performance temporarily".to_string());
            }
            _ => {}
        }

        if issues.is_empty() {
            issues.push("Monitor for performance impact".to_string());
        }

        issues
    }

    fn infer_business_context(namespace: &str) -> String {
        let parts: Vec<&str> = namespace.split('.').collect();
        if parts.len() >= 2 {
            let collection = parts[1];
            match collection {
                s if s.contains("user") => "User management operations".to_string(),
                s if s.contains("order") => "Order processing operations".to_string(),
                s if s.contains("product") => "Product catalog operations".to_string(),
                s if s.contains("log") => "Logging operations".to_string(),
                s if s.contains("audit") => "Audit trail operations".to_string(),
                s if s.contains("session") => "Session management operations".to_string(),
                s if s.contains("config") => "Configuration management".to_string(),
                _ => "Business data operations".to_string(),
            }
        } else {
            "Unknown business context".to_string()
        }
    }
}
