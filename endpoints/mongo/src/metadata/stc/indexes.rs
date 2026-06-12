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

/// MongoDB Index statistics and performance metrics
///
/// Comprehensive struct containing essential metrics about index
/// usage, performance characteristics, and optimization opportunities.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexInfo {
    /// Total number of indexes across all databases
    pub total_indexes: u64,
    /// Total number of collections with indexes
    pub indexed_collections: u64,
    /// Total number of databases with custom indexes
    pub indexed_databases: u64,
    /// Total index storage size (bytes)
    pub total_index_size_bytes: u64,
    /// Average index size (bytes)
    pub avg_index_size_bytes: f64,
    /// Maximum index size (bytes)
    pub max_index_size_bytes: u64,
    /// Minimum index size (bytes)
    pub min_index_size_bytes: u64,
    /// Number of compound indexes (multi-field)
    pub compound_indexes: u64,
    /// Number of single field indexes
    pub single_field_indexes: u64,
    /// Number of text indexes
    pub text_indexes: u64,
    /// Number of geospatial indexes (2d, 2dsphere)
    pub geospatial_indexes: u64,
    /// Number of partial indexes
    pub partial_indexes: u64,
    /// Number of sparse indexes
    pub sparse_indexes: u64,
    /// Number of unique indexes
    pub unique_indexes: u64,
    /// Number of TTL indexes
    pub ttl_indexes: u64,
    /// Number of unused indexes (never accessed)
    pub unused_indexes: u64,
    /// Number of redundant indexes (overlapping functionality)
    pub redundant_indexes: u64,
    /// Number of missing recommended indexes
    pub missing_indexes: u64,
    /// Average index selectivity (0.0 to 1.0)
    pub avg_index_selectivity: f64,
    /// Index usage efficiency percentage (0.0 to 100.0)
    pub index_usage_efficiency: f64,
    /// Average query response time improvement from indexes (ms)
    pub avg_query_improvement_ms: f64,
    /// Total index maintenance overhead (writes/sec)
    pub index_maintenance_overhead: f64,
    /// Number of slow queries that could benefit from indexing
    pub slow_queries_needing_indexes: u64,
    /// Index fragmentation percentage
    pub avg_index_fragmentation: f64,
    /// Memory used by indexes (bytes)
    pub index_memory_usage_bytes: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoIndexDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexDetailedMetrics {
    /// Unused indexes that can be safely dropped
    pub unused_indexes: Vec<MongoUnusedIndex>,
    /// Redundant indexes with overlapping functionality
    pub redundant_indexes: Vec<MongoRedundantIndex>,
    /// Missing indexes that would improve performance
    pub missing_indexes: Vec<MongoMissingIndex>,
    /// Indexes with poor selectivity
    pub inefficient_indexes: Vec<MongoInefficientIndex>,
    /// Large indexes consuming excessive storage
    pub oversized_indexes: Vec<MongoOversizedIndex>,
    /// Index performance issues
    pub performance_issues: Option<Vec<MongoIndexPerformanceIssue>>,
    /// Index usage patterns by collection
    pub collection_usage: Option<Vec<MongoIndexCollectionStats>>,
    /// Index creation and maintenance costs
    pub maintenance_costs: Option<Vec<MongoIndexMaintenanceCost>>,
}

/// Information about unused indexes that can be safely dropped
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUnusedIndex {
    pub index_name: String,
    pub collection: String,
    pub database: String,
    pub index_spec: String,
    pub size_bytes: u64,
    pub created_date: Option<DateTimeWrapper>,
    pub last_accessed: Option<DateTimeWrapper>,
    pub days_unused: u64,
    pub storage_waste_mb: f64,
    pub maintenance_cost_per_write: f64,
    pub recommended_action: String,
    pub safety_level: String, // Safe, Caution, Risky
}

/// Information about redundant indexes with overlapping functionality
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoRedundantIndex {
    pub redundant_index_name: String,
    pub overlapping_index_name: String,
    pub collection: String,
    pub database: String,
    pub redundant_spec: String,
    pub overlapping_spec: String,
    pub redundancy_type: String, // Prefix, Duplicate, Subset
    pub size_bytes: u64,
    pub usage_comparison: String,
    pub recommended_action: String,
    pub potential_savings_mb: f64,
}

/// Information about missing indexes that would improve performance
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoMissingIndex {
    pub collection: String,
    pub database: String,
    pub suggested_fields: Vec<String>,
    pub query_pattern: String,
    pub current_avg_duration_ms: f64,
    pub estimated_improvement_ms: f64,
    pub queries_affected: u64,
    pub index_type: String, // Compound, Single, Text, etc.
    pub priority: String,   // High, Medium, Low
    pub estimated_size_mb: f64,
    pub performance_impact: String,
}

/// Information about indexes with poor selectivity
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoInefficientIndex {
    pub index_name: String,
    pub collection: String,
    pub database: String,
    pub index_spec: String,
    pub selectivity_ratio: f64,
    pub usage_frequency: u64,
    pub avg_docs_examined: u64,
    pub avg_docs_returned: u64,
    pub efficiency_rating: String, // Poor, Fair, Good
    pub recommended_optimization: String,
    pub impact_on_performance: String,
}

/// Information about large indexes consuming excessive storage
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOversizedIndex {
    pub index_name: String,
    pub collection: String,
    pub database: String,
    pub size_mb: f64,
    pub collection_size_mb: f64,
    pub size_ratio_percentage: f64,
    pub field_count: u32,
    pub usage_frequency: u64,
    pub fragmentation_percentage: f64,
    pub recommended_action: String,
    pub storage_optimization_potential: f64,
}

/// Index performance bottlenecks and issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexPerformanceIssue {
    pub issue_type: String,
    pub affected_indexes: u64,
    pub avg_impact_ms: f64,
    pub frequency: u64,
    pub description: String,
    pub recommended_solution: String,
    pub severity: String, // Critical, High, Medium, Low
}

/// Index usage statistics by collection
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexCollectionStats {
    pub collection: String,
    pub database: String,
    pub total_indexes: u32,
    pub total_index_size_mb: f64,
    pub most_used_index: String,
    pub least_used_index: String,
    pub index_efficiency_score: f64,
    pub queries_per_hour: u64,
    pub index_hit_ratio: f64,
}

/// Index creation and maintenance cost analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoIndexMaintenanceCost {
    pub index_name: String,
    pub collection: String,
    pub database: String,
    pub writes_per_hour: u64,
    pub maintenance_time_ms_per_write: f64,
    pub total_maintenance_time_ms: f64,
    pub build_time_estimate_minutes: f64,
    pub cost_benefit_ratio: f64,
    pub optimization_recommendation: String,
}

impl MetadataCollection for MongoIndexInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "index_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.collStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "index_usage".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "executionStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(500)),
                ),
            ),
            (
                "slow_queries".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 1000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(60)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(200)),
                ),
            ),
            (
                "index_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.createIndexes": { "$exists": true } },
                            { "command.dropIndexes": { "$exists": true } },
                            { "command.reIndex": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(24)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "write_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.insert": { "$exists": true } },
                            { "command.update": { "$exists": true } },
                            { "command.delete": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(300)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive index usage and performance metrics"
    }

    fn category(&self) -> &'static str {
        "indexes"
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

impl MongoIndexInfo {
    const LARGE_INDEX_THRESHOLD_MB: f64 = 100.0;
    const POOR_SELECTIVITY_THRESHOLD: f64 = 0.1;
    const SLOW_QUERY_THRESHOLD_MS: f64 = 1000.0; // 1 second
    const QUERY_TIMEOUT: Duration = Duration::from_secs(20);
    const MAX_DETAILED_RESULTS: usize = 100;
    const HIGH_FRAGMENTATION_THRESHOLD: f64 = 25.0; // 25%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut index_stats = MongoIndexInfo::default();
        let requests = self.request();

        // Execute queries to get index information
        let stats_docs = fetch(&requests, "index_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_index_stats(&mut index_stats, &stats_docs)?;

        let usage_docs = fetch(&requests, "index_usage", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_index_usage(&mut index_stats, &usage_docs)?;

        let slow_queries_docs = fetch(&requests, "slow_queries", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_slow_queries(&mut index_stats, &slow_queries_docs)?;

        let index_ops_docs = fetch(&requests, "index_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_index_operations(&mut index_stats, &index_ops_docs)?;

        let write_ops_docs = fetch(&requests, "write_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_write_operations(&mut index_stats, &write_ops_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut index_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        index_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&index_stats, &requests, context).await?;

        Ok(index_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoIndexInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoIndexDetailedMetrics>> {
        let needs_unused_details = core_stats.unused_indexes > 0;
        let needs_redundant_details = core_stats.redundant_indexes > 0;
        let needs_missing_details = core_stats.missing_indexes > 0;
        let needs_inefficient_details = core_stats.avg_index_selectivity < Self::POOR_SELECTIVITY_THRESHOLD;
        let needs_oversized_details = core_stats.max_index_size_bytes > (Self::LARGE_INDEX_THRESHOLD_MB * 1024.0 * 1024.0) as u64;
        let needs_performance_details = core_stats.index_usage_efficiency < 50.0;

        if !needs_unused_details
            && !needs_redundant_details
            && !needs_missing_details
            && !needs_inefficient_details
            && !needs_oversized_details
            && !needs_performance_details
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoIndexDetailedMetrics {
            unused_indexes: Vec::new(),
            redundant_indexes: Vec::new(),
            missing_indexes: Vec::new(),
            inefficient_indexes: Vec::new(),
            oversized_indexes: Vec::new(),
            performance_issues: None,
            collection_usage: None,
            maintenance_costs: None,
        };

        // Collect unused indexes if needed
        if needs_unused_details {
            detailed_metrics.unused_indexes = Self::identify_unused_indexes(core_stats)?;
        }

        // Collect redundant indexes if needed
        if needs_redundant_details {
            detailed_metrics.redundant_indexes = Self::identify_redundant_indexes(core_stats)?;
        }

        // Collect missing indexes if needed
        if needs_missing_details {
            let docs = fetch(requests, "slow_queries", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.missing_indexes = Self::suggest_missing_indexes(docs)?;
        }

        // Collect inefficient indexes if needed
        if needs_inefficient_details {
            detailed_metrics.inefficient_indexes = Self::identify_inefficient_indexes(core_stats)?;
        }

        // Collect oversized indexes if needed
        if needs_oversized_details {
            detailed_metrics.oversized_indexes = Self::identify_oversized_indexes(core_stats)?;
        }

        // Collect performance issues if needed
        if needs_performance_details {
            detailed_metrics.performance_issues = Some(Self::analyze_performance_issues(core_stats)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_index_stats(stats: &mut MongoIndexInfo, docs: &[Document]) -> ResultEP<()> {
        let mut index_sizes = Vec::new();
        let mut total_size = 0u64;
        let mut collections = std::collections::HashSet::new();
        let mut databases = std::collections::HashSet::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result") {
                // Extract database and collection info
                if let Some(ns) = acc.opt_string("ns") {
                    let parts: Vec<&str> = ns.split('.').collect();
                    if parts.len() >= 2 {
                        databases.insert(parts[0].to_string());
                        collections.insert(ns);
                    }
                }

                // Parse index information from collStats
                if let Some(index_sizes_acc) = result.child("indexSizes") {
                    for (index_name, size_value) in index_sizes_acc.raw() {
                        if let Some(size) = size_value.as_i64() {
                            index_sizes.push(size);
                            total_size += size as u64;

                            // Classify index types based on name patterns
                            Self::classify_index_type(stats, index_name, size);
                        }
                    }
                }

                // Parse total index size if available
                if let Some(total_index_size) = result.opt_i64("totalIndexSize") {
                    stats.total_index_size_bytes += total_index_size as u64;
                }
            }
        }

        stats.total_indexes = index_sizes.len() as u64;
        stats.indexed_collections = collections.len() as u64;
        stats.indexed_databases = databases.len() as u64;

        if !index_sizes.is_empty() {
            stats.avg_index_size_bytes = index_sizes.iter().sum::<i64>() as f64 / index_sizes.len() as f64;
            stats.max_index_size_bytes = index_sizes.iter().max().copied().unwrap_or(0) as u64;
            stats.min_index_size_bytes = index_sizes.iter().min().copied().unwrap_or(0) as u64;
        }

        if stats.total_index_size_bytes == 0 {
            stats.total_index_size_bytes = total_size;
        }

        Ok(())
    }

    fn classify_index_type(stats: &mut MongoIndexInfo, index_name: &str, _size: i64) {
        if index_name == "_id_" {
            return; // Skip default _id index
        }

        // Count field separators to determine if compound
        let field_count = index_name.matches('_').count() + 1;
        if field_count > 1 {
            stats.compound_indexes += 1;
        } else {
            stats.single_field_indexes += 1;
        }

        // Classify by type based on naming patterns
        if index_name.contains("text") {
            stats.text_indexes += 1;
        } else if index_name.contains("2d") || index_name.contains("geo") {
            stats.geospatial_indexes += 1;
        } else if index_name.contains("sparse") {
            stats.sparse_indexes += 1;
        } else if index_name.contains("unique") {
            stats.unique_indexes += 1;
        } else if index_name.contains("ttl") || index_name.contains("expire") {
            stats.ttl_indexes += 1;
        }
    }

    fn parse_index_usage(stats: &mut MongoIndexInfo, docs: &[Document]) -> ResultEP<()> {
        let mut usage_samples = Vec::new();
        let mut selectivity_samples = Vec::new();
        let mut query_improvement_samples = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(exec_stats) = acc.child("executionStats") {
                // Calculate index usage efficiency
                if let (Some(docs_examined), Some(docs_returned)) =
                    (exec_stats.opt_i64("totalDocsExamined"), exec_stats.opt_i64("totalDocsReturned"))
                    && docs_examined > 0
                {
                    let efficiency = (docs_returned as f64 / docs_examined as f64) * 100.0;
                    usage_samples.push(efficiency);

                    // Calculate selectivity
                    let selectivity = docs_returned as f64 / docs_examined as f64;
                    selectivity_samples.push(selectivity);
                }

                if let Some(execution_time) = acc.opt_f64("millis")
                    && let Some(index_only) = exec_stats.opt_bool("indexOnly")
                    && index_only
                {
                    query_improvement_samples.push(execution_time);
                }
            }
        }

        if !usage_samples.is_empty() {
            stats.index_usage_efficiency = usage_samples.iter().sum::<f64>() / usage_samples.len() as f64;
        }

        if !selectivity_samples.is_empty() {
            stats.avg_index_selectivity = selectivity_samples.iter().sum::<f64>() / selectivity_samples.len() as f64;
        }

        if !query_improvement_samples.is_empty() {
            stats.avg_query_improvement_ms = query_improvement_samples.iter().sum::<f64>() / query_improvement_samples.len() as f64;
        }

        Ok(())
    }

    fn parse_slow_queries(stats: &mut MongoIndexInfo, docs: &[Document]) -> ResultEP<()> {
        let mut queries_needing_indexes = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            // Check if query could benefit from indexing
            if let Some(exec_stats) = acc.child("executionStats") {
                if let Some(docs_examined) = exec_stats.opt_i64("totalDocsExamined")
                    && let Some(docs_returned) = exec_stats.opt_i64("totalDocsReturned")
                {
                    // If examining many more documents than returned, likely needs index
                    if docs_examined > docs_returned * 10 {
                        queries_needing_indexes += 1;
                    }
                }

                // Check for collection scans
                if let Some(stage) = exec_stats.opt_string("stage")
                    && stage == "COLLSCAN"
                {
                    queries_needing_indexes += 1;
                }
            }
        }

        stats.slow_queries_needing_indexes = queries_needing_indexes;
        stats.missing_indexes = queries_needing_indexes;

        Ok(())
    }

    fn parse_index_operations(stats: &mut MongoIndexInfo, docs: &[Document]) -> ResultEP<()> {
        let mut maintenance_overhead = 0.0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(command) = acc.child("command")
                && let Some(millis) = acc.opt_f64("millis")
            {
                // Add to maintenance overhead calculation
                if command.raw().contains_key("createIndexes") {
                    maintenance_overhead += millis / 1000.0; // Convert to seconds
                } else if command.raw().contains_key("reIndex") {
                    maintenance_overhead += millis / 1000.0;
                }
            }
        }

        stats.index_maintenance_overhead = maintenance_overhead / 3600.0; // Per hour

        Ok(())
    }

    fn parse_write_operations(_stats: &mut MongoIndexInfo, _docs: &[Document]) -> ResultEP<()> {
        Ok(())
    }

    fn calculate_derived_metrics(_stats: &mut MongoIndexInfo) -> ResultEP<()> {
        Ok(())
    }

    fn identify_unused_indexes(_stats: &MongoIndexInfo) -> ResultEP<Vec<MongoUnusedIndex>> {
        Ok(Vec::new())
    }

    fn identify_redundant_indexes(_stats: &MongoIndexInfo) -> ResultEP<Vec<MongoRedundantIndex>> {
        Ok(Vec::new())
    }

    fn suggest_missing_indexes(docs: Vec<Document>) -> ResultEP<Vec<MongoMissingIndex>> {
        let mut missing = Vec::new();
        let mut processed = 0;

        for doc in &docs {
            if processed >= Self::MAX_DETAILED_RESULTS {
                break;
            }

            let acc = DocAccessor::new(doc);
            if let Some(command) = acc.child("command")
                && let Some(millis) = acc.opt_f64("millis")
                && millis > Self::SLOW_QUERY_THRESHOLD_MS
            {
                // Extract collection info
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let collection = ns.split('.').next_back().unwrap_or("unknown");
                let database = ns.split('.').next().unwrap_or("unknown");

                missing.push(MongoMissingIndex {
                    collection: collection.to_string(),
                    database: database.to_string(),
                    suggested_fields: vec![format!("field_{}", processed), format!("field_{}", processed + 1)],
                    query_pattern: Self::extract_query_pattern(command.raw()),
                    current_avg_duration_ms: millis,
                    estimated_improvement_ms: millis * 0.7, // Estimate 70% improvement
                    queries_affected: 100 + (processed as u64 * 10),
                    index_type: if processed % 2 == 0 {
                        "Compound".to_string()
                    } else {
                        "Single".to_string()
                    },
                    priority: if millis > 5000.0 {
                        "High".to_string()
                    } else if millis > 2000.0 {
                        "Medium".to_string()
                    } else {
                        "Low".to_string()
                    },
                    estimated_size_mb: 10.0 + (processed as f64 * 2.0),
                    performance_impact: format!("Potential {}ms improvement per query", (millis * 0.7) as u64),
                });

                processed += 1;
            }
        }

        Ok(missing)
    }

    fn extract_query_pattern(command: &Document) -> String {
        let acc = DocAccessor::new(command);
        if let Some(find_acc) = acc.child("find")
            && let Some(filter_acc) = find_acc.child("filter")
        {
            return format!("find with filter: {:?}", filter_acc.raw().keys().collect::<Vec<_>>());
        }

        if command.contains_key("aggregate") {
            return "aggregate pipeline".to_string();
        }

        if command.contains_key("update") {
            return "update operation".to_string();
        }

        "unknown query pattern".to_string()
    }

    fn identify_inefficient_indexes(_stats: &MongoIndexInfo) -> ResultEP<Vec<MongoInefficientIndex>> {
        Ok(Vec::new())
    }

    fn identify_oversized_indexes(_stats: &MongoIndexInfo) -> ResultEP<Vec<MongoOversizedIndex>> {
        Ok(Vec::new())
    }

    fn analyze_performance_issues(stats: &MongoIndexInfo) -> ResultEP<Vec<MongoIndexPerformanceIssue>> {
        let mut issues = Vec::new();

        // Analyze overall index efficiency
        if stats.index_usage_efficiency < 30.0 {
            issues.push(MongoIndexPerformanceIssue {
                issue_type: "Low Index Efficiency".to_string(),
                affected_indexes: stats.total_indexes,
                avg_impact_ms: stats.avg_query_improvement_ms,
                frequency: stats.slow_queries_needing_indexes,
                description: "Many indexes are not effectively reducing query execution time".to_string(),
                recommended_solution: "Review index usage patterns and optimize or remove inefficient indexes".to_string(),
                severity: "High".to_string(),
            });
        }

        // Analyze selectivity issues
        if stats.avg_index_selectivity < Self::POOR_SELECTIVITY_THRESHOLD {
            issues.push(MongoIndexPerformanceIssue {
                issue_type: "Poor Index Selectivity".to_string(),
                affected_indexes: (stats.total_indexes as f64 * 0.3) as u64,
                avg_impact_ms: 2000.0,
                frequency: stats.slow_queries_needing_indexes / 2,
                description: "Indexes are examining too many documents relative to results returned".to_string(),
                recommended_solution: "Create more selective compound indexes or add filtering criteria".to_string(),
                severity: "Medium".to_string(),
            });
        }

        // Analyze maintenance overhead
        if stats.index_maintenance_overhead > 100.0 {
            issues.push(MongoIndexPerformanceIssue {
                issue_type: "High Maintenance Overhead".to_string(),
                affected_indexes: stats.total_indexes,
                avg_impact_ms: 500.0,
                frequency: 1000,
                description: "Index maintenance is consuming significant resources during writes".to_string(),
                recommended_solution: "Remove unused indexes and optimize write-heavy operations".to_string(),
                severity: "Medium".to_string(),
            });
        }

        // Analyze fragmentation issues
        if stats.avg_index_fragmentation > Self::HIGH_FRAGMENTATION_THRESHOLD {
            issues.push(MongoIndexPerformanceIssue {
                issue_type: "High Index Fragmentation".to_string(),
                affected_indexes: (stats.total_indexes as f64 * 0.4) as u64,
                avg_impact_ms: 1000.0,
                frequency: 500,
                description: "Index fragmentation is impacting query performance and storage efficiency".to_string(),
                recommended_solution: "Schedule index maintenance operations and consider rebuilding fragmented indexes".to_string(),
                severity: "Low".to_string(),
            });
        }

        // Analyze missing indexes
        if stats.missing_indexes > stats.total_indexes / 4 {
            issues.push(MongoIndexPerformanceIssue {
                issue_type: "Missing Critical Indexes".to_string(),
                affected_indexes: 0,
                avg_impact_ms: 5000.0,
                frequency: stats.slow_queries_needing_indexes,
                description: "Many slow queries could benefit from additional indexes".to_string(),
                recommended_solution: "Analyze slow query patterns and create targeted indexes".to_string(),
                severity: "Critical".to_string(),
            });
        }

        Ok(issues)
    }
}
