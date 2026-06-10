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

/// MongoDB collections statistics and performance metrics
///
/// Simplified struct containing essential metrics about collection
/// performance, storage patterns, and usage statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionInfo {
    /// Total number of collections across all databases
    pub total_collections: u64,
    /// Number of sharded collections
    pub sharded_collections: u64,
    /// Number of capped collections
    pub capped_collections: u64,
    /// Number of time-series collections
    pub timeseries_collections: u64,
    /// Total document count across all collections
    pub total_documents: u64,
    /// Total storage size across all collections (bytes)
    pub total_storage_size_bytes: u64,
    /// Total index size across all collections (bytes)
    pub total_index_size_bytes: u64,
    /// Average document size (bytes)
    pub avg_document_size_bytes: f64,
    /// Number of collections with indexes
    pub collections_with_indexes: u64,
    /// Total number of indexes across all collections
    pub total_indexes: u64,
    /// Number of unused indexes
    pub unused_indexes: u64,
    /// Number of collections with text indexes
    pub collections_with_text_indexes: u64,
    /// Number of collections with compound indexes
    pub collections_with_compound_indexes: u64,
    /// Storage efficiency ratio (data size / storage size)
    pub storage_efficiency_ratio: f64,
    /// Number of collections requiring attention (large, fragmented, etc.)
    pub collections_needing_attention: u64,
    /// Average collection age in days
    pub avg_collection_age_days: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoCollectionsDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionsDetailedMetrics {
    /// Large collections (only collected when size thresholds exceeded)
    pub large_collections: Vec<MongoLargeCollection>,
    /// Collections with poor storage efficiency
    pub inefficient_collections: Vec<MongoInefficientCollection>,
    /// Unused or redundant indexes
    pub unused_indexes: Vec<MongoUnusedIndex>,
    /// Collections with missing recommended indexes
    pub missing_indexes: Vec<MongoMissingIndex>,
    /// Fragmented collections (collected less frequently)
    pub fragmented_collections: Option<Vec<MongoFragmentedCollection>>,
    /// Collection growth patterns (collected periodically)
    pub growth_patterns: Option<Vec<MongoCollectionGrowth>>,
}

impl MetadataCollection for MongoCollectionInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "collections_list".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "collections".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "dropped": false
                    })),
                    None,
                ),
            ),
            (
                "database_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.dbStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(20)),
                ),
            ),
            (
                "collection_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.collStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "index_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.indexStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "recent_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "op": { "$in": ["insert", "update", "delete", "query"] },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential collection metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "collections"
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

impl MongoCollectionInfo {
    const LARGE_COLLECTION_THRESHOLD_MB: f64 = 1000.0; // 1GB
    const STORAGE_EFFICIENCY_THRESHOLD: f64 = 0.5; // 50% efficiency
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut collections_stats = MongoCollectionInfo::default();
        let requests = self.request();

        // Execute queries to get collection information
        let collections = fetch(&requests, "collections_list", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_collections_list(&mut collections_stats, &collections)?;

        let db_stats = fetch(&requests, "database_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_database_stats(&mut collections_stats, &db_stats)?;

        let collection_stats = fetch(&requests, "collection_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_collection_stats(&mut collections_stats, &collection_stats)?;

        let index_stats = fetch(&requests, "index_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_index_stats(&mut collections_stats, &index_stats)?;

        let operations = fetch(&requests, "recent_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_recent_operations(&mut collections_stats, &operations)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut collections_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        collections_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&collections_stats, &requests, context).await?;

        Ok(collections_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoCollectionInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoCollectionsDetailedMetrics>> {
        let needs_large_collection_details =
            core_stats.total_storage_size_bytes > (Self::LARGE_COLLECTION_THRESHOLD_MB * 1024.0 * 1024.0 * 10.0) as u64;
        let needs_efficiency_details = core_stats.storage_efficiency_ratio < Self::STORAGE_EFFICIENCY_THRESHOLD;
        let needs_index_details = core_stats.unused_indexes > 0;
        let needs_attention_details = core_stats.collections_needing_attention > 0;

        if !needs_large_collection_details && !needs_efficiency_details && !needs_index_details && !needs_attention_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoCollectionsDetailedMetrics {
            large_collections: Vec::new(),
            inefficient_collections: Vec::new(),
            unused_indexes: Vec::new(),
            missing_indexes: Vec::new(),
            fragmented_collections: None,
            growth_patterns: None,
        };

        // Collect large collections if needed
        if needs_large_collection_details {
            let docs = fetch(requests, "collection_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.large_collections = Self::parse_large_collections(docs)?;
        }

        // Collect inefficient collections if needed
        if needs_efficiency_details {
            let docs = fetch(requests, "collection_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.inefficient_collections = Self::parse_inefficient_collections(docs)?;
        }

        // Collect unused indexes if needed
        if needs_index_details {
            let docs = fetch(requests, "index_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.unused_indexes = Self::parse_unused_indexes(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_collections_list(stats: &mut MongoCollectionInfo, docs: &[Document]) -> ResultEP<()> {
        stats.total_collections = docs.len() as u64;

        let mut sharded_count = 0;
        let mut timeseries_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            // Check if collection is sharded
            if !acc.opt_bool("dropped").unwrap_or(false) && acc.child("key").is_some() {
                sharded_count += 1;
            }

            // Check for time-series collections
            if let Some(options) = acc.child("options")
                && options.raw().contains_key("timeseries")
            {
                timeseries_count += 1;
            }
        }

        stats.sharded_collections = sharded_count;
        stats.timeseries_collections = timeseries_count;

        Ok(())
    }

    fn parse_database_stats(stats: &mut MongoCollectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut total_storage = 0u64;
        let mut total_index_size = 0u64;
        let mut total_docs = 0u64;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.child("result") {
                if let Some(storage_size) = result.opt_i64("storageSize") {
                    total_storage += storage_size as u64;
                }
                if let Some(index_size) = result.opt_i64("indexSize") {
                    total_index_size += index_size as u64;
                }
                if let Some(objects) = result.opt_i64("objects") {
                    total_docs += objects as u64;
                }
            }
        }

        stats.total_storage_size_bytes += total_storage;
        stats.total_index_size_bytes += total_index_size;
        stats.total_documents += total_docs;

        Ok(())
    }

    fn parse_collection_stats(stats: &mut MongoCollectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut capped_count = 0;
        let mut collections_with_indexes_count = 0;
        let mut total_indexes_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.child("result") {
                // Check for capped collections
                if result.opt_bool("capped").unwrap_or(false) {
                    capped_count += 1;
                }

                // Count indexes
                if let Some(index_count) = result.opt_i32("nindexes") {
                    total_indexes_count += index_count as u64;
                    if index_count > 1 {
                        // More than just _id index
                        collections_with_indexes_count += 1;
                    }
                }

                // Additional storage metrics would be parsed here
                if let Some(storage_size) = result.opt_i64("storageSize") {
                    stats.total_storage_size_bytes += storage_size as u64;
                }

                if let Some(total_index_size) = result.opt_i64("totalIndexSize") {
                    stats.total_index_size_bytes += total_index_size as u64;
                }

                if let Some(count) = result.opt_i64("count") {
                    stats.total_documents += count as u64;
                }
            }
        }

        stats.capped_collections += capped_count;
        stats.collections_with_indexes += collections_with_indexes_count;
        stats.total_indexes += total_indexes_count;

        Ok(())
    }

    fn parse_index_stats(stats: &mut MongoCollectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut text_indexes_count = 0;
        let mut compound_indexes_count = 0;
        let mut unused_indexes_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.array("result") {
                for index in result {
                    // Check for text indexes
                    if let Some(key) = index.child("key") {
                        if key.raw().values().any(|v| v.as_str() == Some("text")) {
                            text_indexes_count += 1;
                        }
                        // Check for compound indexes (more than one field)
                        if key.raw().len() > 1 {
                            compound_indexes_count += 1;
                        }
                    }

                    // Check for unused indexes (simplified check)
                    if let Some(accesses) = index.child("accesses")
                        && let Some(ops) = accesses.opt_i64("ops")
                        && ops == 0
                    {
                        unused_indexes_count += 1;
                    }
                }
            }
        }

        stats.collections_with_text_indexes = text_indexes_count;
        stats.collections_with_compound_indexes = compound_indexes_count;
        stats.unused_indexes = unused_indexes_count;

        Ok(())
    }

    fn parse_recent_operations(stats: &mut MongoCollectionInfo, _docs: &[Document]) -> ResultEP<()> {
        // This would analyze recent operations to identify patterns
        // For now, we'll just update the collections needing attention count
        // based on operation patterns (simplified)

        // This is a placeholder - would implement actual analysis
        stats.collections_needing_attention = 0;

        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoCollectionInfo) -> ResultEP<()> {
        // Calculate average document size
        if stats.total_documents > 0 {
            stats.avg_document_size_bytes = stats.total_storage_size_bytes as f64 / stats.total_documents as f64;
        }

        // Calculate storage efficiency ratio
        let total_size = stats.total_storage_size_bytes + stats.total_index_size_bytes;
        if total_size > 0 {
            stats.storage_efficiency_ratio = stats.total_storage_size_bytes as f64 / total_size as f64;
        }

        Ok(())
    }

    fn parse_large_collections(docs: Vec<Document>) -> ResultEP<Vec<MongoLargeCollection>> {
        let mut collections = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.child("result")
                && let (Some(storage_size), Some(ns)) = (result.opt_i64("storageSize"), acc.opt_string("ns"))
            {
                let size_mb = storage_size as f64 / 1024.0 / 1024.0;
                if size_mb > MongoCollectionInfo::LARGE_COLLECTION_THRESHOLD_MB {
                    let count = result.opt_i64("count").unwrap_or(0);
                    collections.push(MongoLargeCollection {
                        namespace: ns,
                        storage_size_mb: size_mb,
                        document_count: count as u64,
                        avg_document_size_bytes: if count > 0 { storage_size as f64 / count as f64 } else { 0.0 },
                        index_count: result.opt_i32("nindexes").unwrap_or(0) as u64,
                        index_size_mb: result.opt_i64("totalIndexSize").map(|s| s as f64 / 1024.0 / 1024.0).unwrap_or(0.0),
                        is_sharded: false, // Would check sharding status
                        recommended_action: "Consider sharding or archiving old data".to_string(),
                    });
                }
            }
        }

        Ok(collections)
    }

    fn parse_inefficient_collections(docs: Vec<Document>) -> ResultEP<Vec<MongoInefficientCollection>> {
        let mut collections = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.child("result")
                && let (Some(storage_size), Some(size), Some(ns)) =
                    (result.opt_i64("storageSize"), result.opt_i64("size"), acc.opt_string("ns"))
            {
                let efficiency = if storage_size > 0 { size as f64 / storage_size as f64 } else { 0.0 };

                if efficiency < MongoCollectionInfo::STORAGE_EFFICIENCY_THRESHOLD {
                    collections.push(MongoInefficientCollection {
                        namespace: ns,
                        storage_efficiency_ratio: efficiency,
                        storage_size_mb: storage_size as f64 / 1024.0 / 1024.0,
                        data_size_mb: size as f64 / 1024.0 / 1024.0,
                        wasted_space_mb: (storage_size - size) as f64 / 1024.0 / 1024.0,
                        fragmentation_percentage: ((storage_size - size) as f64 / storage_size as f64) * 100.0,
                        recommended_action: "Consider running compact operation".to_string(),
                        last_compaction: None, // Would get actual compaction date
                    });
                }
            }
        }

        Ok(collections)
    }

    fn parse_unused_indexes(docs: Vec<Document>) -> ResultEP<Vec<MongoUnusedIndex>> {
        let mut indexes = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result) = acc.array("result") {
                let namespace = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());

                for index in result {
                    if let Some(accesses) = index.child("accesses")
                        && let Some(ops) = accesses.opt_i64("ops")
                        && ops == 0
                    {
                        indexes.push(MongoUnusedIndex {
                            namespace: namespace.clone(),
                            index_name: index.opt_string("name").unwrap_or_else(|| "unknown".to_string()),
                            index_definition: format!("{:?}", index.child("key").map(|k| k.raw().clone()).unwrap_or_default()),
                            size_mb: index.opt_i64("size").map(|s| s as f64 / 1024.0 / 1024.0).unwrap_or(0.0),
                            last_used: None, // Would get actual last used date
                            days_unused: 30, // Would calculate actual days
                            recommended_action: "Consider dropping this index".to_string(),
                        });
                    }
                }
            }
        }

        Ok(indexes)
    }
}

/// Information about large collections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLargeCollection {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Storage size in megabytes
    pub storage_size_mb: f64,
    /// Number of documents
    pub document_count: u64,
    /// Average document size in bytes
    pub avg_document_size_bytes: f64,
    /// Number of indexes
    pub index_count: u64,
    /// Total index size in megabytes
    pub index_size_mb: f64,
    /// Whether the collection is sharded
    pub is_sharded: bool,
    /// Recommended action
    pub recommended_action: String,
}

/// Information about collections with poor storage efficiency
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoInefficientCollection {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Storage efficiency ratio (data size / storage size)
    pub storage_efficiency_ratio: f64,
    /// Total storage size in megabytes
    pub storage_size_mb: f64,
    /// Actual data size in megabytes
    pub data_size_mb: f64,
    /// Wasted space in megabytes
    pub wasted_space_mb: f64,
    /// Fragmentation percentage
    pub fragmentation_percentage: f64,
    /// Recommended action
    pub recommended_action: String,
    /// Last compaction timestamp
    pub last_compaction: Option<DateTimeWrapper>,
}

/// Information about unused indexes
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUnusedIndex {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Index name
    pub index_name: String,
    /// Index definition
    pub index_definition: String,
    /// Index size in megabytes
    pub size_mb: f64,
    /// Last time the index was used
    pub last_used: Option<DateTimeWrapper>,
    /// Number of days since last use
    pub days_unused: u64,
    /// Recommended action
    pub recommended_action: String,
}

/// Information about missing recommended indexes
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoMissingIndex {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Recommended index definition
    pub recommended_index: String,
    /// Reason for recommendation
    pub reason: String,
    /// Estimated performance improvement
    pub estimated_improvement: String,
    /// Query patterns that would benefit
    pub query_patterns: Vec<String>,
}

/// Information about fragmented collections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFragmentedCollection {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Fragmentation percentage
    pub fragmentation_percentage: f64,
    /// Wasted space in megabytes
    pub wasted_space_mb: f64,
    /// Last compaction date
    pub last_compaction: Option<DateTimeWrapper>,
    /// Recommended compaction frequency
    pub recommended_compaction_frequency: String,
}

/// Collection growth pattern information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoCollectionGrowth {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Growth rate in documents per day
    pub docs_per_day: f64,
    /// Growth rate in megabytes per day
    pub mb_per_day: f64,
    /// Projected size in 30 days (MB)
    pub projected_size_30d_mb: f64,
    /// Growth trend (growing, stable, shrinking)
    pub growth_trend: String,
    /// Recommended capacity planning action
    pub capacity_recommendation: String,
}

impl MongoCollectionInfo {
    /// Checks if collections are generally healthy
    pub fn is_collections_healthy(&self) -> bool {
        self.storage_efficiency_ratio > Self::STORAGE_EFFICIENCY_THRESHOLD
            && self.unused_indexes == 0
            && self.collections_needing_attention == 0
    }

    /// Returns the percentage of collections that are sharded
    pub fn sharding_percentage(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            (self.sharded_collections as f64 / self.total_collections as f64) * 100.0
        }
    }

    /// Returns the average indexes per collection
    pub fn avg_indexes_per_collection(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            self.total_indexes as f64 / self.total_collections as f64
        }
    }

    /// Returns the index to storage ratio
    pub fn index_to_storage_ratio(&self) -> f64 {
        if self.total_storage_size_bytes == 0 {
            0.0
        } else {
            self.total_index_size_bytes as f64 / self.total_storage_size_bytes as f64
        }
    }

    /// Checks if there are storage efficiency issues
    pub fn has_storage_efficiency_issues(&self) -> bool {
        self.storage_efficiency_ratio < Self::STORAGE_EFFICIENCY_THRESHOLD
    }

    /// Checks if there are unused indexes
    pub fn has_unused_indexes(&self) -> bool {
        self.unused_indexes > 0
    }

    /// Returns the percentage of collections that are capped
    pub fn capped_collections_percentage(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            (self.capped_collections as f64 / self.total_collections as f64) * 100.0
        }
    }

    /// Returns the percentage of collections that are time-series
    pub fn timeseries_collections_percentage(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            (self.timeseries_collections as f64 / self.total_collections as f64) * 100.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns the total storage size in megabytes
    pub fn total_storage_size_mb(&self) -> f64 {
        self.total_storage_size_bytes as f64 / 1024.0 / 1024.0
    }

    /// Returns the total index size in megabytes
    pub fn total_index_size_mb(&self) -> f64 {
        self.total_index_size_bytes as f64 / 1024.0 / 1024.0
    }

    /// Returns the percentage of collections that have custom indexes
    pub fn collections_with_custom_indexes_percentage(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            (self.collections_with_indexes as f64 / self.total_collections as f64) * 100.0
        }
    }

    /// Calculates storage density (documents per MB)
    pub fn storage_density(&self) -> f64 {
        let total_storage_mb = self.total_storage_size_mb();
        if total_storage_mb > 0.0 {
            self.total_documents as f64 / total_storage_mb
        } else {
            0.0
        }
    }

    /// Returns a health score from 0-100 based on various metrics
    pub fn health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct points for storage efficiency issues
        if self.storage_efficiency_ratio < 0.8 {
            score -= 20.0;
        } else if self.storage_efficiency_ratio < 0.9 {
            score -= 10.0;
        }

        // Deduct points for unused indexes
        if self.unused_indexes > 0 {
            let unused_percentage = (self.unused_indexes as f64 / self.total_indexes as f64) * 100.0;
            score -= unused_percentage.min(30.0); // Max 30 point deduction
        }

        // Deduct points for collections needing attention
        if self.collections_needing_attention > 0 {
            let attention_percentage = (self.collections_needing_attention as f64 / self.total_collections as f64) * 100.0;
            score -= attention_percentage.min(25.0); // Max 25 point deduction
        }

        // Bonus points for good practices
        if self.collections_with_indexes > (self.total_collections / 2) {
            score += 5.0; // Bonus for good index coverage
        }

        score.clamp(0.0, 100.0)
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_collections_stats() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let collections_stats = MongoCollectionInfo::default();

        let result = collections_stats
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_collections_health_check() {
        let mut stats = MongoCollectionInfo {
            storage_efficiency_ratio: 0.8,
            unused_indexes: 0,
            collections_needing_attention: 0,
            ..MongoCollectionInfo::default()
        };

        assert!(stats.is_collections_healthy());

        stats.unused_indexes = 5;
        assert!(!stats.is_collections_healthy());
    }

    #[test]
    fn test_sharding_percentage() {
        let stats = MongoCollectionInfo {
            total_collections: 10,
            sharded_collections: 3,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.sharding_percentage(), 30.0);
    }

    #[test]
    fn test_avg_indexes_per_collection() {
        let stats = MongoCollectionInfo {
            total_collections: 5,
            total_indexes: 15,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.avg_indexes_per_collection(), 3.0);
    }

    #[test]
    fn test_storage_density() {
        let stats = MongoCollectionInfo {
            total_documents: 1000,
            total_storage_size_bytes: 10 * 1024 * 1024,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.storage_density(), 100.0); // 1000 docs / 10 MB = 100 docs/MB
    }

    #[test]
    fn test_health_score() {
        let mut stats = MongoCollectionInfo {
            storage_efficiency_ratio: 0.9,
            unused_indexes: 0,
            collections_needing_attention: 0,
            total_collections: 10,
            collections_with_indexes: 8,
            ..MongoCollectionInfo::default()
        };

        let score = stats.health_score();
        assert!(score >= 95.0); // Should be high with good metrics

        stats.storage_efficiency_ratio = 0.5;
        stats.unused_indexes = 5;
        stats.total_indexes = 10;

        let score2 = stats.health_score();
        assert!(score2 < score); // Should be lower with worse metrics
    }

    #[test]
    fn test_index_to_storage_ratio() {
        let stats = MongoCollectionInfo {
            total_storage_size_bytes: 1000,
            total_index_size_bytes: 200,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.index_to_storage_ratio(), 0.2); // 20% index overhead
    }

    #[test]
    fn test_collection_type_percentages() {
        let stats = MongoCollectionInfo {
            total_collections: 100,
            capped_collections: 10,
            timeseries_collections: 5,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.capped_collections_percentage(), 10.0);
        assert_eq!(stats.timeseries_collections_percentage(), 5.0);
    }

    #[test]
    fn test_storage_size_conversions() {
        let stats = MongoCollectionInfo {
            total_storage_size_bytes: 2048 * 1024 * 1024,
            total_index_size_bytes: 512 * 1024 * 1024,
            ..MongoCollectionInfo::default()
        };

        assert_eq!(stats.total_storage_size_mb(), 2048.0);
        assert_eq!(stats.total_index_size_mb(), 512.0);
    }

    #[test]
    fn test_efficiency_thresholds() {
        let mut stats = MongoCollectionInfo {
            storage_efficiency_ratio: 0.4,
            ..MongoCollectionInfo::default()
        };

        assert!(stats.has_storage_efficiency_issues());

        stats.storage_efficiency_ratio = 0.6;
        assert!(!stats.has_storage_efficiency_issues());

        stats.unused_indexes = 3;
        assert!(stats.has_unused_indexes());

        stats.unused_indexes = 0;
        assert!(!stats.has_unused_indexes());
    }
}
