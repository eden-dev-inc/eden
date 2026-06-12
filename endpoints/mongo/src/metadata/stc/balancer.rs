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

use super::utils::{DocAccessor, fetch};
use crate::metadata::capabilities::MONGO_SHARDED_OR_MONGOS;

/// MongoDB balancer statistics and performance metrics
///
/// Simplified struct containing essential metrics about balancer
/// performance, chunk distribution, and migration patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoBalancerInfo {
    /// Whether the balancer is currently enabled
    pub balancer_enabled: bool,
    /// Whether the balancer is currently running
    pub balancer_active: bool,
    /// Total number of chunks across all collections
    pub total_chunks: u64,
    /// Number of chunks currently being migrated
    pub chunks_migrating: u64,
    /// Number of completed migrations in the last period
    pub completed_migrations: u64,
    /// Number of failed migrations in the last period
    pub failed_migrations: u64,
    /// Average migration time (milliseconds)
    pub avg_migration_time_ms: f64,
    /// Maximum migration time (milliseconds)
    pub max_migration_time_ms: f64,
    /// Minimum migration time (milliseconds)
    pub min_migration_time_ms: f64,
    /// Total bytes migrated in the last period
    pub total_bytes_migrated: u64,
    /// Average bytes per migration
    pub avg_bytes_per_migration: f64,
    /// Number of imbalanced collections
    pub imbalanced_collections: u64,
    /// Maximum chunk size difference between shards
    pub max_chunk_imbalance: u64,
    /// Number of jumbo chunks (oversized chunks)
    pub jumbo_chunks: u64,
    /// Number of shards in the cluster
    pub total_shards: u64,
    /// Balancer round duration (milliseconds)
    pub balancer_round_duration_ms: f64,
    /// Last balancer round timestamp
    pub last_balancer_round: Option<DateTimeWrapper>,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoBalancerDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoBalancerDetailedMetrics {
    /// Failed migrations (only collected when failed_migrations > threshold)
    pub failed_migrations: Vec<MongoFailedMigration>,
    /// Slow migrations (only collected when max_migration_time_ms > threshold)
    pub slow_migrations: Vec<MongoSlowMigration>,
    /// Imbalanced collections (only collected when imbalance detected)
    pub imbalanced_collections: Option<Vec<MongoImbalancedCollection>>,
    /// Jumbo chunks (only collected when jumbo chunks exist)
    pub jumbo_chunks: Vec<MongoJumboChunk>,
    /// Shard distribution details (collected less frequently)
    pub shard_distribution: Option<Vec<MongoShardDistribution>>,
}

impl MetadataCollection for MongoBalancerInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "balancer_settings".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "settings".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "_id": "balancer"
                    })),
                    None,
                ),
            ),
            (
                "balancer_status".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "mongos".to_string(),
                    None,
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ping": -1 })).with_limit(1)),
                ),
            ),
            (
                "chunk_info".to_string(),
                FindInput::new("config".to_string(), "chunks".to_string(), None, Some(FindOptionsWrapper::new().with_limit(1000))),
            ),
            (
                "migration_log".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "changelog".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "what": { "$in": ["moveChunk.start", "moveChunk.commit", "moveChunk.error"] },
                        "time": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "time": -1 })).with_limit(200)),
                ),
            ),
            (
                "collections_info".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "collections".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "dropped": false
                    })),
                    None,
                ),
            ),
            ("shards_info".to_string(), FindInput::new("config".to_string(), "shards".to_string(), None, None)),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential balancer metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "balancer"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl MongoBalancerInfo {
    const SLOW_MIGRATION_THRESHOLD_MS: f64 = 30000.0; // 30 seconds
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        if !capabilities.has(&MONGO_SHARDED_OR_MONGOS) {
            return Ok(MongoBalancerInfo::default());
        }

        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut balancer_stats = MongoBalancerInfo::default();
        let requests = self.request();

        // Execute queries to get balancer information
        let balancer_settings = fetch(&requests, "balancer_settings", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_balancer_settings(&mut balancer_stats, &balancer_settings)?;

        let chunks = fetch(&requests, "chunk_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_chunk_info(&mut balancer_stats, &chunks)?;

        let migration_logs = fetch(&requests, "migration_log", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_migration_logs(&mut balancer_stats, &migration_logs)?;

        let shards = fetch(&requests, "shards_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_shards_info(&mut balancer_stats, &shards)?;

        // Conditionally collect detailed metrics only when problems are detected
        balancer_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&balancer_stats, &requests, context).await?;

        Ok(balancer_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoBalancerInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoBalancerDetailedMetrics>> {
        let needs_failed_migration_details = core_stats.failed_migrations > 0;
        let needs_slow_migration_details = core_stats.max_migration_time_ms > Self::SLOW_MIGRATION_THRESHOLD_MS;
        let needs_imbalance_details = core_stats.imbalanced_collections > 0;
        let needs_jumbo_details = core_stats.jumbo_chunks > 0;

        if !needs_failed_migration_details && !needs_slow_migration_details && !needs_imbalance_details && !needs_jumbo_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoBalancerDetailedMetrics {
            failed_migrations: Vec::new(),
            slow_migrations: Vec::new(),
            imbalanced_collections: None,
            jumbo_chunks: Vec::new(),
            shard_distribution: None,
        };

        // Collect detailed migration data if needed
        if needs_failed_migration_details || needs_slow_migration_details {
            let docs = fetch(requests, "migration_log", context.clone(), Self::QUERY_TIMEOUT).await?;
            if needs_failed_migration_details {
                detailed_metrics.failed_migrations = Self::parse_failed_migrations(docs.clone())?;
            }
            if needs_slow_migration_details {
                detailed_metrics.slow_migrations = Self::parse_slow_migrations(docs)?;
            }
        }

        // Collect imbalanced collections if needed
        if needs_imbalance_details {
            let docs = fetch(requests, "collections_info", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.imbalanced_collections = Some(Self::parse_imbalanced_collections(docs)?);
        }

        // Collect jumbo chunks if needed
        if needs_jumbo_details {
            let docs = fetch(requests, "chunk_info", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.jumbo_chunks = Self::parse_jumbo_chunks(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_balancer_settings(stats: &mut MongoBalancerInfo, docs: &[Document]) -> ResultEP<()> {
        if let Some(settings_doc) = docs.first() {
            let acc = DocAccessor::new(settings_doc);
            // Check if balancer is enabled (default is true if not explicitly disabled)
            stats.balancer_enabled = !acc.opt_bool("stopped").unwrap_or(false);

            // Check for active balancer window
            if acc.child("activeWindow").is_some() {
                stats.balancer_active = true;
            }
        } else {
            // No settings document means balancer is enabled by default
            stats.balancer_enabled = true;
        }

        Ok(())
    }

    fn parse_chunk_info(stats: &mut MongoBalancerInfo, docs: &[Document]) -> ResultEP<()> {
        stats.total_chunks = docs.len() as u64;

        let mut jumbo_count = 0;
        let mut migrating_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            // Check for jumbo chunks
            if acc.opt_bool("jumbo").unwrap_or(false) {
                jumbo_count += 1;
            }

            // Check for chunks being migrated (simplified check)
            if let Some(history) = acc.array("history")
                && !history.is_empty()
            {
                migrating_count += 1;
            }
        }

        stats.jumbo_chunks = jumbo_count;
        stats.chunks_migrating = migrating_count;

        Ok(())
    }

    fn parse_migration_logs(stats: &mut MongoBalancerInfo, docs: &[Document]) -> ResultEP<()> {
        let mut migration_times = Vec::new();
        let mut completed_migrations = 0;
        let mut failed_migrations = 0;
        let mut total_bytes = 0u64;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(what) = acc.opt_string("what") {
                match what.as_str() {
                    "moveChunk.commit" => {
                        completed_migrations += 1;

                        // Extract migration time if available
                        if let Some(details) = acc.child("details") {
                            if let Some(millis) = details.opt_f64("millis") {
                                migration_times.push(millis);
                            }

                            // Extract bytes migrated
                            if let Some(bytes) = details.opt_i64("bytes") {
                                total_bytes += bytes as u64;
                            }
                        }
                    }
                    "moveChunk.error" => {
                        failed_migrations += 1;
                    }
                    _ => {}
                }
            }
        }

        stats.completed_migrations = completed_migrations;
        stats.failed_migrations = failed_migrations;
        stats.total_bytes_migrated = total_bytes;

        if !migration_times.is_empty() {
            stats.avg_migration_time_ms = migration_times.iter().sum::<f64>() / migration_times.len() as f64;
            stats.max_migration_time_ms = migration_times.iter().fold(0.0f64, |a, &b| a.max(b));
            stats.min_migration_time_ms = migration_times.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        }

        if stats.completed_migrations > 0 {
            stats.avg_bytes_per_migration = stats.total_bytes_migrated as f64 / stats.completed_migrations as f64;
        }

        Ok(())
    }

    fn parse_shards_info(stats: &mut MongoBalancerInfo, docs: &[Document]) -> ResultEP<()> {
        stats.total_shards = docs.len() as u64;
        Ok(())
    }

    fn parse_failed_migrations(docs: Vec<Document>) -> ResultEP<Vec<MongoFailedMigration>> {
        let mut migrations = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);
            if let Some(what) = acc.opt_string("what")
                && what == "moveChunk.error"
                && let (Some(time), Some(ns)) = (acc.opt_datetime("time"), acc.opt_string("ns"))
            {
                let details = acc.child("details");
                let error_msg = details.as_ref().and_then(|d| d.opt_string("errmsg")).unwrap_or_else(|| "Unknown error".to_string());

                migrations.push(MongoFailedMigration {
                    namespace: ns,
                    timestamp: time,
                    error_message: error_msg,
                    from_shard: Some(details.as_ref().and_then(|d| d.opt_string("from")).unwrap_or_default()),
                    to_shard: Some(details.as_ref().and_then(|d| d.opt_string("to")).unwrap_or_default()),
                });
            }
        }

        Ok(migrations)
    }

    fn parse_slow_migrations(docs: Vec<Document>) -> ResultEP<Vec<MongoSlowMigration>> {
        let mut migrations = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);
            if let Some(what) = acc.opt_string("what")
                && what == "moveChunk.commit"
                && let (Some(time), Some(ns)) = (acc.opt_datetime("time"), acc.opt_string("ns"))
                && let Some(details) = acc.child("details")
                && let Some(millis) = details.opt_f64("millis")
                && millis > MongoBalancerInfo::SLOW_MIGRATION_THRESHOLD_MS
            {
                migrations.push(MongoSlowMigration {
                    namespace: ns,
                    execution_time_ms: millis,
                    timestamp: time,
                    bytes_migrated: details.opt_i64("bytes").unwrap_or(0) as u64,
                    from_shard: details.opt_string("from"),
                    to_shard: details.opt_string("to"),
                });
            }
        }

        Ok(migrations)
    }

    fn parse_imbalanced_collections(_docs: Vec<Document>) -> ResultEP<Vec<MongoImbalancedCollection>> {
        Ok(Vec::new())
    }

    fn parse_jumbo_chunks(docs: Vec<Document>) -> ResultEP<Vec<MongoJumboChunk>> {
        let mut chunks = Vec::new();
        let empty_doc = Document::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);
            if acc.opt_bool("jumbo").unwrap_or(false)
                && let (Some(ns), Some(shard)) = (acc.opt_string("ns"), acc.opt_string("shard"))
            {
                let last_modified = acc.opt_datetime("lastmod").unwrap_or_else(|| DateTimeWrapper::from(Utc::now()));

                chunks.push(MongoJumboChunk {
                    namespace: ns,
                    shard,
                    chunk_id: acc.opt_string("_id").unwrap_or_else(|| "unknown".to_string()),
                    min_key: format!("{:?}", acc.child("min").map(|a| a.raw().clone()).unwrap_or_else(|| empty_doc.clone())),
                    max_key: format!("{:?}", acc.child("max").map(|a| a.raw().clone()).unwrap_or_else(|| empty_doc.clone())),
                    estimated_size_mb: 0.0,
                    last_modified,
                });
            }
        }

        Ok(chunks)
    }
}

/// Information about failed migration operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedMigration {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Timestamp when the migration failed
    pub timestamp: DateTimeWrapper,
    /// Error message
    pub error_message: String,
    /// Source shard
    pub from_shard: Option<String>,
    /// Destination shard
    pub to_shard: Option<String>,
}

/// Information about slow migration operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowMigration {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Migration execution time in milliseconds
    pub execution_time_ms: f64,
    /// Timestamp when the migration completed
    pub timestamp: DateTimeWrapper,
    /// Number of bytes migrated
    pub bytes_migrated: u64,
    /// Source shard
    pub from_shard: Option<String>,
    /// Destination shard
    pub to_shard: Option<String>,
}

/// Information about imbalanced collections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoImbalancedCollection {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Total number of chunks for this collection
    pub chunk_count: u64,
    /// Percentage of imbalance between shards
    pub imbalance_percentage: f64,
    /// Number of chunks on the shard with most chunks
    pub largest_shard_chunks: u64,
    /// Number of chunks on the shard with least chunks
    pub smallest_shard_chunks: u64,
    /// Recommended action to fix imbalance
    pub recommended_action: String,
}

/// Information about jumbo chunks
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoJumboChunk {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Shard containing the jumbo chunk
    pub shard: String,
    /// Chunk identifier
    pub chunk_id: String,
    /// Minimum key of the chunk range
    pub min_key: String,
    /// Maximum key of the chunk range
    pub max_key: String,
    /// Estimated size in megabytes
    pub estimated_size_mb: f64,
    /// Last modification timestamp
    pub last_modified: DateTimeWrapper,
}

/// Shard distribution information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoShardDistribution {
    /// Shard name
    pub shard_name: String,
    /// Total number of chunks on this shard
    pub chunk_count: u64,
    /// Estimated data size on this shard (MB)
    pub data_size_mb: f64,
    /// Percentage of total data on this shard
    pub data_percentage: f64,
    /// Whether this shard is considered overloaded
    pub is_overloaded: bool,
}

impl MongoBalancerInfo {
    /// Checks if the balancer is healthy (enabled and not experiencing issues)
    pub fn is_balancer_healthy(&self) -> bool {
        self.balancer_enabled && self.failed_migrations == 0 && self.imbalanced_collections == 0 && self.jumbo_chunks == 0
    }

    /// Returns the migration success rate as a percentage
    pub fn migration_success_rate(&self) -> f64 {
        let total_migrations = self.completed_migrations + self.failed_migrations;
        if total_migrations == 0 {
            100.0
        } else {
            (self.completed_migrations as f64 / total_migrations as f64) * 100.0
        }
    }

    /// Checks if there are any performance issues with migrations
    pub fn has_migration_performance_issues(&self) -> bool {
        self.max_migration_time_ms > Self::SLOW_MIGRATION_THRESHOLD_MS || self.failed_migrations > 0
    }

    /// Checks if the cluster has distribution issues
    pub fn has_distribution_issues(&self) -> bool {
        self.imbalanced_collections > 0 || self.jumbo_chunks > 0
    }

    /// Returns the percentage of chunks that are jumbo
    pub fn jumbo_chunk_percentage(&self) -> f64 {
        if self.total_chunks == 0 {
            0.0
        } else {
            (self.jumbo_chunks as f64 / self.total_chunks as f64) * 100.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns the average throughput in MB per second for migrations
    pub fn migration_throughput_mbps(&self) -> f64 {
        if self.avg_migration_time_ms > 0.0 && self.avg_bytes_per_migration > 0.0 {
            let mb_per_migration = self.avg_bytes_per_migration / 1024.0 / 1024.0;
            let seconds_per_migration = self.avg_migration_time_ms / 1000.0;
            mb_per_migration / seconds_per_migration
        } else {
            0.0
        }
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_balancer_stats() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;
        let balancer_stats = MongoBalancerInfo::default();

        let result = balancer_stats
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_balancer_health_check() {
        let mut stats = MongoBalancerInfo {
            balancer_enabled: true,
            failed_migrations: 0,
            imbalanced_collections: 0,
            jumbo_chunks: 0,
            ..MongoBalancerInfo::default()
        };

        assert!(stats.is_balancer_healthy());

        stats.failed_migrations = 1;
        assert!(!stats.is_balancer_healthy());
    }

    #[test]
    fn test_migration_success_rate() {
        let stats = MongoBalancerInfo {
            completed_migrations: 8,
            failed_migrations: 2,
            ..MongoBalancerInfo::default()
        };

        assert_eq!(stats.migration_success_rate(), 80.0);
    }

    #[test]
    fn test_jumbo_chunk_percentage() {
        let stats = MongoBalancerInfo {
            total_chunks: 100,
            jumbo_chunks: 5,
            ..MongoBalancerInfo::default()
        };

        assert_eq!(stats.jumbo_chunk_percentage(), 5.0);
    }
}
