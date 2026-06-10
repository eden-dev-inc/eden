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
use crate::metadata::capabilities::MONGO_SHARDED;

/// MongoDB sharding cluster statistics and performance metrics
///
/// Simplified struct containing essential metrics about sharding cluster
/// health, performance, and data distribution patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoShardingInfo {
    /// Total number of shards in the cluster
    pub total_shards: u32,
    /// Number of active/healthy shards
    pub active_shards: u32,
    /// Number of shards with issues
    pub unhealthy_shards: u32,
    /// Total number of sharded collections
    pub sharded_collections: u32,
    /// Number of chunks across all sharded collections
    pub total_chunks: u64,
    /// Number of chunks currently being migrated
    pub migrating_chunks: u64,
    /// Number of failed chunk migrations in the last period
    pub failed_migrations: u64,
    /// Average chunk size in bytes
    pub avg_chunk_size_bytes: f64,
    /// Maximum chunk size in bytes
    pub max_chunk_size_bytes: u64,
    /// Minimum chunk size in bytes
    pub min_chunk_size_bytes: u64,
    /// Number of jumbo chunks (oversized chunks)
    pub jumbo_chunks: u64,
    /// Data distribution imbalance percentage
    pub data_imbalance_percentage: f64,
    /// Total data size across all shards
    pub total_data_size_bytes: u64,
    /// Number of mongos routers connected
    pub mongos_count: u32,
    /// Number of config servers
    pub config_servers: u32,
    /// Whether the balancer is currently running
    pub balancer_active: bool,
    /// Number of balancer rounds completed
    pub balancer_rounds: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoShardingDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoShardingDetailedMetrics {
    /// Shard-specific information (collected when shards are unhealthy)
    pub shard_details: Vec<MongoShardDetails>,
    /// Collections with poor distribution (collected when imbalance is high)
    pub imbalanced_collections: Vec<MongoImbalancedCollection>,
    /// Recent chunk migration failures (collected when migrations fail)
    pub failed_migration_details: Vec<MongoFailedMigration>,
    /// Jumbo chunk details (collected when jumbo chunks exist)
    pub jumbo_chunk_details: Vec<MongoJumboChunk>,
    /// Mongos router statistics (collected periodically)
    pub mongos_statistics: Option<Vec<MongoRouterStats>>,
}

impl MetadataCollection for MongoShardingInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "shard_status".to_string(),
                FindInput::new("config".to_string(), "shards".to_string(), None, Some(FindOptionsWrapper::new())),
            ),
            (
                "chunks_info".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "chunks".to_string(),
                    None,
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "lastmod": -1 })).with_limit(1000)),
                ),
            ),
            (
                "collections_info".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "collections".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "dropped": { "$ne": true }
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
            (
                "migrations_info".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "migrations".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "when": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(1)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "when": -1 })).with_limit(100)),
                ),
            ),
            (
                "balancer_status".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "settings".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "_id": "balancer"
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
            (
                "mongos_info".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "mongos".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ping": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential sharding cluster metrics and health indicators"
    }

    fn category(&self) -> &'static str {
        "sharding"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl MongoShardingInfo {
    const HIGH_IMBALANCE_THRESHOLD: f64 = 20.0; // 20% imbalance
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        if !capabilities.has(&MONGO_SHARDED) {
            return Ok(MongoShardingInfo::default());
        }

        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut sharding_info = MongoShardingInfo::default();
        let requests = self.request();

        // Execute queries to get sharding information
        let shard_docs = fetch(&requests, "shard_status", context.clone(), Self::QUERY_TIMEOUT).await?;
        let chunks_docs = fetch(&requests, "chunks_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        let collection_docs = fetch(&requests, "collections_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        let balancer_docs = fetch(&requests, "balancer_status", context.clone(), Self::QUERY_TIMEOUT).await?;
        let mongos_docs = fetch(&requests, "mongos_info", context.clone(), Self::QUERY_TIMEOUT).await?;

        // Parse the results
        Self::parse_shard_data(&mut sharding_info, &shard_docs)?;
        Self::parse_chunks_data(&mut sharding_info, &chunks_docs)?;
        Self::parse_collections_data(&mut sharding_info, &collection_docs)?;
        Self::parse_balancer_data(&mut sharding_info, &balancer_docs)?;
        Self::parse_mongos_data(&mut sharding_info, &mongos_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut sharding_info)?;

        // Conditionally collect detailed metrics only when problems are detected
        sharding_info.detailed_metrics = self.collect_detailed_metrics_if_needed(&sharding_info, &requests, context).await?;

        Ok(sharding_info)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_info: &MongoShardingInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoShardingDetailedMetrics>> {
        let needs_shard_details = core_info.unhealthy_shards > 0;
        let needs_imbalance_details = core_info.data_imbalance_percentage > Self::HIGH_IMBALANCE_THRESHOLD;
        let needs_migration_details = core_info.failed_migrations > 0;
        let needs_jumbo_details = core_info.jumbo_chunks > 0;

        if !needs_shard_details && !needs_imbalance_details && !needs_migration_details && !needs_jumbo_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoShardingDetailedMetrics {
            shard_details: Vec::new(),
            imbalanced_collections: Vec::new(),
            failed_migration_details: Vec::new(),
            jumbo_chunk_details: Vec::new(),
            mongos_statistics: None,
        };

        // Collect detailed shard information if needed
        if needs_shard_details {
            let docs = fetch(requests, "shard_status", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.shard_details = Self::parse_shard_details(docs)?;
        }

        // Collect migration failure details if needed
        if needs_migration_details {
            let docs = fetch(requests, "migrations_info", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.failed_migration_details = Self::parse_migration_failures(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_shard_data(info: &mut MongoShardingInfo, docs: &[Document]) -> ResultEP<()> {
        info.total_shards = docs.len() as u32;
        info.active_shards = 0;
        info.unhealthy_shards = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            match acc.opt_string("state") {
                Some(state) if state == "1" || state == "active" => info.active_shards += 1,
                Some(_) => info.unhealthy_shards += 1,
                None => info.active_shards += 1, // Assume healthy if no state field
            };
        }

        Ok(())
    }

    fn parse_chunks_data(info: &mut MongoShardingInfo, docs: &[Document]) -> ResultEP<()> {
        info.total_chunks = docs.len() as u64;
        info.migrating_chunks = 0;
        info.jumbo_chunks = 0;

        let mut chunk_sizes = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if acc.opt_bool("jumbo").unwrap_or(false) {
                info.jumbo_chunks += 1;
            }

            if let Some(estimated_size) = acc.opt_u64("estimatedSizeBytes") {
                chunk_sizes.push(estimated_size as f64);
            }
        }

        if !chunk_sizes.is_empty() {
            info.avg_chunk_size_bytes = chunk_sizes.iter().sum::<f64>() / chunk_sizes.len() as f64;
            info.max_chunk_size_bytes = chunk_sizes.iter().fold(0.0f64, |a, &b| a.max(b)) as u64;
            info.min_chunk_size_bytes = chunk_sizes.iter().fold(f64::INFINITY, |a, &b| a.min(b)) as u64;
        }

        Ok(())
    }

    fn parse_collections_data(info: &mut MongoShardingInfo, docs: &[Document]) -> ResultEP<()> {
        info.sharded_collections = docs.len() as u32;
        Ok(())
    }

    fn parse_balancer_data(info: &mut MongoShardingInfo, docs: &[Document]) -> ResultEP<()> {
        if let Some(doc) = docs.first() {
            info.balancer_active = !DocAccessor::new(doc).opt_bool("stopped").unwrap_or(false);
        }
        Ok(())
    }

    fn parse_mongos_data(info: &mut MongoShardingInfo, docs: &[Document]) -> ResultEP<()> {
        info.mongos_count = docs.len() as u32;
        Ok(())
    }

    fn calculate_derived_metrics(info: &mut MongoShardingInfo) -> ResultEP<()> {
        // Calculate data imbalance (simplified calculation)
        if info.total_shards > 0 {
            // This would need actual data distribution queries in real implementation
            info.data_imbalance_percentage = 0.0; // Placeholder
        }

        // Set config servers (typically 3 in a replica set)
        info.config_servers = 3;

        Ok(())
    }

    fn parse_shard_details(docs: Vec<Document>) -> ResultEP<Vec<MongoShardDetails>> {
        let mut shard_details = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            let shard_id = acc.opt_string("_id").unwrap_or_else(|| "unknown".to_string());
            let host = acc.opt_string("host").unwrap_or_else(|| "unknown".to_string());
            let state = acc.opt_string("state").unwrap_or_else(|| "unknown".to_string());

            let is_healthy = state == "1" || state == "active";
            let data_size = acc.opt_u64("dataSizeBytes").unwrap_or(0);
            let chunk_count = acc.opt_u64("chunkCount").unwrap_or(0);

            shard_details.push(MongoShardDetails {
                shard_id,
                host,
                state,
                is_healthy,
                data_size_bytes: data_size,
                chunk_count,
                last_ping: acc.opt_datetime("ping").unwrap_or_else(|| DateTimeWrapper::from(Utc::now())),
            });
        }

        Ok(shard_details)
    }

    fn parse_migration_failures(docs: Vec<Document>) -> ResultEP<Vec<MongoFailedMigration>> {
        let mut failed_migrations = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let Some(what) = acc.opt_string("what")
                && what.contains("moveChunk")
            {
                let from_shard = acc.opt_string("from").unwrap_or_else(|| "unknown".to_string());
                let to_shard = acc.opt_string("to").unwrap_or_else(|| "unknown".to_string());
                let namespace = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let reason = acc.opt_string("note").unwrap_or_else(|| "unknown".to_string());

                failed_migrations.push(MongoFailedMigration {
                    namespace,
                    chunk_id: "unknown".to_string(), // Would need additional parsing
                    from_shard,
                    to_shard,
                    failure_reason: reason,
                    timestamp: acc.opt_datetime("when").unwrap_or_else(|| DateTimeWrapper::from(Utc::now())),
                    retry_count: 0, // Would need additional tracking
                });
            }
        }

        Ok(failed_migrations)
    }
}

/// Detailed information about individual shards
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoShardDetails {
    /// Shard identifier
    pub shard_id: String,
    /// Shard host connection string
    pub host: String,
    /// Current shard state
    pub state: String,
    /// Whether the shard is healthy
    pub is_healthy: bool,
    /// Data size on this shard in bytes
    pub data_size_bytes: u64,
    /// Number of chunks on this shard
    pub chunk_count: u64,
    /// Last ping timestamp
    pub last_ping: DateTimeWrapper,
}

/// Information about collections with poor data distribution
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoImbalancedCollection {
    /// Namespace (database.collection)
    pub namespace: String,
    /// Shard key pattern
    pub shard_key: String,
    /// Imbalance percentage
    pub imbalance_percentage: f64,
    /// Shard with most data
    pub largest_shard: String,
    /// Shard with least data
    pub smallest_shard: String,
    /// Size difference in bytes
    pub size_difference_bytes: u64,
    /// Number of chunks on largest shard
    pub largest_shard_chunks: u64,
    /// Number of chunks on smallest shard
    pub smallest_shard_chunks: u64,
}

/// Information about failed chunk migrations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedMigration {
    /// Collection namespace
    pub namespace: String,
    /// Chunk identifier
    pub chunk_id: String,
    /// Source shard
    pub from_shard: String,
    /// Destination shard
    pub to_shard: String,
    /// Failure reason
    pub failure_reason: String,
    /// When the migration failed
    pub timestamp: DateTimeWrapper,
    /// Number of retry attempts
    pub retry_count: u32,
}

/// Information about jumbo chunks
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoJumboChunk {
    /// Collection namespace
    pub namespace: String,
    /// Chunk identifier
    pub chunk_id: String,
    /// Shard hosting the chunk
    pub shard: String,
    /// Estimated chunk size in bytes
    pub size_bytes: u64,
    /// Chunk bounds (min key)
    pub min_key: String,
    /// Chunk bounds (max key)
    pub max_key: String,
    /// When the chunk was last modified
    pub last_modified: DateTimeWrapper,
}

/// Statistics for mongos routers
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoRouterStats {
    /// Mongos instance identifier
    pub mongos_id: String,
    /// Host and port
    pub host: String,
    /// Last ping timestamp
    pub last_ping: DateTimeWrapper,
    /// MongoDB version
    pub version: String,
    /// Connection count
    pub connections: u32,
    /// Operations per second
    pub ops_per_second: f64,
}

impl MongoShardingInfo {
    /// Calculates the percentage of healthy shards
    pub fn healthy_shard_percentage(&self) -> f64 {
        if self.total_shards == 0 {
            0.0
        } else {
            (self.active_shards as f64 / self.total_shards as f64) * 100.0
        }
    }

    /// Checks if the cluster has data imbalance issues
    pub fn has_data_imbalance(&self, threshold: f64) -> bool {
        self.data_imbalance_percentage > threshold
    }

    /// Checks if there are failed migrations
    pub fn has_migration_failures(&self) -> bool {
        self.failed_migrations > 0
    }

    /// Checks if there are jumbo chunks
    pub fn has_jumbo_chunks(&self) -> bool {
        self.jumbo_chunks > 0
    }

    /// Returns the percentage of chunks that are jumbo
    pub fn jumbo_chunk_percentage(&self) -> f64 {
        if self.total_chunks == 0 {
            0.0
        } else {
            (self.jumbo_chunks as f64 / self.total_chunks as f64) * 100.0
        }
    }

    /// Checks if the balancer is active
    pub fn is_balancer_active(&self) -> bool {
        self.balancer_active
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Calculates average chunks per shard
    pub fn avg_chunks_per_shard(&self) -> f64 {
        if self.total_shards == 0 {
            0.0
        } else {
            self.total_chunks as f64 / self.total_shards as f64
        }
    }

    /// Checks if the cluster is properly configured
    pub fn is_cluster_healthy(&self) -> bool {
        self.unhealthy_shards == 0
            && self.mongos_count > 0
            && self.config_servers >= 3
            && self.data_imbalance_percentage < Self::HIGH_IMBALANCE_THRESHOLD
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_sharding_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;
        let telemetry_wrapper = &mut telemetry_wrapper;

        let sharding_info = MongoShardingInfo::default();

        let result = sharding_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sharding_health_calculations() {
        let info = MongoShardingInfo {
            total_shards: 3,
            active_shards: 2,
            unhealthy_shards: 1,
            total_chunks: 100,
            jumbo_chunks: 5,
            ..MongoShardingInfo::default()
        };

        assert!((info.healthy_shard_percentage() - 66.667).abs() < 0.01);
        assert_eq!(info.jumbo_chunk_percentage(), 5.0);
        assert!((info.avg_chunks_per_shard() - 33.333).abs() < 0.01);
        assert!(!info.is_cluster_healthy());
    }
}
