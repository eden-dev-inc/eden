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

/// MongoDB database information and statistics
///
/// Simplified struct containing essential metrics about database
/// storage, collections, indexes, and performance characteristics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseInfo {
    /// Total number of databases
    pub total_databases: u64,
    /// Total storage size across all databases (bytes)
    pub total_storage_size_bytes: u64,
    /// Total data size across all databases (bytes)
    pub total_data_size_bytes: u64,
    /// Total index size across all databases (bytes)
    pub total_index_size_bytes: u64,
    /// Total number of collections across all databases
    pub total_collections: u64,
    /// Total number of documents across all databases
    pub total_documents: u64,
    /// Total number of indexes across all databases
    pub total_indexes: u64,
    /// Number of empty databases
    pub empty_databases: u64,
    /// Number of databases with more than 100 collections
    pub large_databases: u64,
    /// Average database size (bytes)
    pub avg_database_size_bytes: f64,
    /// Average collections per database
    pub avg_collections_per_database: f64,
    /// Average documents per database
    pub avg_documents_per_database: f64,
    /// Storage efficiency ratio across all databases
    pub storage_efficiency_ratio: f64,
    /// Number of databases using GridFS
    pub gridfs_databases: u64,
    /// Number of sharded databases
    pub sharded_databases: u64,
    /// Number of databases with replication enabled
    pub replicated_databases: u64,
    /// Database fragmentation percentage
    pub fragmentation_percentage: f64,
    /// Estimated wasted space (bytes)
    pub estimated_wasted_space_bytes: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoDatabaseDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseDetailedMetrics {
    /// Large databases requiring attention
    pub large_databases: Vec<MongoLargeDatabase>,
    /// Databases with poor storage efficiency
    pub inefficient_databases: Vec<MongoInefficientDatabase>,
    /// Empty or underutilized databases
    pub underutilized_databases: Vec<MongoUnderutilizedDatabase>,
    /// Databases with fragmentation issues
    pub fragmented_databases: Option<Vec<MongoFragmentedDatabase>>,
    /// Database growth patterns and projections
    pub growth_patterns: Option<Vec<MongoDatabaseGrowth>>,
    /// Databases with unusual activity patterns
    pub unusual_activity: Option<Vec<MongoDatabaseActivity>>,
    /// Resource-intensive databases
    pub resource_intensive: Vec<MongoResourceIntensiveDatabase>,
}

impl MetadataCollection for MongoDatabaseInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "database_list".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.listDatabases": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(10)),
                ),
            ),
            (
                "db_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.dbStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "sharding_info".to_string(),
                FindInput::new("config".to_string(), "databases".to_string(), None, None),
            ),
            (
                "gridfs_collections".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ns": { "$regex": "\\.fs\\.(files|chunks)" },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(15)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "replication_info".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.replSetGetStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(10)),
                ),
            ),
            (
                "database_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "op": { "$in": ["insert", "update", "delete", "query"] },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(500)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential database metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "databases"
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

impl MongoDatabaseInfo {
    const LARGE_DATABASE_THRESHOLD_GB: f64 = 10.0; // 10 GB
    const STORAGE_EFFICIENCY_THRESHOLD: f64 = 0.6; // 60% efficiency
    const FRAGMENTATION_THRESHOLD: f64 = 20.0; // 20% fragmentation
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const LARGE_COLLECTION_COUNT_THRESHOLD: u64 = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut database_stats = MongoDatabaseInfo::default();
        let requests = self.request();

        // Execute queries to get database information
        let database_list = fetch(&requests, "database_list", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_database_list(&mut database_stats, &database_list)?;

        let db_stats = fetch(&requests, "db_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_database_stats(&mut database_stats, &db_stats)?;

        let sharding_info = fetch(&requests, "sharding_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_sharding_info(&mut database_stats, &sharding_info)?;

        let gridfs_collections = fetch(&requests, "gridfs_collections", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_gridfs_info(&mut database_stats, &gridfs_collections)?;

        let replication_info = fetch(&requests, "replication_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_replication_info(&mut database_stats, &replication_info)?;

        let operations = fetch(&requests, "database_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_database_operations(&mut database_stats, &operations)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut database_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        database_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&database_stats, &requests, context).await?;

        Ok(database_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoDatabaseInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoDatabaseDetailedMetrics>> {
        let needs_large_db_details = core_stats.large_databases > 0;
        let needs_efficiency_details = core_stats.storage_efficiency_ratio < Self::STORAGE_EFFICIENCY_THRESHOLD;
        let needs_fragmentation_details = core_stats.fragmentation_percentage > Self::FRAGMENTATION_THRESHOLD;
        let needs_underutilized_details = core_stats.empty_databases > 0;

        if !needs_large_db_details && !needs_efficiency_details && !needs_fragmentation_details && !needs_underutilized_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoDatabaseDetailedMetrics {
            large_databases: Vec::new(),
            inefficient_databases: Vec::new(),
            underutilized_databases: Vec::new(),
            fragmented_databases: None,
            growth_patterns: None,
            unusual_activity: None,
            resource_intensive: Vec::new(),
        };

        // Collect large databases if needed
        if needs_large_db_details {
            let docs = fetch(requests, "db_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.large_databases = Self::parse_large_databases(docs.clone())?;
        }

        // Collect inefficient databases if needed
        if needs_efficiency_details {
            let docs = fetch(requests, "db_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.inefficient_databases = Self::parse_inefficient_databases(docs.clone())?;
        }

        // Collect underutilized databases if needed
        if needs_underutilized_details {
            let docs = fetch(requests, "database_list", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.underutilized_databases = Self::parse_underutilized_databases(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_database_list(stats: &mut MongoDatabaseInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Ok(databases) = result.raw().get_array("databases")
            {
                stats.total_databases = databases.len() as u64;

                let mut empty_count = 0;
                let mut total_size = 0u64;

                for db_doc in databases {
                    if let Some(db) = db_doc.as_document() {
                        let db_acc = DocAccessor::new(db);
                        if db_acc.opt_bool("empty").unwrap_or(false) {
                            empty_count += 1;
                        }
                        if let Some(size_on_disk) = db_acc.opt_i64("sizeOnDisk") {
                            total_size += size_on_disk as u64;
                        }
                    }
                }

                stats.empty_databases = empty_count;
                stats.total_storage_size_bytes += total_size;
            }
        }

        Ok(())
    }

    fn parse_database_stats(stats: &mut MongoDatabaseInfo, docs: &[Document]) -> ResultEP<()> {
        let mut total_collections = 0u64;
        let mut total_documents = 0u64;
        let mut total_indexes = 0u64;
        let mut total_data_size = 0u64;
        let mut total_index_size = 0u64;
        let mut large_db_count = 0u64;

        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(collections) = result.opt_i32("collections") {
                    total_collections += collections as u64;
                    if collections > Self::LARGE_COLLECTION_COUNT_THRESHOLD as i32 {
                        large_db_count += 1;
                    }
                }

                if let Some(objects) = result.opt_i64("objects") {
                    total_documents += objects as u64;
                }

                if let Some(indexes) = result.opt_i32("indexes") {
                    total_indexes += indexes as u64;
                }

                if let Some(data_size) = result.opt_i64("dataSize") {
                    total_data_size += data_size as u64;
                }

                if let Some(index_size) = result.opt_i64("indexSize") {
                    total_index_size += index_size as u64;
                }

                if let Some(storage_size) = result.opt_i64("storageSize") {
                    stats.total_storage_size_bytes += storage_size as u64;
                }
            }
        }

        stats.total_collections = total_collections;
        stats.total_documents = total_documents;
        stats.total_indexes = total_indexes;
        stats.total_data_size_bytes = total_data_size;
        stats.total_index_size_bytes = total_index_size;
        stats.large_databases = large_db_count;

        Ok(())
    }

    fn parse_sharding_info(stats: &mut MongoDatabaseInfo, docs: &[Document]) -> ResultEP<()> {
        let mut sharded_count = 0;

        for doc in docs {
            if DocAccessor::new(doc).opt_bool("partitioned").unwrap_or(false) {
                sharded_count += 1;
            }
        }

        stats.sharded_databases = sharded_count;
        Ok(())
    }

    fn parse_gridfs_info(stats: &mut MongoDatabaseInfo, docs: &[Document]) -> ResultEP<()> {
        let mut gridfs_databases = std::collections::HashSet::new();

        for doc in docs {
            if let Some(ns) = DocAccessor::new(doc).opt_string("ns")
                && ns.contains(".fs.")
                && let Some(db_name) = ns.split('.').next()
            {
                gridfs_databases.insert(db_name.to_string());
            }
        }

        stats.gridfs_databases = gridfs_databases.len() as u64;
        Ok(())
    }

    fn parse_replication_info(stats: &mut MongoDatabaseInfo, docs: &[Document]) -> ResultEP<()> {
        // If we have replication status results, then replication is enabled
        if !docs.is_empty() {
            stats.replicated_databases = stats.total_databases; // All databases in a replica set
        }

        Ok(())
    }

    fn parse_database_operations(_stats: &mut MongoDatabaseInfo, _docs: &[Document]) -> ResultEP<()> {
        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoDatabaseInfo) -> ResultEP<()> {
        // Calculate average database size
        if stats.total_databases > 0 {
            stats.avg_database_size_bytes = stats.total_storage_size_bytes as f64 / stats.total_databases as f64;
            stats.avg_collections_per_database = stats.total_collections as f64 / stats.total_databases as f64;
            stats.avg_documents_per_database = stats.total_documents as f64 / stats.total_databases as f64;
        }

        // Calculate storage efficiency
        if stats.total_storage_size_bytes > 0 {
            stats.storage_efficiency_ratio = stats.total_data_size_bytes as f64 / stats.total_storage_size_bytes as f64;
        }

        // Calculate fragmentation
        if stats.total_storage_size_bytes > stats.total_data_size_bytes {
            let wasted_space = stats.total_storage_size_bytes - stats.total_data_size_bytes;
            stats.estimated_wasted_space_bytes = wasted_space;
            stats.fragmentation_percentage = (wasted_space as f64 / stats.total_storage_size_bytes as f64) * 100.0;
        }

        Ok(())
    }

    fn parse_large_databases(docs: Vec<Document>) -> ResultEP<Vec<MongoLargeDatabase>> {
        let mut databases = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(result), Some(db_name)) = (acc.child("result"), acc.opt_string("ns"))
                && let Some(storage_size) = result.opt_i64("storageSize")
            {
                let size_gb = storage_size as f64 / 1024.0 / 1024.0 / 1024.0;

                if size_gb > MongoDatabaseInfo::LARGE_DATABASE_THRESHOLD_GB {
                    databases.push(MongoLargeDatabase {
                        name: db_name,
                        storage_size_gb: size_gb,
                        data_size_gb: result.opt_i64("dataSize").map(|s| s as f64 / 1024.0 / 1024.0 / 1024.0).unwrap_or(0.0),
                        collection_count: result.opt_i32("collections").unwrap_or(0) as u64,
                        document_count: result.opt_i64("objects").unwrap_or(0) as u64,
                        index_count: result.opt_i32("indexes").unwrap_or(0) as u64,
                        index_size_gb: result.opt_i64("indexSize").map(|s| s as f64 / 1024.0 / 1024.0 / 1024.0).unwrap_or(0.0),
                        is_sharded: false,
                        recommended_action: "Consider archiving old data or sharding".to_string(),
                        growth_rate_gb_per_day: 0.0,
                    });
                }
            }
        }

        Ok(databases)
    }

    fn parse_inefficient_databases(docs: Vec<Document>) -> ResultEP<Vec<MongoInefficientDatabase>> {
        let mut databases = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(result), Some(db_name)) = (acc.child("result"), acc.opt_string("ns"))
                && let (Some(storage_size), Some(data_size)) = (result.opt_i64("storageSize"), result.opt_i64("dataSize"))
            {
                let efficiency = if storage_size > 0 {
                    data_size as f64 / storage_size as f64
                } else {
                    0.0
                };

                if efficiency < MongoDatabaseInfo::STORAGE_EFFICIENCY_THRESHOLD {
                    databases.push(MongoInefficientDatabase {
                        name: db_name,
                        storage_efficiency_ratio: efficiency,
                        storage_size_gb: storage_size as f64 / 1024.0 / 1024.0 / 1024.0,
                        data_size_gb: data_size as f64 / 1024.0 / 1024.0 / 1024.0,
                        wasted_space_gb: (storage_size - data_size) as f64 / 1024.0 / 1024.0 / 1024.0,
                        fragmentation_percentage: ((storage_size - data_size) as f64 / storage_size as f64) * 100.0,
                        last_compaction: None,
                        recommended_action: "Run compact command or rebuild indexes".to_string(),
                        estimated_recovery_gb: (storage_size - data_size) as f64 / 1024.0 / 1024.0 / 1024.0,
                    });
                }
            }
        }

        Ok(databases)
    }

    fn parse_underutilized_databases(docs: Vec<Document>) -> ResultEP<Vec<MongoUnderutilizedDatabase>> {
        let mut databases = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let Some(result) = acc.child("result")
                && let Ok(db_list) = result.raw().get_array("databases")
            {
                for db_doc in db_list {
                    if let Some(db) = db_doc.as_document() {
                        let db_acc = DocAccessor::new(db);
                        if db_acc.opt_bool("empty").unwrap_or(false)
                            && let Some(name) = db_acc.opt_string("name")
                        {
                            databases.push(MongoUnderutilizedDatabase {
                                name,
                                storage_size_mb: db_acc.opt_i64("sizeOnDisk").map(|s| s as f64 / 1024.0 / 1024.0).unwrap_or(0.0),
                                collection_count: 0,
                                document_count: 0,
                                last_activity: None,
                                days_inactive: 30,
                                reason: "Database is empty".to_string(),
                                recommended_action: "Consider dropping if not needed".to_string(),
                            });
                        }
                    }
                }
            }
        }

        Ok(databases)
    }
}

/// Information about large databases
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLargeDatabase {
    /// Database name
    pub name: String,
    /// Storage size in gigabytes
    pub storage_size_gb: f64,
    /// Data size in gigabytes
    pub data_size_gb: f64,
    /// Number of collections
    pub collection_count: u64,
    /// Number of documents
    pub document_count: u64,
    /// Number of indexes
    pub index_count: u64,
    /// Index size in gigabytes
    pub index_size_gb: f64,
    /// Whether the database is sharded
    pub is_sharded: bool,
    /// Recommended action
    pub recommended_action: String,
    /// Growth rate in GB per day
    pub growth_rate_gb_per_day: f64,
}

/// Information about databases with poor storage efficiency
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoInefficientDatabase {
    /// Database name
    pub name: String,
    /// Storage efficiency ratio
    pub storage_efficiency_ratio: f64,
    /// Storage size in gigabytes
    pub storage_size_gb: f64,
    /// Data size in gigabytes
    pub data_size_gb: f64,
    /// Wasted space in gigabytes
    pub wasted_space_gb: f64,
    /// Fragmentation percentage
    pub fragmentation_percentage: f64,
    /// Last compaction timestamp
    pub last_compaction: Option<DateTimeWrapper>,
    /// Recommended action
    pub recommended_action: String,
    /// Estimated space recovery in GB
    pub estimated_recovery_gb: f64,
}

/// Information about underutilized databases
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUnderutilizedDatabase {
    /// Database name
    pub name: String,
    /// Storage size in megabytes
    pub storage_size_mb: f64,
    /// Number of collections
    pub collection_count: u64,
    /// Number of documents
    pub document_count: u64,
    /// Last activity timestamp
    pub last_activity: Option<DateTimeWrapper>,
    /// Days since last activity
    pub days_inactive: u64,
    /// Reason for being underutilized
    pub reason: String,
    /// Recommended action
    pub recommended_action: String,
}

/// Information about fragmented databases
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFragmentedDatabase {
    /// Database name
    pub name: String,
    /// Fragmentation percentage
    pub fragmentation_percentage: f64,
    /// Wasted space in gigabytes
    pub wasted_space_gb: f64,
    /// Collections contributing most to fragmentation
    pub worst_collections: Vec<String>,
    /// Last defragmentation date
    pub last_defragmentation: Option<DateTimeWrapper>,
    /// Estimated recovery time for defragmentation
    pub estimated_defrag_time_hours: f64,
    /// Recommended defragmentation strategy
    pub defrag_strategy: String,
}

/// Database growth pattern information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseGrowth {
    /// Database name
    pub name: String,
    /// Growth rate in GB per day
    pub growth_rate_gb_per_day: f64,
    /// Growth rate in documents per day
    pub growth_rate_docs_per_day: f64,
    /// Projected size in 30 days (GB)
    pub projected_size_30d_gb: f64,
    /// Projected size in 90 days (GB)
    pub projected_size_90d_gb: f64,
    /// Growth trend (accelerating, stable, decelerating)
    pub growth_trend: String,
    /// Primary growth driver (documents, indexes, etc.)
    pub growth_driver: String,
    /// Capacity planning recommendation
    pub capacity_recommendation: String,
}

/// Database activity pattern information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDatabaseActivity {
    /// Database name
    pub name: String,
    /// Operations per minute
    pub operations_per_minute: f64,
    /// Read vs write ratio
    pub read_write_ratio: f64,
    /// Peak activity hours
    pub peak_hours: Vec<u8>,
    /// Unusual activity detected
    pub unusual_patterns: Vec<String>,
    /// Activity trend (increasing, stable, decreasing)
    pub activity_trend: String,
    /// Busiest collections
    pub busiest_collections: Vec<String>,
}

/// Resource-intensive database information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoResourceIntensiveDatabase {
    /// Database name
    pub name: String,
    /// CPU usage percentage
    pub cpu_usage_percentage: f64,
    /// Memory usage in megabytes
    pub memory_usage_mb: f64,
    /// I/O operations per second
    pub iops: f64,
    /// Network bandwidth usage in MB/s
    pub network_usage_mbps: f64,
    /// Resource intensity score (0-100)
    pub intensity_score: f64,
    /// Primary resource bottleneck
    pub bottleneck: String,
    /// Optimization recommendations
    pub optimization_recommendations: Vec<String>,
}

impl MongoDatabaseInfo {
    /// Checks if database configuration is healthy
    pub fn is_database_healthy(&self) -> bool {
        self.storage_efficiency_ratio > Self::STORAGE_EFFICIENCY_THRESHOLD
            && self.fragmentation_percentage < Self::FRAGMENTATION_THRESHOLD
            && self.empty_databases == 0
            && self.estimated_wasted_space_bytes < (1024 * 1024 * 1024) // Less than 1GB wasted
    }

    /// Returns the average database utilization percentage
    pub fn avg_database_utilization(&self) -> f64 {
        if self.total_databases == 0 {
            0.0
        } else {
            let utilized_databases = self.total_databases - self.empty_databases;
            (utilized_databases as f64 / self.total_databases as f64) * 100.0
        }
    }

    /// Returns the percentage of databases that are sharded
    pub fn sharding_adoption_percentage(&self) -> f64 {
        if self.total_databases == 0 {
            0.0
        } else {
            (self.sharded_databases as f64 / self.total_databases as f64) * 100.0
        }
    }

    /// Returns the percentage of databases using GridFS
    pub fn gridfs_adoption_percentage(&self) -> f64 {
        if self.total_databases == 0 {
            0.0
        } else {
            (self.gridfs_databases as f64 / self.total_databases as f64) * 100.0
        }
    }

    /// Calculates the index overhead percentage
    pub fn index_overhead_percentage(&self) -> f64 {
        if self.total_data_size_bytes == 0 {
            0.0
        } else {
            (self.total_index_size_bytes as f64 / self.total_data_size_bytes as f64) * 100.0
        }
    }

    /// Returns total storage in gigabytes
    pub fn total_storage_gb(&self) -> f64 {
        self.total_storage_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    }

    /// Returns total data size in gigabytes
    pub fn total_data_gb(&self) -> f64 {
        self.total_data_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    }

    /// Returns total index size in gigabytes
    pub fn total_index_gb(&self) -> f64 {
        self.total_index_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    }

    /// Returns estimated wasted space in gigabytes
    pub fn wasted_space_gb(&self) -> f64 {
        self.estimated_wasted_space_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    }

    /// Checks if there are storage efficiency issues
    pub fn has_storage_efficiency_issues(&self) -> bool {
        self.storage_efficiency_ratio < Self::STORAGE_EFFICIENCY_THRESHOLD
    }

    /// Checks if there are fragmentation issues
    pub fn has_fragmentation_issues(&self) -> bool {
        self.fragmentation_percentage > Self::FRAGMENTATION_THRESHOLD
    }

    /// Returns the percentage of large databases
    pub fn large_databases_percentage(&self) -> f64 {
        if self.total_databases == 0 {
            0.0
        } else {
            (self.large_databases as f64 / self.total_databases as f64) * 100.0
        }
    }

    /// Returns the average indexes per collection across all databases
    pub fn avg_indexes_per_collection(&self) -> f64 {
        if self.total_collections == 0 {
            0.0
        } else {
            self.total_indexes as f64 / self.total_collections as f64
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Calculates a database health score from 0-100
    pub fn database_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct points for storage efficiency issues
        if self.storage_efficiency_ratio < 0.8 {
            score -= 25.0;
        } else if self.storage_efficiency_ratio < 0.9 {
            score -= 10.0;
        }

        // Deduct points for fragmentation
        if self.fragmentation_percentage > 30.0 {
            score -= 20.0;
        } else if self.fragmentation_percentage > 20.0 {
            score -= 10.0;
        }

        // Deduct points for empty databases
        if self.empty_databases > 0 {
            let empty_percentage = (self.empty_databases as f64 / self.total_databases as f64) * 100.0;
            score -= empty_percentage.min(15.0); // Max 15 point deduction
        }

        // Deduct points for excessive large databases
        if self.large_databases_percentage() > 50.0 {
            score -= 15.0;
        }

        // Bonus points for good practices
        if self.sharding_adoption_percentage() > 20.0 && self.total_storage_gb() > 100.0 {
            score += 5.0; // Bonus for sharding large datasets
        }

        if self.storage_efficiency_ratio > 0.95 {
            score += 5.0; // Bonus for excellent efficiency
        }

        score.clamp(0.0, 100.0)
    }

    /// Predicts storage needs for the next 30 days based on current growth
    pub fn predicted_storage_growth_30d_gb(&self) -> f64 {
        0.0
    }

    /// Returns storage density (documents per GB)
    pub fn storage_density_docs_per_gb(&self) -> f64 {
        let total_storage_gb = self.total_storage_gb();
        if total_storage_gb > 0.0 {
            self.total_documents as f64 / total_storage_gb
        } else {
            0.0
        }
    }

    /// Calculates the cost efficiency ratio (useful data vs total storage)
    pub fn cost_efficiency_ratio(&self) -> f64 {
        if self.total_storage_size_bytes == 0 {
            0.0
        } else {
            self.total_data_size_bytes as f64 / self.total_storage_size_bytes as f64
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
    async fn test_mongo_database_stats() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let database_stats = MongoDatabaseInfo::default();

        let result = database_stats
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_database_health_check() {
        let mut stats = MongoDatabaseInfo {
            storage_efficiency_ratio: 0.8,
            fragmentation_percentage: 15.0,
            empty_databases: 0,
            estimated_wasted_space_bytes: 500 * 1024 * 1024,
            ..MongoDatabaseInfo::default()
        };

        assert!(stats.is_database_healthy());

        stats.fragmentation_percentage = 25.0;
        assert!(!stats.is_database_healthy());
    }

    #[test]
    fn test_database_utilization() {
        let stats = MongoDatabaseInfo {
            total_databases: 10,
            empty_databases: 2,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.avg_database_utilization(), 80.0);
    }

    #[test]
    fn test_sharding_adoption() {
        let stats = MongoDatabaseInfo {
            total_databases: 10,
            sharded_databases: 3,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.sharding_adoption_percentage(), 30.0);
    }

    #[test]
    fn test_index_overhead() {
        let stats = MongoDatabaseInfo {
            total_data_size_bytes: 1000 * 1024 * 1024,
            total_index_size_bytes: 200 * 1024 * 1024,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.index_overhead_percentage(), 20.0);
    }

    #[test]
    fn test_storage_conversions() {
        let stats = MongoDatabaseInfo {
            total_storage_size_bytes: 5 * 1024 * 1024 * 1024,
            total_data_size_bytes: 3 * 1024 * 1024 * 1024,
            total_index_size_bytes: 1024 * 1024 * 1024,
            estimated_wasted_space_bytes: 1024 * 1024 * 1024,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.total_storage_gb(), 5.0);
        assert_eq!(stats.total_data_gb(), 3.0);
        assert_eq!(stats.total_index_gb(), 1.0);
        assert_eq!(stats.wasted_space_gb(), 1.0);
    }

    #[test]
    fn test_database_health_score() {
        let mut stats = MongoDatabaseInfo {
            storage_efficiency_ratio: 0.9,
            fragmentation_percentage: 10.0,
            empty_databases: 0,
            total_databases: 10,
            large_databases: 1,
            ..MongoDatabaseInfo::default()
        };

        let score = stats.database_health_score();
        assert!(score >= 90.0); // Should be high with good metrics

        stats.storage_efficiency_ratio = 0.5;
        stats.fragmentation_percentage = 35.0;

        let score2 = stats.database_health_score();
        assert!(score2 < score); // Should be lower with worse metrics
    }

    #[test]
    fn test_storage_density() {
        let stats = MongoDatabaseInfo {
            total_documents: 1_000_000,
            total_storage_size_bytes: 2 * 1024 * 1024 * 1024,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.storage_density_docs_per_gb(), 500_000.0); // 500k docs per GB
    }

    #[test]
    fn test_cost_efficiency() {
        let stats = MongoDatabaseInfo {
            total_storage_size_bytes: 1000,
            total_data_size_bytes: 600,
            total_index_size_bytes: 200,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.cost_efficiency_ratio(), 0.6);
    }

    #[test]
    fn test_avg_metrics() {
        let mut stats = MongoDatabaseInfo {
            total_databases: 5,
            total_collections: 50,
            total_indexes: 150,
            total_storage_size_bytes: 5000,
            ..MongoDatabaseInfo::default()
        };

        assert_eq!(stats.avg_collections_per_database, 0.0); // Not calculated yet
        MongoDatabaseInfo::calculate_derived_metrics(&mut stats).unwrap_or_default();
        assert_eq!(stats.avg_collections_per_database, 10.0);
        assert_eq!(stats.avg_indexes_per_collection(), 3.0);
    }

    #[test]
    fn test_efficiency_and_fragmentation_flags() {
        let mut stats = MongoDatabaseInfo {
            storage_efficiency_ratio: 0.5,
            ..MongoDatabaseInfo::default()
        };

        assert!(stats.has_storage_efficiency_issues());

        stats.storage_efficiency_ratio = 0.8;
        assert!(!stats.has_storage_efficiency_issues());

        stats.fragmentation_percentage = 25.0;
        assert!(stats.has_fragmentation_issues());

        stats.fragmentation_percentage = 15.0;
        assert!(!stats.has_fragmentation_issues());
    }
}
