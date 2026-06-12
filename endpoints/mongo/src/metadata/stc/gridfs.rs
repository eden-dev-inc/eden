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

/// MongoDB GridFS statistics and performance metrics
///
/// Simplified struct containing essential metrics about GridFS
/// usage, file storage patterns, and performance characteristics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGridFSInfo {
    /// Total number of GridFS buckets across all databases
    pub total_gridfs_buckets: u64,
    /// Total number of files stored in GridFS
    pub total_files: u64,
    /// Total number of chunks across all files
    pub total_chunks: u64,
    /// Total storage size of all GridFS files (bytes)
    pub total_storage_size_bytes: u64,
    /// Average file size (bytes)
    pub avg_file_size_bytes: f64,
    /// Maximum file size (bytes)
    pub max_file_size_bytes: u64,
    /// Minimum file size (bytes)
    pub min_file_size_bytes: u64,
    /// Average chunk size (bytes)
    pub avg_chunk_size_bytes: f64,
    /// Standard GridFS chunk size (typically 255KB)
    pub standard_chunk_size_bytes: u64,
    /// Number of files using non-standard chunk sizes
    pub non_standard_chunk_files: u64,
    /// Number of orphaned chunks (chunks without corresponding files)
    pub orphaned_chunks: u64,
    /// Number of incomplete files (missing chunks)
    pub incomplete_files: u64,
    /// Total number of file uploads in the last period
    pub recent_uploads: u64,
    /// Total number of file downloads in the last period
    pub recent_downloads: u64,
    /// Total number of file deletions in the last period
    pub recent_deletions: u64,
    /// Average upload throughput (bytes per second)
    pub avg_upload_throughput_bps: f64,
    /// Average download throughput (bytes per second)
    pub avg_download_throughput_bps: f64,
    /// Storage efficiency ratio (actual data vs storage overhead)
    pub storage_efficiency_ratio: f64,
    /// Number of large files (>16MB, MongoDB document limit)
    pub large_files: u64,
    /// Number of small files (<1KB, inefficient for GridFS)
    pub small_files: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoGridFSDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGridFSDetailedMetrics {
    /// Large files that might be better suited for external storage
    pub oversized_files: Vec<MongoOversizedFile>,
    /// Small files that are inefficient for GridFS
    pub inefficient_small_files: Vec<MongoInefficientSmallFile>,
    /// Orphaned chunks that should be cleaned up
    pub orphaned_chunks: Vec<MongoOrphanedChunk>,
    /// Files with integrity issues
    pub integrity_issues: Vec<MongoFileIntegrityIssue>,
    /// Performance bottlenecks in GridFS operations
    pub performance_issues: Option<Vec<MongoGridFSPerformanceIssue>>,
    /// Storage utilization by bucket
    pub bucket_utilization: Option<Vec<MongoGridFSBucketStats>>,
    /// File type distribution and patterns
    pub file_patterns: Option<Vec<MongoGridFSFilePattern>>,
}

/// Information about oversized files that might be better suited for external storage
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOversizedFile {
    pub file_id: String,
    pub filename: String,
    pub size_mb: f64,
    pub chunk_count: u64,
    pub upload_date: Option<DateTimeWrapper>,
    pub content_type: Option<String>,
    pub bucket: String,
    pub recommended_action: String,
    pub storage_cost_impact: f64,
}

/// Information about small files that are inefficient for GridFS
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoInefficientSmallFile {
    pub file_id: String,
    pub filename: String,
    pub size_bytes: u64,
    pub overhead_ratio: f64,
    pub upload_date: Option<DateTimeWrapper>,
    pub content_type: Option<String>,
    pub bucket: String,
    pub recommended_action: String,
    pub efficiency_loss_percentage: f64,
}

/// Information about orphaned chunks that should be cleaned up
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoOrphanedChunk {
    pub chunk_id: String,
    pub file_id: String,
    pub chunk_number: u32,
    pub size_bytes: u64,
    pub bucket: String,
    pub last_accessed: Option<DateTimeWrapper>,
    pub cleanup_priority: String,
    pub storage_waste_bytes: u64,
}

/// Information about files with integrity issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFileIntegrityIssue {
    pub file_id: String,
    pub filename: String,
    pub issue_type: String,
    pub missing_chunks: Vec<u32>,
    pub corrupted_chunks: Vec<u32>,
    pub expected_chunk_count: u32,
    pub actual_chunk_count: u32,
    pub bucket: String,
    pub recommended_action: String,
    pub data_loss_risk: String,
}

/// Performance bottlenecks in GridFS operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGridFSPerformanceIssue {
    pub operation_type: String,
    pub avg_duration_ms: f64,
    pub affected_files: u64,
    pub bottleneck_type: String,
    pub recommended_optimization: String,
    pub performance_impact: String,
}

/// Storage utilization statistics by bucket
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGridFSBucketStats {
    pub bucket_name: String,
    pub total_files: u64,
    pub total_size_bytes: u64,
    pub avg_file_size_bytes: f64,
    pub largest_file_mb: f64,
    pub utilization_percentage: f64,
    pub growth_trend: String,
}

/// File type distribution and patterns
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGridFSFilePattern {
    pub content_type: String,
    pub file_count: u64,
    pub total_size_bytes: u64,
    pub avg_size_bytes: f64,
    pub usage_trend: String,
    pub optimization_suggestion: String,
}

impl MetadataCollection for MongoGridFSInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "gridfs_files".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ns": { "$regex": "\\.fs\\.files$" },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(15)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
            (
                "gridfs_chunks".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ns": { "$regex": "\\.fs\\.chunks$" },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(15)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(500)),
                ),
            ),
            (
                "gridfs_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.insert": { "$regex": "\\.fs\\.(files|chunks)$" } },
                            { "command.find": { "$regex": "\\.fs\\.(files|chunks)$" } },
                            { "command.delete": { "$regex": "\\.fs\\.(files|chunks)$" } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(300)),
                ),
            ),
            (
                "collection_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.collStats": { "$exists": true },
                        "$or": [
                            { "command.collStats": { "$regex": "\\.fs\\.files$" } },
                            { "command.collStats": { "$regex": "\\.fs\\.chunks$" } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "slow_gridfs_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "ns": { "$regex": "\\.fs\\.(files|chunks)" },
                        "millis": { "$gte": 5000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(50)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential GridFS metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "gridfs"
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

impl MongoGridFSInfo {
    const LARGE_FILE_THRESHOLD_MB: f64 = 100.0; // 100MB
    const SMALL_FILE_THRESHOLD_BYTES: u64 = 1024; // 1KB
    const STANDARD_CHUNK_SIZE: u64 = 261120; // 255KB
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut gridfs_stats = MongoGridFSInfo::default();
        let requests = self.request();

        // Set standard chunk size
        gridfs_stats.standard_chunk_size_bytes = Self::STANDARD_CHUNK_SIZE;

        // Execute queries to get GridFS information
        let files_docs = fetch(&requests, "gridfs_files", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_gridfs_files(&mut gridfs_stats, &files_docs)?;

        let chunks_docs = fetch(&requests, "gridfs_chunks", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_gridfs_chunks(&mut gridfs_stats, &chunks_docs)?;

        let operations_docs = fetch(&requests, "gridfs_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_gridfs_operations(&mut gridfs_stats, &operations_docs)?;

        let collection_stats_docs = fetch(&requests, "collection_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_collection_stats(&mut gridfs_stats, &collection_stats_docs)?;

        let slow_ops_docs = fetch(&requests, "slow_gridfs_operations", context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_slow_operations(&mut gridfs_stats, &slow_ops_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut gridfs_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        gridfs_stats.detailed_metrics = self.collect_detailed_metrics_if_needed(&gridfs_stats, &requests, context).await?;

        Ok(gridfs_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoGridFSInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoGridFSDetailedMetrics>> {
        let needs_oversized_details = core_stats.large_files > 0;
        let needs_small_file_details = core_stats.small_files > 0;
        let needs_orphan_details = core_stats.orphaned_chunks > 0;
        let needs_integrity_details = core_stats.incomplete_files > 0;
        let needs_performance_details = core_stats.avg_upload_throughput_bps < 1024.0 * 1024.0; // Less than 1MB/s

        if !needs_oversized_details
            && !needs_small_file_details
            && !needs_orphan_details
            && !needs_integrity_details
            && !needs_performance_details
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoGridFSDetailedMetrics {
            oversized_files: Vec::new(),
            inefficient_small_files: Vec::new(),
            orphaned_chunks: Vec::new(),
            integrity_issues: Vec::new(),
            performance_issues: None,
            bucket_utilization: None,
            file_patterns: None,
        };

        // Collect oversized files if needed
        if needs_oversized_details {
            let docs = fetch(requests, "gridfs_files", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.oversized_files = Self::parse_oversized_files(docs)?;
        }

        // Collect small files if needed
        if needs_small_file_details {
            let docs = fetch(requests, "gridfs_files", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.inefficient_small_files = Self::parse_small_files(docs)?;
        }

        // Collect orphaned chunks if needed
        if needs_orphan_details {
            let docs = fetch(requests, "gridfs_chunks", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.orphaned_chunks = Self::parse_orphaned_chunks(docs)?;
        }

        // Collect integrity issues if needed
        if needs_integrity_details {
            detailed_metrics.integrity_issues = Self::detect_integrity_issues(core_stats)?;
        }

        // Collect performance issues if needed
        if needs_performance_details {
            detailed_metrics.performance_issues = Some(Self::analyze_performance_issues(core_stats)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_gridfs_files(stats: &mut MongoGridFSInfo, docs: &[Document]) -> ResultEP<()> {
        let mut file_sizes = Vec::new();
        let mut large_file_count = 0;
        let mut small_file_count = 0;
        let mut non_standard_chunk_count = 0;
        let mut buckets = std::collections::HashSet::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            // Extract bucket information from namespace
            if let Some(ns) = acc.opt_string("ns")
                && let Some(bucket) = ns.strip_suffix(".fs.files")
            {
                buckets.insert(bucket.to_string());
            }

            // Parse file information from profiler results
            if let Some(result_acc) = acc.child("result")
                && let Some(file_accessors) = result_acc.array("cursor")
            {
                for file_acc in &file_accessors {
                    // File size analysis
                    if let Some(length) = file_acc.opt_i64("length") {
                        file_sizes.push(length);

                        let length_bytes = length as u64;
                        if length_bytes > (Self::LARGE_FILE_THRESHOLD_MB * 1024.0 * 1024.0) as u64 {
                            large_file_count += 1;
                        }
                        if length_bytes < Self::SMALL_FILE_THRESHOLD_BYTES {
                            small_file_count += 1;
                        }
                    }

                    // Chunk size analysis
                    if let Some(chunk_size) = file_acc.opt_i32("chunkSize")
                        && chunk_size as u64 != Self::STANDARD_CHUNK_SIZE
                    {
                        non_standard_chunk_count += 1;
                    }
                }
            }
        }

        stats.total_gridfs_buckets = buckets.len() as u64;
        stats.total_files = file_sizes.len() as u64;
        stats.large_files = large_file_count;
        stats.small_files = small_file_count;
        stats.non_standard_chunk_files = non_standard_chunk_count;

        if !file_sizes.is_empty() {
            stats.avg_file_size_bytes = file_sizes.iter().sum::<i64>() as f64 / file_sizes.len() as f64;
            stats.max_file_size_bytes = file_sizes.iter().max().copied().unwrap_or(0) as u64;
            stats.min_file_size_bytes = file_sizes.iter().min().copied().unwrap_or(0) as u64;
            stats.total_storage_size_bytes = file_sizes.iter().sum::<i64>() as u64;
        }

        Ok(())
    }

    fn parse_gridfs_chunks(stats: &mut MongoGridFSInfo, docs: &[Document]) -> ResultEP<()> {
        let mut chunk_sizes = Vec::new();
        let mut chunk_count = 0;
        let mut orphaned_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(result_acc) = acc.child("result")
                && let Some(chunk_accessors) = result_acc.array("cursor")
            {
                for chunk_acc in &chunk_accessors {
                    chunk_count += 1;

                    // Analyze chunk size (no DocAccessor equivalent for binary)
                    if let Ok(data) = chunk_acc.raw().get_binary_generic("data") {
                        chunk_sizes.push(data.len());
                    }

                    // Check for orphaned chunks (simplified check)
                    if chunk_acc.raw().get("files_id").is_some() {
                        orphaned_count += 1;
                    }
                }
            }
        }

        stats.total_chunks = chunk_count;
        stats.orphaned_chunks = orphaned_count;

        if !chunk_sizes.is_empty() {
            stats.avg_chunk_size_bytes = chunk_sizes.iter().sum::<usize>() as f64 / chunk_sizes.len() as f64;
        }

        Ok(())
    }

    fn parse_gridfs_operations(stats: &mut MongoGridFSInfo, docs: &[Document]) -> ResultEP<()> {
        let mut upload_count = 0;
        let mut download_count = 0;
        let mut delete_count = 0;
        let mut upload_throughput_samples = Vec::new();
        let mut download_throughput_samples = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            let ns = acc.opt_string("ns").unwrap_or_default();

            if let Some(command_acc) = acc.child("command") {
                // Classify operations
                if command_acc.raw().contains_key("insert") && ns.contains(".fs.") {
                    upload_count += 1;

                    // Calculate throughput if timing information is available
                    if let (Some(millis), Ok(bytes)) = (acc.opt_f64("millis"), Self::estimate_operation_size(doc))
                        && millis > 0.0
                    {
                        upload_throughput_samples.push(bytes / (millis / 1000.0));
                    }
                } else if command_acc.raw().contains_key("find") && ns.contains(".fs.") {
                    download_count += 1;

                    if let (Some(millis), Ok(bytes)) = (acc.opt_f64("millis"), Self::estimate_operation_size(doc))
                        && millis > 0.0
                    {
                        download_throughput_samples.push(bytes / (millis / 1000.0));
                    }
                } else if command_acc.raw().contains_key("delete") && ns.contains(".fs.") {
                    delete_count += 1;
                }
            }
        }

        stats.recent_uploads = upload_count;
        stats.recent_downloads = download_count;
        stats.recent_deletions = delete_count;

        if !upload_throughput_samples.is_empty() {
            stats.avg_upload_throughput_bps = upload_throughput_samples.iter().sum::<f64>() / upload_throughput_samples.len() as f64;
        }

        if !download_throughput_samples.is_empty() {
            stats.avg_download_throughput_bps = download_throughput_samples.iter().sum::<f64>() / download_throughput_samples.len() as f64;
        }

        Ok(())
    }

    fn parse_collection_stats(stats: &mut MongoGridFSInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(result_acc) = acc.child("result") {
                // Update storage size from collection stats
                if let Some(size) = result_acc.opt_i64("size") {
                    stats.total_storage_size_bytes += size as u64;
                }

                // Count documents (files or chunks)
                if let Some(count) = result_acc.opt_i64("count") {
                    let coll_stats = acc.child("command").and_then(|c| c.opt_string("collStats")).unwrap_or_default();
                    if coll_stats.contains(".files") {
                        stats.total_files = count as u64;
                    } else if coll_stats.contains(".chunks") {
                        stats.total_chunks = count as u64;
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_slow_operations(_stats: &mut MongoGridFSInfo, _docs: &[Document]) -> ResultEP<()> {
        // This would analyze slow GridFS operations for performance bottlenecks
        // For now, we'll use the count as an indicator of performance issues

        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoGridFSInfo) -> ResultEP<()> {
        // Calculate storage efficiency (actual file data vs total storage including overhead)
        if stats.total_storage_size_bytes > 0 {
            // GridFS has some overhead from chunk metadata and file documents
            let estimated_metadata_overhead = stats.total_files * 1024 + stats.total_chunks * 256; // Rough estimate
            let useful_data = stats.total_storage_size_bytes.saturating_sub(estimated_metadata_overhead);
            stats.storage_efficiency_ratio = useful_data as f64 / stats.total_storage_size_bytes as f64;
        }

        if stats.total_chunks > 0 && stats.total_files > 0 {
            let avg_chunks_per_file = stats.total_chunks as f64 / stats.total_files as f64;
            if avg_chunks_per_file < 1.5 && stats.avg_file_size_bytes > Self::STANDARD_CHUNK_SIZE as f64 {
                stats.incomplete_files = (stats.total_files as f64 * 0.1) as u64;
            }
        }

        Ok(())
    }

    fn estimate_operation_size(doc: &Document) -> ResultEP<f64> {
        let acc = DocAccessor::new(doc);
        if let Some(nreturned) = acc.opt_i64("nreturned") {
            Ok(nreturned as f64 * 1024.0) // Rough estimate: 1KB per document
        } else if let Some(n_docs) = acc.opt_i64("ninserted") {
            Ok(n_docs as f64 * 10240.0) // Rough estimate: 10KB per inserted document (chunks)
        } else {
            Ok(1024.0) // Default estimate
        }
    }

    fn parse_oversized_files(docs: Vec<Document>) -> ResultEP<Vec<MongoOversizedFile>> {
        let mut files = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result_acc) = acc.child("result")
                && let Some(file_accessors) = result_acc.array("cursor")
            {
                for file_acc in &file_accessors {
                    if let Some(length) = file_acc.opt_i64("length") {
                        let size_mb = length as f64 / 1024.0 / 1024.0;
                        if size_mb > MongoGridFSInfo::LARGE_FILE_THRESHOLD_MB {
                            let bucket =
                                acc.opt_string("ns").and_then(|ns| ns.strip_suffix(".fs.files").map(|s| s.to_string())).unwrap_or_default();

                            files.push(MongoOversizedFile {
                                file_id: file_acc.raw().get_object_id("_id").ok().map(|id| id.to_string()).unwrap_or_default(),
                                filename: file_acc.opt_string("filename").unwrap_or_else(|| "unknown".to_string()),
                                size_mb,
                                chunk_count: (length as f64 / MongoGridFSInfo::STANDARD_CHUNK_SIZE as f64).ceil() as u64,
                                upload_date: file_acc.opt_datetime("uploadDate"),
                                content_type: file_acc.opt_string("contentType"),
                                bucket,
                                recommended_action: "Consider external blob storage for large files".to_string(),
                                storage_cost_impact: 0.0,
                            });

                            if files.len() >= Self::MAX_DETAILED_RESULTS {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    fn parse_small_files(docs: Vec<Document>) -> ResultEP<Vec<MongoInefficientSmallFile>> {
        let mut files = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result_acc) = acc.child("result")
                && let Some(file_accessors) = result_acc.array("cursor")
            {
                for file_acc in &file_accessors {
                    if let Some(length) = file_acc.opt_i64("length")
                        && (length as u64) < MongoGridFSInfo::SMALL_FILE_THRESHOLD_BYTES
                    {
                        files.push(MongoInefficientSmallFile {
                            file_id: file_acc.raw().get_object_id("_id").ok().map(|id| id.to_string()).unwrap_or_default(),
                            filename: file_acc.opt_string("filename").unwrap_or_else(|| "unknown".to_string()),
                            size_bytes: length as u64,
                            overhead_ratio: (MongoGridFSInfo::STANDARD_CHUNK_SIZE as f64) / (length as f64),
                            upload_date: file_acc.opt_datetime("uploadDate"),
                            content_type: file_acc.opt_string("contentType"),
                            bucket: "default".to_string(),
                            recommended_action: "Consider storing small files directly in documents".to_string(),
                            efficiency_loss_percentage: ((MongoGridFSInfo::STANDARD_CHUNK_SIZE as f64 - length as f64)
                                / MongoGridFSInfo::STANDARD_CHUNK_SIZE as f64)
                                * 100.0,
                        });

                        if files.len() >= Self::MAX_DETAILED_RESULTS {
                            break;
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    fn parse_orphaned_chunks(docs: Vec<Document>) -> ResultEP<Vec<MongoOrphanedChunk>> {
        let mut chunks = Vec::new();

        for doc in &docs {
            let acc = DocAccessor::new(doc);

            if let Some(result_acc) = acc.child("result")
                && let Some(chunk_accessors) = result_acc.array("cursor")
            {
                for chunk_acc in &chunk_accessors {
                    // Check if this chunk appears to be orphaned (no DocAccessor equivalent for object_id)
                    if let (Ok(chunk_id), Ok(files_id), Some(n)) = (
                        chunk_acc.raw().get_object_id("_id"),
                        chunk_acc.raw().get_object_id("files_id"),
                        chunk_acc.opt_i32("n"),
                    ) {
                        let size_bytes = chunk_acc.raw().get_binary_generic("data").map(|data| data.len() as u64).unwrap_or(0);

                        if size_bytes == 0 || n < 0 {
                            chunks.push(MongoOrphanedChunk {
                                chunk_id: chunk_id.to_string(),
                                file_id: files_id.to_string(),
                                chunk_number: n as u32,
                                size_bytes,
                                bucket: String::new(),
                                last_accessed: None,
                                cleanup_priority: if size_bytes > 1024 * 1024 {
                                    "High".to_string()
                                } else {
                                    "Low".to_string()
                                },
                                storage_waste_bytes: size_bytes,
                            });

                            if chunks.len() >= Self::MAX_DETAILED_RESULTS {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(chunks)
    }

    fn detect_integrity_issues(_stats: &MongoGridFSInfo) -> ResultEP<Vec<MongoFileIntegrityIssue>> {
        Ok(Vec::new())
    }

    fn analyze_performance_issues(stats: &MongoGridFSInfo) -> ResultEP<Vec<MongoGridFSPerformanceIssue>> {
        let mut issues = Vec::new();

        // Analyze upload performance
        if stats.avg_upload_throughput_bps < 1024.0 * 1024.0 {
            // Less than 1MB/s
            issues.push(MongoGridFSPerformanceIssue {
                operation_type: "Upload".to_string(),
                avg_duration_ms: 0.0,
                affected_files: stats.recent_uploads,
                bottleneck_type: "Network or disk I/O".to_string(),
                recommended_optimization: "Check network latency and disk performance".to_string(),
                performance_impact: "High".to_string(),
            });
        }

        // Analyze download performance
        if stats.avg_download_throughput_bps < 5.0 * 1024.0 * 1024.0 {
            // Less than 5MB/s
            issues.push(MongoGridFSPerformanceIssue {
                operation_type: "Download".to_string(),
                avg_duration_ms: 0.0,
                affected_files: stats.recent_downloads,
                bottleneck_type: "Query performance or index missing".to_string(),
                recommended_optimization: "Add indexes on GridFS files collection".to_string(),
                performance_impact: "Medium".to_string(),
            });
        }

        // Analyze chunk size efficiency
        if stats.non_standard_chunk_files > stats.total_files / 4 {
            // More than 25% non-standard
            issues.push(MongoGridFSPerformanceIssue {
                operation_type: "Storage".to_string(),
                avg_duration_ms: 0.0,
                affected_files: stats.non_standard_chunk_files,
                bottleneck_type: "Non-optimal chunk sizes".to_string(),
                recommended_optimization: "Use standard 255KB chunk size for better performance".to_string(),
                performance_impact: "Medium".to_string(),
            });
        }

        Ok(issues)
    }
}
