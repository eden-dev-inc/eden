use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};

use super::utils::DocAccessor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// MongoDB WiredTiger storage engine statistics and performance metrics
///
/// Comprehensive metrics about WiredTiger storage engine performance,
/// caching behavior, compression, and transaction processing.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerInfo {
    /// WiredTiger cache size in bytes
    pub cache_size_bytes: u64,
    /// WiredTiger cache size in use
    pub cache_used_bytes: u64,
    /// Cache utilization percentage
    pub cache_utilization_percentage: f64,
    /// Number of cache evictions
    pub cache_evictions: u64,
    /// Number of pages read into cache
    pub cache_pages_read: u64,
    /// Number of pages written from cache
    pub cache_pages_written: u64,
    /// Number of application threads waiting for cache
    pub cache_application_threads_waiting: u64,
    /// Number of cache misses
    pub cache_misses: u64,
    /// Cache hit ratio percentage
    pub cache_hit_ratio_percentage: f64,
    /// Total bytes read from disk
    pub bytes_read_from_disk: u64,
    /// Total bytes written to disk
    pub bytes_written_to_disk: u64,
    /// Number of checkpoints completed
    pub checkpoints_completed: u64,
    /// Average checkpoint duration in milliseconds
    pub avg_checkpoint_duration_ms: f64,
    /// Maximum checkpoint duration in milliseconds
    pub max_checkpoint_duration_ms: f64,
    /// Compression ratio (compressed/uncompressed)
    pub compression_ratio: f64,
    /// Total compressed bytes
    pub compressed_bytes: u64,
    /// Total uncompressed bytes
    pub uncompressed_bytes: u64,
    /// Number of transactions started
    pub transactions_started: u64,
    /// Number of transactions committed
    pub transactions_committed: u64,
    /// Number of transactions rolled back
    pub transactions_rolled_back: u64,
    /// Transaction checkpoint generation
    pub transaction_checkpoint_generation: u64,
    /// Number of files currently open
    pub files_open: u32,
    /// Maximum files opened concurrently
    pub max_files_open: u32,
    /// Number of data handles (tables/indexes) open
    pub data_handles_open: u32,
    /// Block manager bytes allocated
    pub block_manager_bytes_allocated: u64,
    /// Block manager bytes freed
    pub block_manager_bytes_freed: u64,
    /// Cursor operations count
    pub cursor_operations: u64,
    /// Average cursor operation time
    pub avg_cursor_operation_time_ms: f64,
    /// Log manager bytes written
    pub log_bytes_written: u64,
    /// Log manager flush operations
    pub log_flush_operations: u64,
    /// Connection sweep operations
    pub connection_sweep_operations: u64,
    /// Detailed metrics collected only when performance issues are detected
    pub detailed_metrics: Option<MongoWiredTigerDetailedMetrics>,
}

/// Detailed WiredTiger metrics collected only when performance issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerDetailedMetrics {
    /// Cache pressure details (collected when cache utilization is high)
    pub cache_pressure_details: Vec<MongoWiredTigerCachePressure>,
    /// Slow checkpoint details (collected when checkpoints are slow)
    pub slow_checkpoint_details: Vec<MongoWiredTigerSlowCheckpoint>,
    /// Connection and session statistics (collected when resource contention occurs)
    pub connection_session_stats: Option<MongoWiredTigerConnectionStats>,
    /// Block manager statistics (collected when I/O issues are detected)
    pub block_manager_stats: Option<MongoWiredTigerBlockManagerStats>,
    /// Transaction statistics (collected when transaction issues occur)
    pub transaction_stats: Option<MongoWiredTigerTransactionStats>,
    /// Cursor statistics by operation type (collected when cursor performance is poor)
    pub cursor_stats: Option<Vec<MongoWiredTigerCursorStats>>,
    /// Log manager detailed statistics (collected when log performance is poor)
    pub log_manager_stats: Option<MongoWiredTigerLogStats>,
}

impl MetadataCollection for MongoWiredTigerInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "wiredtiger_general".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "$cmd".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "serverStatus": 1,
                        "wiredTiger": 1
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
            (
                "wiredtiger_cache".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "$cmd".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "serverStatus": 1,
                        "wiredTiger": 1
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
            (
                "recent_slow_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 1000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(100)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive WiredTiger storage engine performance metrics"
    }

    fn category(&self) -> &'static str {
        "wiredtiger"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High // WiredTiger metrics change frequently
    }
}

use function_name::named;
use std::time::Duration;
use tokio::time::timeout;

#[allow(dead_code)]
impl MongoWiredTigerInfo {
    const HIGH_CACHE_UTILIZATION_THRESHOLD: f64 = 85.0; // 85% cache usage
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 100;
    const SLOW_CHECKPOINT_THRESHOLD_MS: f64 = 10000.0; // 10 seconds
    const LOW_CACHE_HIT_RATIO_THRESHOLD: f64 = 80.0; // 80% hit ratio
    const HIGH_EVICTION_THRESHOLD: u64 = 1000; // evictions per collection period

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut wiredtiger_info = MongoWiredTigerInfo::default();

        let mongo_client = context.get().await.map_err(EpError::connect)?;
        let admin_db = mongo_client.database("admin");

        // Execute serverStatus command to get WiredTiger stats
        let server_status_result = timeout(Self::QUERY_TIMEOUT, admin_db.run_command(doc! { "serverStatus": 1, "wiredTiger": 1 }, None))
            .await
            .map_err(|_| EpError::metadata("Query timeout for serverStatus"))?;

        match server_status_result {
            Ok(server_status_doc) => {
                Self::parse_server_status(&mut wiredtiger_info, &server_status_doc)?;
            }
            Err(err) => {
                return Err(EpError::metadata(format!("serverStatus command failed; WiredTiger metrics unavailable: {err}")));
            }
        }

        // Conditionally collect detailed metrics only when performance issues are detected
        wiredtiger_info.detailed_metrics = self.collect_detailed_metrics_if_needed(&wiredtiger_info, context).await?;

        Ok(wiredtiger_info)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_info: &MongoWiredTigerInfo,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoWiredTigerDetailedMetrics>> {
        let needs_cache_pressure_details = core_info.cache_utilization_percentage > Self::HIGH_CACHE_UTILIZATION_THRESHOLD;
        let needs_checkpoint_details = core_info.max_checkpoint_duration_ms > Self::SLOW_CHECKPOINT_THRESHOLD_MS;
        let needs_performance_details = core_info.cache_hit_ratio_percentage < Self::LOW_CACHE_HIT_RATIO_THRESHOLD;
        let needs_eviction_details = core_info.cache_evictions > Self::HIGH_EVICTION_THRESHOLD;

        if !needs_cache_pressure_details && !needs_checkpoint_details && !needs_performance_details && !needs_eviction_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoWiredTigerDetailedMetrics {
            cache_pressure_details: Vec::new(),
            slow_checkpoint_details: Vec::new(),
            connection_session_stats: None,
            block_manager_stats: None,
            transaction_stats: None,
            cursor_stats: None,
            log_manager_stats: None,
        };

        // Collect detailed cache pressure information
        if needs_cache_pressure_details {
            detailed_metrics.cache_pressure_details = Self::analyze_cache_pressure(core_info)?;
        }

        // Collect checkpoint performance details
        if needs_checkpoint_details {
            detailed_metrics.slow_checkpoint_details = Self::analyze_checkpoint_performance(core_info)?;
        }

        // Collect connection and session statistics
        if needs_performance_details {
            detailed_metrics.connection_session_stats = Some(Self::get_connection_session_stats(context.clone()).await?);
        }

        // Collect block manager statistics
        if needs_eviction_details {
            detailed_metrics.block_manager_stats = Some(Self::get_block_manager_stats(core_info)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_server_status(info: &mut MongoWiredTigerInfo, server_status: &Document) -> ResultEP<()> {
        let acc = DocAccessor::new(server_status);
        if let Some(wt) = acc.child("wiredTiger") {
            // Parse cache statistics
            if let Some(cache) = wt.child("cache") {
                info.cache_size_bytes = cache.opt_u64("maximum bytes configured").unwrap_or(0);
                info.cache_used_bytes = cache.opt_u64("bytes currently in the cache").unwrap_or(0);
                info.cache_evictions =
                    cache.opt_u64("unmodified pages evicted").unwrap_or(0) + cache.opt_u64("modified pages evicted").unwrap_or(0);
                info.cache_pages_read = cache.opt_u64("pages read into cache").unwrap_or(0);
                info.cache_pages_written = cache.opt_u64("pages written from cache").unwrap_or(0);
                info.cache_application_threads_waiting = cache.opt_u64("application threads waiting for cache space").unwrap_or(0);
                info.cache_misses = cache.opt_u64("cache misses").unwrap_or(0);

                // Calculate cache utilization
                if info.cache_size_bytes > 0 {
                    info.cache_utilization_percentage = (info.cache_used_bytes as f64 / info.cache_size_bytes as f64) * 100.0;
                }

                // Calculate cache hit ratio
                let cache_hits = cache.opt_u64("cache hits").unwrap_or(0);
                let total_cache_requests = cache_hits + info.cache_misses;
                if total_cache_requests > 0 {
                    info.cache_hit_ratio_percentage = (cache_hits as f64 / total_cache_requests as f64) * 100.0;
                }
            }

            // Parse transaction statistics
            if let Some(txn) = wt.child("transaction") {
                info.transactions_started = txn.opt_u64("transaction begins").unwrap_or(0);
                info.transactions_committed = txn.opt_u64("transaction commits").unwrap_or(0);
                info.transactions_rolled_back = txn.opt_u64("transaction rollbacks").unwrap_or(0);
                info.transaction_checkpoint_generation = txn.opt_u64("transaction checkpoint generation").unwrap_or(0);
            }

            // Parse block manager statistics
            if let Some(block) = wt.child("block-manager") {
                info.block_manager_bytes_allocated = block.opt_u64("bytes allocated").unwrap_or(0);
                info.block_manager_bytes_freed = block.opt_u64("bytes freed").unwrap_or(0);
            }

            // Parse data handle statistics
            if let Some(dh) = wt.child("data-handle") {
                info.data_handles_open = dh.opt_u64("data handles currently active").unwrap_or(0) as u32;
            }

            // Parse connection statistics
            if let Some(conn) = wt.child("connection") {
                info.files_open = conn.opt_u64("files currently open").unwrap_or(0) as u32;
                info.max_files_open = conn.opt_u64("maximum files open").unwrap_or(0) as u32;
                info.connection_sweep_operations = conn.opt_u64("total sweep operations").unwrap_or(0);
            }

            // Parse cursor statistics
            if let Some(cursor) = wt.child("cursor") {
                info.cursor_operations = cursor.opt_u64("cursor operations").unwrap_or(0);
            }

            // Parse log statistics
            if let Some(log) = wt.child("log") {
                info.log_bytes_written = log.opt_u64("log bytes written").unwrap_or(0);
                info.log_flush_operations = log.opt_u64("log flush operations").unwrap_or(0);
            }

            // Parse compression statistics (if available)
            Self::parse_compression_stats(info, wt.raw())?;

            // Parse checkpoint statistics (if available)
            Self::parse_checkpoint_stats(info, wt.raw())?;
        }

        Ok(())
    }

    fn parse_compression_stats(info: &mut MongoWiredTigerInfo, wt_doc: &Document) -> ResultEP<()> {
        let acc = DocAccessor::new(wt_doc);
        // Compression stats might be under different paths depending on MongoDB version
        if let Some(compression) = acc.child("compression") {
            info.compressed_bytes = compression.opt_u64("compressed bytes written").unwrap_or(0);
            info.uncompressed_bytes = compression.opt_u64("uncompressed bytes written").unwrap_or(0);

            if info.uncompressed_bytes > 0 {
                info.compression_ratio = info.compressed_bytes as f64 / info.uncompressed_bytes as f64;
            }
        }
        Ok(())
    }

    fn parse_checkpoint_stats(info: &mut MongoWiredTigerInfo, wt_doc: &Document) -> ResultEP<()> {
        let acc = DocAccessor::new(wt_doc);
        if let Some(checkpoint) = acc.child("checkpoint") {
            info.checkpoints_completed = checkpoint.opt_u64("checkpoints completed").unwrap_or(0);

            // Parse checkpoint timing if available
            let total_checkpoint_time = checkpoint.opt_u64("checkpoint time (ms)").unwrap_or(0);
            if info.checkpoints_completed > 0 {
                info.avg_checkpoint_duration_ms = total_checkpoint_time as f64 / info.checkpoints_completed as f64;
            }
            info.max_checkpoint_duration_ms = checkpoint.opt_f64("maximum checkpoint time (ms)").unwrap_or(0.0);
        }
        Ok(())
    }

    fn analyze_cache_pressure(core_info: &MongoWiredTigerInfo) -> ResultEP<Vec<MongoWiredTigerCachePressure>> {
        let mut pressure_details = Vec::new();

        if core_info.cache_utilization_percentage > Self::HIGH_CACHE_UTILIZATION_THRESHOLD {
            pressure_details.push(MongoWiredTigerCachePressure {
                pressure_type: "High Cache Utilization".to_string(),
                severity_level: if core_info.cache_utilization_percentage > 95.0 { 9 } else { 7 },
                current_value: core_info.cache_utilization_percentage,
                threshold_value: Self::HIGH_CACHE_UTILIZATION_THRESHOLD,
                description: format!("Cache utilization at {:.1}% is above recommended threshold", core_info.cache_utilization_percentage),
                recommendation: "Consider increasing cache size or optimizing query patterns".to_string(),
                timestamp: DateTimeWrapper::from(Utc::now()),
            });
        }

        if core_info.cache_application_threads_waiting > 0 {
            pressure_details.push(MongoWiredTigerCachePressure {
                pressure_type: "Application Threads Waiting".to_string(),
                severity_level: 8,
                current_value: core_info.cache_application_threads_waiting as f64,
                threshold_value: 0.0,
                description: format!("{} application threads waiting for cache space", core_info.cache_application_threads_waiting),
                recommendation: "Increase cache size or reduce working set size".to_string(),
                timestamp: DateTimeWrapper::from(Utc::now()),
            });
        }

        Ok(pressure_details)
    }

    fn analyze_checkpoint_performance(core_info: &MongoWiredTigerInfo) -> ResultEP<Vec<MongoWiredTigerSlowCheckpoint>> {
        let mut slow_checkpoints = Vec::new();

        if core_info.max_checkpoint_duration_ms > Self::SLOW_CHECKPOINT_THRESHOLD_MS {
            slow_checkpoints.push(MongoWiredTigerSlowCheckpoint {
                checkpoint_id: format!("checkpoint_{}", core_info.checkpoints_completed),
                duration_ms: core_info.max_checkpoint_duration_ms,
                data_written_bytes: core_info.bytes_written_to_disk,
                pages_written: core_info.cache_pages_written,
                blocking_operations: 0, // Would need additional stats
                timestamp: DateTimeWrapper::from(Utc::now()),
                performance_impact: if core_info.max_checkpoint_duration_ms > 30000.0 {
                    "High".to_string()
                } else {
                    "Medium".to_string()
                },
            });
        }

        Ok(slow_checkpoints)
    }

    async fn get_connection_session_stats(context: MongoAsync) -> ResultEP<MongoWiredTigerConnectionStats> {
        let mongo_client = context.get().await.map_err(EpError::connect)?;
        let admin_db = mongo_client.database("admin");

        let server_status = timeout(Self::QUERY_TIMEOUT, admin_db.run_command(doc! { "serverStatus": 1 }, None))
            .await
            .map_err(|_| EpError::metadata("Query timeout for serverStatus (connection stats)"))?
            .map_err(EpError::database)?;

        let acc = DocAccessor::new(&server_status);
        let connections = acc.child("connections");
        let total_connections = connections.as_ref().and_then(|c| c.opt_u64("current")).unwrap_or(0) as u32;
        let available = connections.as_ref().and_then(|c| c.opt_u64("available")).unwrap_or(0) as u32;
        let total_created = connections.as_ref().and_then(|c| c.opt_u64("totalCreated")).unwrap_or(0);
        let active_connections = connections.as_ref().and_then(|c| c.opt_u64("active")).unwrap_or(0) as u32;
        let idle_connections = total_connections.saturating_sub(active_connections);

        Ok(MongoWiredTigerConnectionStats {
            total_connections,
            active_connections,
            idle_connections,
            connection_creation_rate: total_created as f64,
            connection_destruction_rate: 0.0,
            average_connection_lifetime_seconds: 0.0,
            session_creation_rate: 0.0,
            session_destruction_rate: 0.0,
            total_sessions: available,
            active_sessions: active_connections,
        })
    }

    fn get_block_manager_stats(core_info: &MongoWiredTigerInfo) -> ResultEP<MongoWiredTigerBlockManagerStats> {
        Ok(MongoWiredTigerBlockManagerStats {
            bytes_allocated: core_info.block_manager_bytes_allocated,
            bytes_freed: core_info.block_manager_bytes_freed,
            allocation_rate_bytes_per_second: 0.0,
            free_rate_bytes_per_second: 0.0,
            file_allocation_operations: 0,
            file_extension_operations: 0,
            block_reuse_percentage: if core_info.block_manager_bytes_allocated > 0 {
                (core_info.block_manager_bytes_freed as f64 / core_info.block_manager_bytes_allocated as f64) * 100.0
            } else {
                0.0
            },
            fragmentation_percentage: 0.0,
        })
    }
}

/// Information about cache pressure situations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerCachePressure {
    /// Type of cache pressure
    pub pressure_type: String,
    /// Severity level (1-10)
    pub severity_level: u32,
    /// Current metric value
    pub current_value: f64,
    /// Threshold value that was exceeded
    pub threshold_value: f64,
    /// Description of the pressure situation
    pub description: String,
    /// Recommendation to address the pressure
    pub recommendation: String,
    /// Timestamp when pressure was detected
    pub timestamp: DateTimeWrapper,
}

/// Information about slow checkpoint operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerSlowCheckpoint {
    /// Checkpoint identifier
    pub checkpoint_id: String,
    /// Duration of the checkpoint in milliseconds
    pub duration_ms: f64,
    /// Amount of data written during checkpoint
    pub data_written_bytes: u64,
    /// Number of pages written
    pub pages_written: u64,
    /// Number of operations blocked by checkpoint
    pub blocking_operations: u32,
    /// Timestamp when checkpoint occurred
    pub timestamp: DateTimeWrapper,
    /// Performance impact level
    pub performance_impact: String,
}

/// Connection and session statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerConnectionStats {
    /// Total number of connections
    pub total_connections: u32,
    /// Number of active connections
    pub active_connections: u32,
    /// Number of idle connections
    pub idle_connections: u32,
    /// Rate of connection creation (per second)
    pub connection_creation_rate: f64,
    /// Rate of connection destruction (per second)
    pub connection_destruction_rate: f64,
    /// Average connection lifetime in seconds
    pub average_connection_lifetime_seconds: f64,
    /// Rate of session creation (per second)
    pub session_creation_rate: f64,
    /// Rate of session destruction (per second)
    pub session_destruction_rate: f64,
    /// Total number of sessions
    pub total_sessions: u32,
    /// Number of active sessions
    pub active_sessions: u32,
}

/// Block manager statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerBlockManagerStats {
    /// Total bytes allocated
    pub bytes_allocated: u64,
    /// Total bytes freed
    pub bytes_freed: u64,
    /// Allocation rate in bytes per second
    pub allocation_rate_bytes_per_second: f64,
    /// Free rate in bytes per second
    pub free_rate_bytes_per_second: f64,
    /// Number of file allocation operations
    pub file_allocation_operations: u64,
    /// Number of file extension operations
    pub file_extension_operations: u64,
    /// Percentage of blocks reused
    pub block_reuse_percentage: f64,
    /// File fragmentation percentage
    pub fragmentation_percentage: f64,
}

/// Transaction processing statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerTransactionStats {
    /// Average transaction duration in milliseconds
    pub avg_transaction_duration_ms: f64,
    /// Maximum transaction duration in milliseconds
    pub max_transaction_duration_ms: f64,
    /// Transaction throughput (transactions per second)
    pub transaction_throughput_per_second: f64,
    /// Average transaction size in bytes
    pub avg_transaction_size_bytes: f64,
    /// Rollback rate percentage
    pub rollback_rate_percentage: f64,
    /// Read-only transaction percentage
    pub read_only_transaction_percentage: f64,
    /// Transaction conflict rate
    pub conflict_rate_per_second: f64,
}

/// Cursor operation statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerCursorStats {
    /// Type of cursor operation
    pub operation_type: String,
    /// Number of operations
    pub operation_count: u64,
    /// Average operation time in milliseconds
    pub avg_operation_time_ms: f64,
    /// Maximum operation time in milliseconds
    pub max_operation_time_ms: f64,
    /// Cache hit rate for this operation type
    pub cache_hit_rate_percentage: f64,
    /// Bytes processed per operation
    pub avg_bytes_per_operation: f64,
}

/// Log manager statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoWiredTigerLogStats {
    /// Total log bytes written
    pub total_log_bytes_written: u64,
    /// Log write rate in bytes per second
    pub log_write_rate_bytes_per_second: f64,
    /// Number of log flush operations
    pub log_flush_operations: u64,
    /// Average log flush time in milliseconds
    pub avg_log_flush_time_ms: f64,
    /// Log file size in bytes
    pub log_file_size_bytes: u64,
    /// Number of log files
    pub log_file_count: u32,
    /// Log archival operations
    pub log_archival_operations: u64,
}

impl MongoWiredTigerInfo {
    /// Checks if cache performance is healthy
    pub fn is_cache_healthy(&self) -> bool {
        self.cache_utilization_percentage < Self::HIGH_CACHE_UTILIZATION_THRESHOLD
            && self.cache_hit_ratio_percentage > Self::LOW_CACHE_HIT_RATIO_THRESHOLD
            && self.cache_application_threads_waiting == 0
    }

    /// Checks if checkpoint performance is acceptable
    pub fn is_checkpoint_performance_healthy(&self) -> bool {
        self.max_checkpoint_duration_ms < Self::SLOW_CHECKPOINT_THRESHOLD_MS
            && self.avg_checkpoint_duration_ms < (Self::SLOW_CHECKPOINT_THRESHOLD_MS / 2.0)
    }

    /// Checks if there's cache pressure
    pub fn has_cache_pressure(&self) -> bool {
        self.cache_utilization_percentage > Self::HIGH_CACHE_UTILIZATION_THRESHOLD
            || self.cache_application_threads_waiting > 0
            || self.cache_evictions > Self::HIGH_EVICTION_THRESHOLD
    }

    /// Checks if I/O performance is healthy
    pub fn is_io_performance_healthy(&self) -> bool {
        // Check if read/write ratios are reasonable
        let total_io = self.bytes_read_from_disk + self.bytes_written_to_disk;
        if total_io == 0 {
            return true;
        }

        // Check if compression is working effectively
        self.compression_ratio < 0.9 && self.compression_ratio > 0.1
    }

    /// Returns cache efficiency score (0-100)
    ///
    /// Based primarily on hit ratio (how well the cache serves requests),
    /// with penalties for high utilization pressure and excessive evictions.
    pub fn cache_efficiency_score(&self) -> f64 {
        let mut score = self.cache_hit_ratio_percentage;

        // Penalize high utilization (indicates cache pressure)
        if self.cache_utilization_percentage > 90.0 {
            score -= (self.cache_utilization_percentage - 90.0) * 2.0;
        }

        // Penalize excessive evictions
        if self.cache_evictions > Self::HIGH_EVICTION_THRESHOLD {
            score -= 10.0;
        }

        score.clamp(0.0, 100.0)
    }

    /// Returns storage efficiency percentage
    pub fn storage_efficiency_percentage(&self) -> f64 {
        if self.uncompressed_bytes == 0 {
            100.0
        } else {
            ((self.uncompressed_bytes - self.compressed_bytes) as f64 / self.uncompressed_bytes as f64) * 100.0
        }
    }

    /// Checks if transaction processing is healthy
    pub fn is_transaction_processing_healthy(&self) -> bool {
        if self.transactions_started == 0 {
            return true;
        }

        let commit_rate = (self.transactions_committed as f64 / self.transactions_started as f64) * 100.0;
        let rollback_rate = (self.transactions_rolled_back as f64 / self.transactions_started as f64) * 100.0;

        commit_rate > 95.0 && rollback_rate < 5.0
    }

    /// Returns transaction success rate percentage
    pub fn transaction_success_rate_percentage(&self) -> f64 {
        if self.transactions_started == 0 {
            0.0
        } else {
            (self.transactions_committed as f64 / self.transactions_started as f64) * 100.0
        }
    }

    /// Checks if file handle usage is within limits
    pub fn is_file_handle_usage_healthy(&self) -> bool {
        if self.max_files_open == 0 {
            return true;
        }

        let usage_percentage = (self.files_open as f64 / self.max_files_open as f64) * 100.0;
        usage_percentage < 80.0 // Less than 80% of max file handles
    }

    /// Returns overall WiredTiger health score (0-100)
    pub fn overall_health_score(&self) -> f64 {
        let cache_score = if self.is_cache_healthy() {
            25.0
        } else {
            self.cache_efficiency_score() * 0.25
        };

        let checkpoint_score = if self.is_checkpoint_performance_healthy() { 20.0 } else { 10.0 };

        let io_score = if self.is_io_performance_healthy() { 20.0 } else { 10.0 };

        let transaction_score = if self.is_transaction_processing_healthy() {
            20.0
        } else {
            self.transaction_success_rate_percentage() * 0.2
        };

        let file_handle_score = if self.is_file_handle_usage_healthy() { 15.0 } else { 5.0 };

        cache_score + checkpoint_score + io_score + transaction_score + file_handle_score
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Calculates I/O throughput in MB/s (estimated)
    pub fn estimated_io_throughput_mb_per_second(&self) -> f64 {
        // Assuming metrics cover a reasonable time window (e.g., 60 seconds)
        let total_io_bytes = self.bytes_read_from_disk + self.bytes_written_to_disk;
        (total_io_bytes as f64 / (1024.0 * 1024.0)) / 60.0 // Convert to MB/s
    }

    /// Returns cache pressure level (None, Low, Medium, High, Critical)
    pub fn cache_pressure_level(&self) -> String {
        if self.cache_application_threads_waiting > 10 {
            "Critical".to_string()
        } else if self.cache_utilization_percentage > 95.0 {
            "High".to_string()
        } else if self.cache_utilization_percentage > Self::HIGH_CACHE_UTILIZATION_THRESHOLD {
            "Medium".to_string()
        } else if self.cache_utilization_percentage > 70.0 {
            "Low".to_string()
        } else {
            "None".to_string()
        }
    }

    /// Generates performance recommendations
    pub fn performance_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !self.is_cache_healthy() {
            if self.cache_utilization_percentage > Self::HIGH_CACHE_UTILIZATION_THRESHOLD {
                recommendations.push("Consider increasing WiredTiger cache size to reduce cache pressure.".to_string());
            }

            if self.cache_hit_ratio_percentage < Self::LOW_CACHE_HIT_RATIO_THRESHOLD {
                recommendations.push("Low cache hit ratio detected. Review query patterns and indexing strategy.".to_string());
            }

            if self.cache_application_threads_waiting > 0 {
                recommendations
                    .push("Application threads waiting for cache space. Increase cache size or optimize working set.".to_string());
            }
        }

        if !self.is_checkpoint_performance_healthy() {
            recommendations.push("Slow checkpoints detected. Consider tuning checkpoint frequency or improving I/O subsystem.".to_string());
        }

        if self.compression_ratio > 0.9 {
            recommendations.push("Poor compression ratio. Review compression algorithm settings or data patterns.".to_string());
        }

        if !self.is_transaction_processing_healthy() {
            recommendations.push("High transaction rollback rate. Review transaction logic and conflict resolution.".to_string());
        }

        if !self.is_file_handle_usage_healthy() {
            recommendations
                .push("High file handle usage. Consider consolidating collections or increasing file handle limits.".to_string());
        }

        if self.cache_evictions > Self::HIGH_EVICTION_THRESHOLD {
            recommendations.push("High cache eviction rate. Increase cache size or reduce working set size.".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("WiredTiger performance appears healthy. Continue monitoring.".to_string());
        }

        recommendations
    }

    /// Returns resource utilization summary
    pub fn resource_utilization_summary(&self) -> Vec<(String, f64, String)> {
        vec![
            ("Cache Utilization".to_string(), self.cache_utilization_percentage, "%".to_string()),
            ("Cache Hit Ratio".to_string(), self.cache_hit_ratio_percentage, "%".to_string()),
            ("Storage Efficiency".to_string(), self.storage_efficiency_percentage(), "%".to_string()),
            ("Transaction Success Rate".to_string(), self.transaction_success_rate_percentage(), "%".to_string()),
            (
                "File Handle Usage".to_string(),
                if self.max_files_open > 0 {
                    (self.files_open as f64 / self.max_files_open as f64) * 100.0
                } else {
                    0.0
                },
                "%".to_string(),
            ),
        ]
    }

    /// Checks if WiredTiger needs immediate attention
    pub fn needs_immediate_attention(&self) -> bool {
        self.cache_application_threads_waiting > 0
            || self.cache_utilization_percentage > 98.0
            || self.max_checkpoint_duration_ms > 60000.0 // 1 minute checkpoints
            || self.transaction_success_rate_percentage() < 90.0
    }

    /// Returns estimated memory pressure score (0-100)
    pub fn memory_pressure_score(&self) -> f64 {
        let mut pressure_score = 0.0;

        // Cache utilization pressure
        if self.cache_utilization_percentage > 90.0 {
            pressure_score += (self.cache_utilization_percentage - 90.0) * 2.0;
        }

        // Application threads waiting adds significant pressure
        pressure_score += self.cache_application_threads_waiting as f64 * 10.0;

        // High eviction rate indicates pressure
        if self.cache_evictions > Self::HIGH_EVICTION_THRESHOLD {
            pressure_score += 20.0;
        }

        pressure_score.min(100.0)
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_wiredtiger_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let wiredtiger_info = MongoWiredTigerInfo::default();

        let result = wiredtiger_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.cache_utilization_percentage >= 0.0);
    }

    #[tokio::test]
    async fn test_wiredtiger_health_calculations() {
        let info = MongoWiredTigerInfo {
            cache_size_bytes: 1024 * 1024 * 1024,
            cache_used_bytes: 512 * 1024 * 1024,
            cache_utilization_percentage: 50.0,
            cache_hit_ratio_percentage: 95.0,
            cache_application_threads_waiting: 0,
            cache_evictions: 100,
            max_checkpoint_duration_ms: 5000.0,
            avg_checkpoint_duration_ms: 2500.0,
            transactions_started: 1000,
            transactions_committed: 980,
            transactions_rolled_back: 20,
            files_open: 40,
            max_files_open: 100,
            compression_ratio: 0.6,
            compressed_bytes: 600 * 1024 * 1024,
            uncompressed_bytes: 1000 * 1024 * 1024,
            ..MongoWiredTigerInfo::default()
        };

        assert!(info.is_cache_healthy());
        assert!(info.is_checkpoint_performance_healthy());
        assert!(!info.has_cache_pressure());
        assert!(info.is_io_performance_healthy());
        assert!(info.is_transaction_processing_healthy());
        assert!(info.is_file_handle_usage_healthy());
        assert!(!info.needs_immediate_attention());

        assert_eq!(info.transaction_success_rate_percentage(), 98.0);
        assert_eq!(info.storage_efficiency_percentage(), 40.0);
        assert!(info.cache_efficiency_score() > 70.0); // (50+95)/2 = 72.5
        assert!(info.overall_health_score() > 90.0);
        assert_eq!(info.cache_pressure_level(), "None");
    }

    #[tokio::test]
    async fn test_wiredtiger_performance_issues() {
        let info = MongoWiredTigerInfo {
            cache_utilization_percentage: 98.0,
            cache_hit_ratio_percentage: 70.0,
            cache_application_threads_waiting: 15,
            cache_evictions: 2000,
            max_checkpoint_duration_ms: 15000.0,
            transactions_started: 1000,
            transactions_committed: 850,
            transactions_rolled_back: 150,
            compression_ratio: 0.95,
            bytes_read_from_disk: 1_000_000,
            bytes_written_to_disk: 1_000_000,
            ..MongoWiredTigerInfo::default()
        };

        assert!(!info.is_cache_healthy());
        assert!(!info.is_checkpoint_performance_healthy());
        assert!(info.has_cache_pressure());
        assert!(!info.is_io_performance_healthy());
        assert!(!info.is_transaction_processing_healthy());
        assert!(info.needs_immediate_attention());

        assert_eq!(info.transaction_success_rate_percentage(), 85.0);
        assert!(info.cache_efficiency_score() < 60.0); // (60+70)/2 - 10 = 55
        assert!(info.overall_health_score() < 70.0);
        assert_eq!(info.cache_pressure_level(), "Critical");
        assert!(info.memory_pressure_score() > 50.0);

        let recommendations = info.performance_recommendations();
        assert!(recommendations.len() > 3);
        assert!(recommendations.iter().any(|r| r.contains("cache size")));
        assert!(recommendations.iter().any(|r| r.contains("checkpoint")));
    }

    #[tokio::test]
    async fn test_resource_utilization_summary() {
        let info = MongoWiredTigerInfo {
            cache_utilization_percentage: 75.0,
            cache_hit_ratio_percentage: 92.0,
            compressed_bytes: 700 * 1024 * 1024,
            uncompressed_bytes: 1000 * 1024 * 1024,
            transactions_started: 500,
            transactions_committed: 485,
            files_open: 30,
            max_files_open: 100,
            ..MongoWiredTigerInfo::default()
        };

        let summary = info.resource_utilization_summary();
        assert_eq!(summary.len(), 5);

        // Check cache utilization
        assert_eq!(summary[0].0, "Cache Utilization");
        assert_eq!(summary[0].1, 75.0);

        // Check storage efficiency
        assert_eq!(summary[2].0, "Storage Efficiency");
        assert_eq!(summary[2].1, 30.0); // (1000-700)/1000 * 100
    }
}
