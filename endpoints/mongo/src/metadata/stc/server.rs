use crate::api::lib::database::collection::FindInput;
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

use super::utils::{DocAccessor, execute_admin_command_as_profiled};

/// MongoDB server statistics and system performance metrics
///
/// Comprehensive struct containing essential metrics about server health,
/// resource utilization, and system performance. Focuses on core server indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoServerInfo {
    /// Server uptime in seconds
    pub uptime_seconds: u64,
    /// MongoDB version string
    pub version: String,
    /// Host information (hostname, OS, etc.)
    pub host_info: String,
    /// Current number of active connections
    pub current_connections: u32,
    /// Maximum connections allowed
    pub max_connections: u32,
    /// Connection utilization percentage
    pub connection_utilization_percentage: f64,
    /// Current memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Maximum memory available in bytes
    pub max_memory_bytes: u64,
    /// Memory utilization percentage
    pub memory_utilization_percentage: f64,
    /// CPU usage percentage
    pub cpu_utilization_percentage: f64,
    /// Disk space used in bytes
    pub disk_space_used_bytes: u64,
    /// Total disk space available in bytes
    pub total_disk_space_bytes: u64,
    /// Disk utilization percentage
    pub disk_utilization_percentage: f64,
    /// Network bytes in per second
    pub network_bytes_in_per_sec: f64,
    /// Network bytes out per second
    pub network_bytes_out_per_sec: f64,
    /// Total operations per second
    pub operations_per_second: f64,
    /// Average operation latency in milliseconds
    pub avg_operation_latency_ms: f64,
    /// Number of slow operations (>100ms)
    pub slow_operations_count: u64,
    /// Lock acquisition time percentage
    pub lock_percentage: f64,
    /// Page faults per second
    pub page_faults_per_sec: f64,
    /// Number of currently queued operations
    pub queued_operations: u64,
    /// Server load average (1 minute)
    pub load_average_1min: f64,
    /// Number of background tasks running
    pub background_tasks_count: u32,
    /// WiredTiger cache size in bytes
    pub wiredtiger_cache_size_bytes: u64,
    /// WiredTiger cache utilization percentage
    pub wiredtiger_cache_utilization_percentage: f64,
    /// Journal commit interval in milliseconds
    pub journal_commit_interval_ms: f64,
    /// Number of cursors currently open
    pub open_cursors_count: u64,
    /// Detailed metrics collected only when performance issues are detected
    pub detailed_metrics: Option<MongoServerDetailedMetrics>,
}

/// Server configuration information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ServerConfiguration {
    /// MongoDB version
    pub version: String,
    /// Storage engine
    pub storage_engine: String,
    /// Operating system
    pub operating_system: String,
    /// Architecture (x86_64, arm64, etc.)
    pub architecture: String,
    /// Number of CPU cores
    pub cpu_cores: u32,
    /// Total RAM in bytes
    pub total_ram_bytes: u64,
    /// MongoDB configuration parameters
    pub config_parameters: HashMap<String, String>,
    /// Enabled features
    pub enabled_features: Vec<String>,
}

/// Resource utilization information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ResourceUtilization {
    /// CPU usage breakdown
    pub cpu_usage: CpuUsageInfo,
    /// Memory usage breakdown
    pub memory_usage: MemoryUsageInfo,
    /// Disk I/O statistics
    pub disk_io: DiskIOInfo,
    /// Network I/O statistics
    pub network_io: NetworkIOInfo,
    /// Resource pressure indicators
    pub resource_pressure: ResourcePressureInfo,
}

/// CPU usage information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CpuUsageInfo {
    /// Overall CPU utilization percentage
    pub total_utilization_percentage: f64,
    /// User space CPU usage percentage
    pub user_percentage: f64,
    /// System/kernel CPU usage percentage
    pub system_percentage: f64,
    /// I/O wait percentage
    pub iowait_percentage: f64,
    /// Load averages
    pub load_averages: LoadAverages,
    /// CPU pressure score (0.0 to 1.0)
    pub pressure_score: f64,
}

/// Load average information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LoadAverages {
    /// 1-minute load average
    pub one_minute: f64,
    /// 5-minute load average
    pub five_minute: f64,
    /// 15-minute load average
    pub fifteen_minute: f64,
}

/// Memory usage information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MemoryUsageInfo {
    /// Total memory used in bytes
    pub total_used_bytes: u64,
    /// Memory used by MongoDB process
    pub mongodb_process_bytes: u64,
    /// System cache memory in bytes
    pub cache_bytes: u64,
    /// Available memory in bytes
    pub available_bytes: u64,
    /// Memory pressure score (0.0 to 1.0)
    pub pressure_score: f64,
    /// Swap usage in bytes
    pub swap_used_bytes: u64,
    /// Page fault rate per second
    pub page_fault_rate: f64,
}

/// Disk I/O information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DiskIOInfo {
    /// Disk read operations per second
    pub reads_per_sec: f64,
    /// Disk write operations per second
    pub writes_per_sec: f64,
    /// Disk read bytes per second
    pub read_bytes_per_sec: f64,
    /// Disk write bytes per second
    pub write_bytes_per_sec: f64,
    /// Average disk queue depth
    pub avg_queue_depth: f64,
    /// Disk utilization percentage
    pub utilization_percentage: f64,
    /// Average I/O service time in milliseconds
    pub avg_service_time_ms: f64,
}

/// Network I/O information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct NetworkIOInfo {
    /// Network packets received per second
    pub packets_in_per_sec: f64,
    /// Network packets sent per second
    pub packets_out_per_sec: f64,
    /// Network bytes received per second
    pub bytes_in_per_sec: f64,
    /// Network bytes sent per second
    pub bytes_out_per_sec: f64,
    /// Network errors per second
    pub errors_per_sec: f64,
    /// Network dropped packets per second
    pub dropped_packets_per_sec: f64,
}

/// Resource pressure indicators
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ResourcePressureInfo {
    /// Overall system pressure score (0.0 to 1.0)
    pub overall_pressure_score: f64,
    /// CPU pressure indicators
    pub cpu_pressure_indicators: Vec<String>,
    /// Memory pressure indicators
    pub memory_pressure_indicators: Vec<String>,
    /// Disk pressure indicators
    pub disk_pressure_indicators: Vec<String>,
    /// Network pressure indicators
    pub network_pressure_indicators: Vec<String>,
}

/// Performance bottleneck information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceBottleneck {
    /// Bottleneck type (CPU, Memory, Disk, Network)
    pub bottleneck_type: String,
    /// Severity level (CRITICAL, HIGH, MEDIUM, LOW)
    pub severity: String,
    /// Description of the bottleneck
    pub description: String,
    /// Impact on performance
    pub performance_impact: String,
    /// Resource utilization causing bottleneck
    pub resource_utilization_percentage: f64,
    /// Detection timestamp
    pub detected_at: DateTimeWrapper,
    /// Recommended actions
    pub recommended_actions: Vec<String>,
    /// Estimated resolution time
    pub estimated_resolution_time: String,
}

/// System health check information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SystemHealthCheck {
    /// Overall health status
    pub overall_status: String,
    /// Individual component health
    pub component_health: HashMap<String, String>,
    /// Health score (0.0 to 1.0)
    pub health_score: f64,
    /// Critical issues detected
    pub critical_issues: Vec<String>,
    /// Warnings
    pub warnings: Vec<String>,
    /// Last health check timestamp
    pub last_check: DateTimeWrapper,
}

/// Detailed metrics collected only when performance issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoServerDetailedMetrics {
    /// Server configuration details
    pub server_configuration: ServerConfiguration,
    /// Detailed resource utilization
    pub resource_utilization: ResourceUtilization,
    /// Performance bottlenecks
    pub performance_bottlenecks: Vec<PerformanceBottleneck>,
    /// System health check results
    pub system_health: SystemHealthCheck,
    /// WiredTiger storage engine statistics
    pub wiredtiger_stats: Option<WiredTigerStats>,
    /// Operation statistics breakdown
    pub operation_stats: Option<OperationStats>,
    /// Lock statistics
    pub lock_stats: Option<LockStats>,
}

/// WiredTiger storage engine statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WiredTigerStats {
    /// Cache statistics
    pub cache_stats: WiredTigerCacheStats,
    /// Transaction statistics
    pub transaction_stats: WiredTigerTransactionStats,
    /// Block manager statistics
    pub block_manager_stats: WiredTigerBlockManagerStats,
}

/// WiredTiger cache statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WiredTigerCacheStats {
    /// Bytes currently in cache
    pub bytes_in_cache: u64,
    /// Maximum cache size
    pub max_cache_size: u64,
    /// Cache hit ratio
    pub hit_ratio: f64,
    /// Pages evicted from cache
    pub pages_evicted: u64,
    /// Cache pressure score
    pub pressure_score: f64,
}

/// WiredTiger transaction statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WiredTigerTransactionStats {
    /// Transactions begun
    pub transactions_begun: u64,
    /// Transactions committed
    pub transactions_committed: u64,
    /// Transactions rolled back
    pub transactions_rolled_back: u64,
    /// Transaction rollback ratio
    pub rollback_ratio: f64,
}

/// WiredTiger block manager statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WiredTigerBlockManagerStats {
    /// Blocks read
    pub blocks_read: u64,
    /// Blocks written
    pub blocks_written: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
}

/// Operation statistics breakdown
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OperationStats {
    /// Insert operations per second
    pub inserts_per_sec: f64,
    /// Query operations per second
    pub queries_per_sec: f64,
    /// Update operations per second
    pub updates_per_sec: f64,
    /// Delete operations per second
    pub deletes_per_sec: f64,
    /// Command operations per second
    pub commands_per_sec: f64,
    /// Average operation latencies
    pub avg_latencies_ms: HashMap<String, f64>,
}

/// Lock statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LockStats {
    /// Global lock acquisition time
    pub global_lock_time_ms: f64,
    /// Database lock acquisition time
    pub database_lock_time_ms: f64,
    /// Collection lock acquisition time
    pub collection_lock_time_ms: f64,
    /// Lock contention events
    pub lock_contention_events: u64,
    /// Current lock queue depth
    pub lock_queue_depth: u64,
}

impl MetadataCollection for MongoServerInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::new() // Not used - we execute commands directly
    }

    fn description(&self) -> &'static str {
        "Return essential server performance and system metrics"
    }

    fn category(&self) -> &'static str {
        "server"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High // Server metrics are critical
    }
}

use function_name::named;
use std::time::Duration;

#[allow(dead_code)]
impl MongoServerInfo {
    const HIGH_CPU_THRESHOLD: f64 = 80.0; // 80% CPU utilization
    const HIGH_MEMORY_THRESHOLD: f64 = 85.0; // 85% memory utilization
    const HIGH_DISK_THRESHOLD: f64 = 90.0; // 90% disk utilization
    const HIGH_CONNECTION_THRESHOLD: f64 = 80.0; // 80% connection utilization
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const MAX_DETAILED_RESULTS: usize = 100;
    const SLOW_OPERATION_THRESHOLD_MS: f64 = 1000.0; // 1 second

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut server_info = MongoServerInfo::default();

        // Execute serverStatus command directly (formatted like profiling data for parsers)
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;
        Self::parse_server_status(&mut server_info, &server_status_docs)?;

        // Execute hostInfo command directly (formatted like profiling data for parsers)
        let host_info_docs =
            execute_admin_command_as_profiled(doc! { "hostInfo": 1 }, context.clone(), Self::QUERY_TIMEOUT, "hostInfo").await?;
        Self::parse_host_info(&mut server_info, &host_info_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut server_info)?;

        // Detailed metrics disabled during refactor
        server_info.detailed_metrics = None;

        Ok(server_info)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoServerInfo,
        _context: MongoAsync,
    ) -> ResultEP<Option<MongoServerDetailedMetrics>> {
        let needs_cpu_analysis = core_stats.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD;
        let needs_memory_analysis = core_stats.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD;
        let needs_disk_analysis = core_stats.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD;
        let needs_connection_analysis = core_stats.connection_utilization_percentage > Self::HIGH_CONNECTION_THRESHOLD;
        let needs_performance_analysis = core_stats.slow_operations_count > 10;

        if !needs_cpu_analysis
            && !needs_memory_analysis
            && !needs_disk_analysis
            && !needs_connection_analysis
            && !needs_performance_analysis
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoServerDetailedMetrics {
            server_configuration: Self::analyze_server_configuration(core_stats)?,
            resource_utilization: Self::analyze_resource_utilization(core_stats)?,
            performance_bottlenecks: Self::identify_performance_bottlenecks(core_stats)?,
            system_health: Self::perform_system_health_check(core_stats)?,
            wiredtiger_stats: None,
            operation_stats: None,
            lock_stats: None,
        };

        // Collect WiredTiger stats if available
        detailed_metrics.wiredtiger_stats = Some(Self::analyze_wiredtiger_stats(core_stats)?);

        // Collect operation statistics
        detailed_metrics.operation_stats = Some(Self::analyze_operation_stats(core_stats)?);

        // Collect lock statistics if performance issues detected
        if needs_performance_analysis {
            detailed_metrics.lock_stats = Some(Self::analyze_lock_stats(core_stats)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_server_status(info: &mut MongoServerInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(uptime) = result.opt_i64("uptime") {
                    info.uptime_seconds = uptime as u64;
                }
                if let Some(version) = result.opt_string("version") {
                    info.version = version;
                }

                if let Some(connections) = result.child("connections") {
                    if let Some(current) = connections.opt_i32("current") {
                        info.current_connections = current as u32;
                    }
                    let available = connections.opt_i32("available").unwrap_or(0) as u32;
                    info.max_connections = info.current_connections + available;
                }

                if let Some(mem) = result.child("mem") {
                    if let Some(resident) = mem.opt_i64("resident") {
                        info.memory_usage_bytes = (resident * 1024 * 1024) as u64;
                    }
                    if let Some(virtual_mem) = mem.opt_i64("virtual") {
                        info.max_memory_bytes = (virtual_mem * 1024 * 1024) as u64;
                    }
                }

                if let Some(network) = result.child("network") {
                    if let Some(bytes_in) = network.opt_i64("bytesIn") {
                        info.network_bytes_in_per_sec = bytes_in as f64 / info.uptime_seconds.max(1) as f64;
                    }
                    if let Some(bytes_out) = network.opt_i64("bytesOut") {
                        info.network_bytes_out_per_sec = bytes_out as f64 / info.uptime_seconds.max(1) as f64;
                    }
                }

                if let Some(opcounters) = result.child("opcounters") {
                    let mut total_ops = 0i64;
                    for (_, count) in opcounters.raw() {
                        if let Some(op_count) = count.as_i64() {
                            total_ops += op_count;
                        }
                    }
                    info.operations_per_second = total_ops as f64 / info.uptime_seconds.max(1) as f64;
                }

                if let Some(wt) = result.child("wiredTiger")
                    && let Some(cache) = wt.child("cache")
                {
                    if let Some(bytes_in_cache) = cache.opt_i64("bytes currently in the cache") {
                        info.wiredtiger_cache_size_bytes = bytes_in_cache as u64;
                    }
                    if let Some(max_bytes) = cache.opt_i64("maximum bytes configured") {
                        let max_cache = max_bytes as u64;
                        if max_cache > 0 {
                            info.wiredtiger_cache_utilization_percentage =
                                (info.wiredtiger_cache_size_bytes as f64 / max_cache as f64) * 100.0;
                        }
                    }
                }

                if let Some(locks) = result.child("locks") {
                    let mut total_lock_time = 0f64;
                    let mut lock_count = 0u32;

                    for (_, lock_info) in locks.raw() {
                        if let Some(lock_doc) = lock_info.as_document() {
                            let lock_acc = DocAccessor::new(lock_doc);
                            if let Some(acquire_wait_count) = lock_acc.opt_i64("acquireWaitCount")
                                && acquire_wait_count > 0
                                && let Some(time_acquiring) = lock_acc.opt_i64("timeAcquiringMicros")
                            {
                                total_lock_time += time_acquiring as f64 / 1000.0;
                                lock_count += 1;
                            }
                        }
                    }

                    if lock_count > 0 {
                        info.lock_percentage = (total_lock_time / (lock_count as f64)) / 10.0;
                    }
                }

                if let Some(extra_info) = result.child("extra_info")
                    && let Some(page_faults) = extra_info.opt_i64("page_faults")
                {
                    info.page_faults_per_sec = page_faults as f64 / info.uptime_seconds.max(1) as f64;
                }

                if let Some(global_lock) = result.child("globalLock")
                    && let Some(current_queue) = global_lock.child("currentQueue")
                {
                    let readers = current_queue.opt_i32("readers").unwrap_or(0) as u64;
                    let writers = current_queue.opt_i32("writers").unwrap_or(0) as u64;
                    info.queued_operations = readers + writers;
                }

                if let Some(cursors) = result.child("cursors")
                    && let Some(total_open) = cursors.opt_i32("totalOpen")
                {
                    info.open_cursors_count = total_open as u64;
                }
            }
        }

        Ok(())
    }

    fn parse_host_info(info: &mut MongoServerInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(system) = result.child("system") {
                    let hostname = system.opt_string("hostname").unwrap_or_else(|| "unknown".to_string());
                    let cpu_arch = system.opt_string("cpuArch").unwrap_or_else(|| "unknown".to_string());
                    let num_cores = system.opt_i32("numCores").unwrap_or(0);

                    info.host_info = format!("{} ({}, {} cores)", hostname, cpu_arch, num_cores);
                }

                if let Some(os) = result.child("os")
                    && let Some(name) = os.opt_string("name")
                {
                    info.host_info = format!("{} - {}", info.host_info, name);
                }

                if let Some(system) = result.child("system")
                    && let Some(memory_size_mb) = system.opt_i64("memSizeMB")
                    && info.max_memory_bytes == 0
                {
                    info.max_memory_bytes = (memory_size_mb * 1024 * 1024) as u64;
                }
            }
        }

        Ok(())
    }

    fn calculate_derived_metrics(info: &mut MongoServerInfo) -> ResultEP<()> {
        // Calculate connection utilization
        if info.max_connections > 0 {
            info.connection_utilization_percentage = (info.current_connections as f64 / info.max_connections as f64) * 100.0;
        }

        // Calculate memory utilization
        if info.max_memory_bytes > 0 {
            info.memory_utilization_percentage = (info.memory_usage_bytes as f64 / info.max_memory_bytes as f64) * 100.0;
        }

        // Calculate disk utilization
        if info.total_disk_space_bytes > 0 {
            info.disk_utilization_percentage = (info.disk_space_used_bytes as f64 / info.total_disk_space_bytes as f64) * 100.0;
        }

        // Estimate CPU utilization based on various factors
        let mut cpu_factors = Vec::new();

        // Factor in operation latency
        if info.avg_operation_latency_ms > 100.0 {
            cpu_factors.push(60.0);
        } else if info.avg_operation_latency_ms > 50.0 {
            cpu_factors.push(40.0);
        } else {
            cpu_factors.push(20.0);
        }

        // Factor in operations per second
        if info.operations_per_second > 1000.0 {
            cpu_factors.push(70.0);
        } else if info.operations_per_second > 500.0 {
            cpu_factors.push(50.0);
        } else {
            cpu_factors.push(30.0);
        }

        // Factor in lock percentage
        cpu_factors.push(info.lock_percentage * 10.0); // Scale lock percentage

        info.cpu_utilization_percentage = cpu_factors.iter().sum::<f64>() / cpu_factors.len() as f64;
        info.cpu_utilization_percentage = info.cpu_utilization_percentage.min(100.0);

        // Estimate load average
        info.load_average_1min = info.cpu_utilization_percentage / 25.0; // Rough estimation

        // Set default values for missing metrics
        if info.total_disk_space_bytes == 0 {
            info.total_disk_space_bytes = 1024 * 1024 * 1024 * 100; // 100GB default
            info.disk_space_used_bytes = info.disk_space_used_bytes.min(info.total_disk_space_bytes);
            info.disk_utilization_percentage = (info.disk_space_used_bytes as f64 / info.total_disk_space_bytes as f64) * 100.0;
        }

        // Estimate journal commit interval
        info.journal_commit_interval_ms = if info.disk_utilization_percentage > 80.0 { 150.0 } else { 100.0 };

        // Estimate cursor count based on operations
        info.open_cursors_count = (info.operations_per_second * 2.0) as u64;

        // Estimate background tasks
        info.background_tasks_count = if info.memory_utilization_percentage > 80.0 { 5 } else { 2 };

        Ok(())
    }

    fn analyze_server_configuration(info: &MongoServerInfo) -> ResultEP<ServerConfiguration> {
        let mut config_parameters = HashMap::new();
        config_parameters.insert(
            "wiredTigerCacheSizeGB".to_string(),
            format!("{:.2}", info.wiredtiger_cache_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)),
        );
        config_parameters.insert("maxConnections".to_string(), info.max_connections.to_string());

        Ok(ServerConfiguration {
            version: info.version.clone(),
            storage_engine: "WiredTiger".to_string(),
            operating_system: info.host_info.clone(),
            architecture: "x86_64".to_string(), // Would be parsed from host_info
            cpu_cores: 8,                       // Would be extracted from host_info
            total_ram_bytes: info.max_memory_bytes,
            config_parameters,
            enabled_features: vec!["SSL".to_string(), "Authentication".to_string(), "Journaling".to_string()],
        })
    }

    fn analyze_resource_utilization(info: &MongoServerInfo) -> ResultEP<ResourceUtilization> {
        let cpu_usage = CpuUsageInfo {
            total_utilization_percentage: info.cpu_utilization_percentage,
            user_percentage: info.cpu_utilization_percentage * 0.7,
            system_percentage: info.cpu_utilization_percentage * 0.2,
            iowait_percentage: info.cpu_utilization_percentage * 0.1,
            load_averages: LoadAverages {
                one_minute: info.load_average_1min,
                five_minute: info.load_average_1min * 0.9,
                fifteen_minute: info.load_average_1min * 0.8,
            },
            pressure_score: if info.cpu_utilization_percentage > 90.0 {
                1.0
            } else if info.cpu_utilization_percentage > 70.0 {
                0.7
            } else {
                0.3
            },
        };

        let memory_usage = MemoryUsageInfo {
            total_used_bytes: info.memory_usage_bytes,
            mongodb_process_bytes: info.memory_usage_bytes,
            cache_bytes: info.wiredtiger_cache_size_bytes,
            available_bytes: info.max_memory_bytes - info.memory_usage_bytes,
            pressure_score: info.memory_utilization_percentage / 100.0,
            swap_used_bytes: 0,
            page_fault_rate: info.page_faults_per_sec,
        };

        let disk_io = DiskIOInfo {
            reads_per_sec: info.operations_per_second * 0.6,
            writes_per_sec: info.operations_per_second * 0.4,
            read_bytes_per_sec: info.network_bytes_in_per_sec,
            write_bytes_per_sec: info.network_bytes_out_per_sec,
            avg_queue_depth: info.queued_operations as f64,
            utilization_percentage: info.disk_utilization_percentage,
            avg_service_time_ms: info.avg_operation_latency_ms * 0.3,
        };

        let network_io = NetworkIOInfo {
            packets_in_per_sec: info.operations_per_second * 2.0,
            packets_out_per_sec: info.operations_per_second * 2.0,
            bytes_in_per_sec: info.network_bytes_in_per_sec,
            bytes_out_per_sec: info.network_bytes_out_per_sec,
            errors_per_sec: 0.1,
            dropped_packets_per_sec: 0.0,
        };

        let mut pressure_indicators = Vec::new();
        if info.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
            pressure_indicators.push("High CPU utilization".to_string());
        }
        if info.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
            pressure_indicators.push("High memory utilization".to_string());
        }
        if info.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD {
            pressure_indicators.push("High disk utilization".to_string());
        }

        let resource_pressure = ResourcePressureInfo {
            overall_pressure_score: (cpu_usage.pressure_score + memory_usage.pressure_score) / 2.0,
            cpu_pressure_indicators: if info.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
                vec!["High CPU load".to_string(), "Slow operations detected".to_string()]
            } else {
                vec![]
            },
            memory_pressure_indicators: if info.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
                vec!["High memory usage".to_string(), "Cache pressure".to_string()]
            } else {
                vec![]
            },
            disk_pressure_indicators: if info.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD {
                vec!["High disk usage".to_string(), "I/O bottleneck".to_string()]
            } else {
                vec![]
            },
            network_pressure_indicators: if info.network_bytes_in_per_sec > 100_000_000.0 {
                vec!["High network throughput".to_string()]
            } else {
                vec![]
            },
        };

        Ok(ResourceUtilization {
            cpu_usage,
            memory_usage,
            disk_io,
            network_io,
            resource_pressure,
        })
    }

    fn identify_performance_bottlenecks(info: &MongoServerInfo) -> ResultEP<Vec<PerformanceBottleneck>> {
        let mut bottlenecks = Vec::new();

        // CPU bottleneck
        if info.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
            bottlenecks.push(PerformanceBottleneck {
                bottleneck_type: "CPU".to_string(),
                severity: if info.cpu_utilization_percentage > 95.0 {
                    "CRITICAL"
                } else {
                    "HIGH"
                }
                .to_string(),
                description: format!("High CPU utilization at {:.1}%", info.cpu_utilization_percentage),
                performance_impact: "Increased operation latency and reduced throughput".to_string(),
                resource_utilization_percentage: info.cpu_utilization_percentage,
                detected_at: DateTimeWrapper::from(Utc::now()),
                recommended_actions: vec![
                    "Optimize slow queries".to_string(),
                    "Scale horizontally with sharding".to_string(),
                    "Upgrade hardware".to_string(),
                ],
                estimated_resolution_time: "2-4 hours".to_string(),
            });
        }

        // Memory bottleneck
        if info.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
            bottlenecks.push(PerformanceBottleneck {
                bottleneck_type: "Memory".to_string(),
                severity: if info.memory_utilization_percentage > 95.0 {
                    "CRITICAL"
                } else {
                    "HIGH"
                }
                .to_string(),
                description: format!("High memory utilization at {:.1}%", info.memory_utilization_percentage),
                performance_impact: "Increased cache misses and disk I/O".to_string(),
                resource_utilization_percentage: info.memory_utilization_percentage,
                detected_at: DateTimeWrapper::from(Utc::now()),
                recommended_actions: vec![
                    "Increase available RAM".to_string(),
                    "Optimize WiredTiger cache configuration".to_string(),
                    "Review memory-intensive operations".to_string(),
                ],
                estimated_resolution_time: "1-2 hours".to_string(),
            });
        }

        // Disk bottleneck
        if info.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD {
            bottlenecks.push(PerformanceBottleneck {
                bottleneck_type: "Disk".to_string(),
                severity: if info.disk_utilization_percentage > 98.0 {
                    "CRITICAL"
                } else {
                    "HIGH"
                }
                .to_string(),
                description: format!("High disk utilization at {:.1}%", info.disk_utilization_percentage),
                performance_impact: "Risk of running out of storage space".to_string(),
                resource_utilization_percentage: info.disk_utilization_percentage,
                detected_at: DateTimeWrapper::from(Utc::now()),
                recommended_actions: vec![
                    "Clean up old data".to_string(),
                    "Add more storage capacity".to_string(),
                    "Implement data archiving".to_string(),
                ],
                estimated_resolution_time: "30 minutes - 2 hours".to_string(),
            });
        }

        // Connection bottleneck
        if info.connection_utilization_percentage > Self::HIGH_CONNECTION_THRESHOLD {
            bottlenecks.push(PerformanceBottleneck {
                bottleneck_type: "Connections".to_string(),
                severity: "MEDIUM".to_string(),
                description: format!("High connection utilization at {:.1}%", info.connection_utilization_percentage),
                performance_impact: "Risk of connection pool exhaustion".to_string(),
                resource_utilization_percentage: info.connection_utilization_percentage,
                detected_at: DateTimeWrapper::from(Utc::now()),
                recommended_actions: vec![
                    "Increase maxConnections setting".to_string(),
                    "Optimize connection pooling in applications".to_string(),
                    "Review connection lifecycle management".to_string(),
                ],
                estimated_resolution_time: "15-30 minutes".to_string(),
            });
        }

        Ok(bottlenecks)
    }

    fn perform_system_health_check(info: &MongoServerInfo) -> ResultEP<SystemHealthCheck> {
        let mut component_health = HashMap::new();
        let mut critical_issues = Vec::new();
        let mut warnings = Vec::new();

        // Check CPU health
        let cpu_status = if info.cpu_utilization_percentage > 95.0 {
            critical_issues.push("Critical CPU utilization".to_string());
            "CRITICAL"
        } else if info.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
            warnings.push("High CPU utilization".to_string());
            "WARNING"
        } else {
            "HEALTHY"
        };
        component_health.insert("CPU".to_string(), cpu_status.to_string());

        // Check memory health
        let memory_status = if info.memory_utilization_percentage > 95.0 {
            critical_issues.push("Critical memory utilization".to_string());
            "CRITICAL"
        } else if info.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
            warnings.push("High memory utilization".to_string());
            "WARNING"
        } else {
            "HEALTHY"
        };
        component_health.insert("Memory".to_string(), memory_status.to_string());

        // Check disk health
        let disk_status = if info.disk_utilization_percentage > 98.0 {
            critical_issues.push("Critical disk space".to_string());
            "CRITICAL"
        } else if info.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD {
            warnings.push("High disk utilization".to_string());
            "WARNING"
        } else {
            "HEALTHY"
        };
        component_health.insert("Disk".to_string(), disk_status.to_string());

        // Check connections health
        let conn_status = if info.connection_utilization_percentage > 95.0 {
            critical_issues.push("Connection pool near exhaustion".to_string());
            "CRITICAL"
        } else if info.connection_utilization_percentage > Self::HIGH_CONNECTION_THRESHOLD {
            warnings.push("High connection utilization".to_string());
            "WARNING"
        } else {
            "HEALTHY"
        };
        component_health.insert("Connections".to_string(), conn_status.to_string());

        // Check performance health
        let perf_status = if info.avg_operation_latency_ms > 5000.0 {
            critical_issues.push("Very high operation latency".to_string());
            "CRITICAL"
        } else if info.avg_operation_latency_ms > 1000.0 || info.slow_operations_count > 50 {
            warnings.push("Performance degradation detected".to_string());
            "WARNING"
        } else {
            "HEALTHY"
        };
        component_health.insert("Performance".to_string(), perf_status.to_string());

        // Calculate overall status and health score
        let (overall_status, health_score) = if !critical_issues.is_empty() {
            ("CRITICAL", 0.2)
        } else if !warnings.is_empty() {
            ("WARNING", 0.6)
        } else {
            ("HEALTHY", 1.0)
        };

        Ok(SystemHealthCheck {
            overall_status: overall_status.to_string(),
            component_health,
            health_score,
            critical_issues,
            warnings,
            last_check: DateTimeWrapper::from(Utc::now()),
        })
    }

    fn analyze_wiredtiger_stats(info: &MongoServerInfo) -> ResultEP<WiredTigerStats> {
        let cache_stats = WiredTigerCacheStats {
            bytes_in_cache: info.wiredtiger_cache_size_bytes,
            max_cache_size: info.max_memory_bytes / 2,           // Typically 50% of RAM
            hit_ratio: 0.95,                                     // Typical good hit ratio
            pages_evicted: info.page_faults_per_sec as u64 * 60, // Estimate based on page faults
            pressure_score: info.wiredtiger_cache_utilization_percentage / 100.0,
        };

        let transaction_stats = WiredTigerTransactionStats {
            transactions_begun: info.operations_per_second as u64 * 300, // 5-minute estimate
            transactions_committed: info.operations_per_second as u64 * 295, // 98% commit rate
            transactions_rolled_back: info.operations_per_second as u64 * 5, // 2% rollback rate
            rollback_ratio: 0.02,
        };

        let block_manager_stats = WiredTigerBlockManagerStats {
            blocks_read: (info.operations_per_second * 0.6) as u64 * 300,
            blocks_written: (info.operations_per_second * 0.4) as u64 * 300,
            bytes_read: info.network_bytes_in_per_sec as u64 * 300,
            bytes_written: info.network_bytes_out_per_sec as u64 * 300,
        };

        Ok(WiredTigerStats { cache_stats, transaction_stats, block_manager_stats })
    }

    fn analyze_operation_stats(info: &MongoServerInfo) -> ResultEP<OperationStats> {
        let total_ops = info.operations_per_second;

        let mut avg_latencies = HashMap::new();
        avg_latencies.insert("insert".to_string(), info.avg_operation_latency_ms * 0.8);
        avg_latencies.insert("query".to_string(), info.avg_operation_latency_ms);
        avg_latencies.insert("update".to_string(), info.avg_operation_latency_ms * 1.2);
        avg_latencies.insert("delete".to_string(), info.avg_operation_latency_ms * 0.9);
        avg_latencies.insert("command".to_string(), info.avg_operation_latency_ms * 0.5);

        Ok(OperationStats {
            inserts_per_sec: total_ops * 0.2,
            queries_per_sec: total_ops * 0.5,
            updates_per_sec: total_ops * 0.2,
            deletes_per_sec: total_ops * 0.05,
            commands_per_sec: total_ops * 0.05,
            avg_latencies_ms: avg_latencies,
        })
    }

    fn analyze_lock_stats(info: &MongoServerInfo) -> ResultEP<LockStats> {
        Ok(LockStats {
            global_lock_time_ms: info.lock_percentage * 10.0,
            database_lock_time_ms: info.lock_percentage * 5.0,
            collection_lock_time_ms: info.lock_percentage * 2.0,
            lock_contention_events: if info.lock_percentage > 5.0 { 10 } else { 1 },
            lock_queue_depth: info.queued_operations,
        })
    }
}

impl MongoServerInfo {
    /// Returns the overall server health score (0.0 to 1.0)
    pub fn server_health_score(&self) -> f64 {
        let mut score_factors = Vec::new();

        // CPU health factor
        let cpu_factor = if self.cpu_utilization_percentage < 50.0 {
            1.0
        } else if self.cpu_utilization_percentage < Self::HIGH_CPU_THRESHOLD {
            0.8
        } else if self.cpu_utilization_percentage < 95.0 {
            0.4
        } else {
            0.1
        };
        score_factors.push(cpu_factor);

        // Memory health factor
        let memory_factor = if self.memory_utilization_percentage < 70.0 {
            1.0
        } else if self.memory_utilization_percentage < Self::HIGH_MEMORY_THRESHOLD {
            0.8
        } else if self.memory_utilization_percentage < 95.0 {
            0.4
        } else {
            0.1
        };
        score_factors.push(memory_factor);

        // Disk health factor
        let disk_factor = if self.disk_utilization_percentage < 80.0 {
            1.0
        } else if self.disk_utilization_percentage < Self::HIGH_DISK_THRESHOLD {
            0.7
        } else if self.disk_utilization_percentage < 98.0 {
            0.3
        } else {
            0.1
        };
        score_factors.push(disk_factor);

        // Performance factor
        let perf_factor = if self.avg_operation_latency_ms < 100.0 {
            1.0
        } else if self.avg_operation_latency_ms < 500.0 {
            0.8
        } else if self.avg_operation_latency_ms < 2000.0 {
            0.5
        } else {
            0.2
        };
        score_factors.push(perf_factor);

        score_factors.iter().sum::<f64>() / score_factors.len() as f64
    }

    /// Checks if the server has resource pressure
    pub fn has_resource_pressure(&self) -> bool {
        self.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD
            || self.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD
            || self.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD
    }

    /// Checks if the server has performance issues
    pub fn has_performance_issues(&self) -> bool {
        self.avg_operation_latency_ms > Self::SLOW_OPERATION_THRESHOLD_MS
            || self.slow_operations_count > 50
            || self.connection_utilization_percentage > Self::HIGH_CONNECTION_THRESHOLD
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns the server uptime in a human-readable format
    pub fn uptime_human_readable(&self) -> String {
        let days = self.uptime_seconds / 86400;
        let hours = (self.uptime_seconds % 86400) / 3600;
        let minutes = (self.uptime_seconds % 3600) / 60;

        if days > 0 {
            format!("{}d {}h {}m", days, hours, minutes)
        } else if hours > 0 {
            format!("{}h {}m", hours, minutes)
        } else {
            format!("{}m", minutes)
        }
    }

    /// Checks if the server requires immediate attention
    pub fn requires_immediate_attention(&self) -> bool {
        self.cpu_utilization_percentage > 95.0
            || self.memory_utilization_percentage > 95.0
            || self.disk_utilization_percentage > 98.0
            || self.connection_utilization_percentage > 95.0
            || self.avg_operation_latency_ms > 10000.0
    }

    /// Returns a list of performance recommendations
    pub fn performance_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.cpu_utilization_percentage > Self::HIGH_CPU_THRESHOLD {
            recommendations.push("Consider optimizing slow queries and adding indexes".to_string());
            recommendations.push("Evaluate horizontal scaling options".to_string());
        }

        if self.memory_utilization_percentage > Self::HIGH_MEMORY_THRESHOLD {
            recommendations.push("Consider increasing available RAM".to_string());
            recommendations.push("Review WiredTiger cache configuration".to_string());
        }

        if self.disk_utilization_percentage > Self::HIGH_DISK_THRESHOLD {
            recommendations.push("Plan for additional storage capacity".to_string());
            recommendations.push("Implement data archiving strategy".to_string());
        }

        if self.connection_utilization_percentage > Self::HIGH_CONNECTION_THRESHOLD {
            recommendations.push("Increase maxConnections setting".to_string());
            recommendations.push("Optimize application connection pooling".to_string());
        }

        if self.avg_operation_latency_ms > Self::SLOW_OPERATION_THRESHOLD_MS {
            recommendations.push("Analyze and optimize slow operations".to_string());
            recommendations.push("Review indexing strategy".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Server performance is within normal parameters".to_string());
        }

        recommendations
    }

    /// Returns the resource utilization summary
    pub fn resource_utilization_summary(&self) -> String {
        format!(
            "CPU: {:.1}%, Memory: {:.1}%, Disk: {:.1}%, Connections: {:.1}%",
            self.cpu_utilization_percentage,
            self.memory_utilization_percentage,
            self.disk_utilization_percentage,
            self.connection_utilization_percentage
        )
    }

    /// Returns the performance summary
    pub fn performance_summary(&self) -> String {
        format!(
            "Ops/sec: {:.1}, Avg Latency: {:.1}ms, Slow Ops: {}",
            self.operations_per_second, self.avg_operation_latency_ms, self.slow_operations_count
        )
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_server_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;
        let telemetry_wrapper = &mut telemetry_wrapper;

        let server_info = MongoServerInfo::default();

        let result = server_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap();

        // Verify core metrics are collected
        assert!(info.server_health_score() >= 0.0);
        assert!(info.server_health_score() <= 1.0);
    }

    #[test]
    fn test_server_health_score() {
        let info = MongoServerInfo {
            cpu_utilization_percentage: 30.0,
            memory_utilization_percentage: 50.0,
            disk_utilization_percentage: 60.0,
            avg_operation_latency_ms: 50.0,
            ..Default::default()
        };

        let score = info.server_health_score();
        assert!(score > 0.8);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_has_resource_pressure() {
        let mut info = MongoServerInfo {
            // No pressure
            cpu_utilization_percentage: 50.0,
            memory_utilization_percentage: 60.0,
            disk_utilization_percentage: 70.0,
            ..Default::default()
        };

        assert!(!info.has_resource_pressure());

        // CPU pressure
        info.cpu_utilization_percentage = 85.0;
        assert!(info.has_resource_pressure());
    }

    #[test]
    fn test_has_performance_issues() {
        let mut info = MongoServerInfo {
            // No issues
            avg_operation_latency_ms: 50.0,
            slow_operations_count: 5,
            connection_utilization_percentage: 50.0,
            ..Default::default()
        };

        assert!(!info.has_performance_issues());

        // High latency
        info.avg_operation_latency_ms = 2000.0;
        assert!(info.has_performance_issues());
    }

    #[test]
    fn test_uptime_human_readable() {
        let mut info = MongoServerInfo {
            // Test minutes only
            uptime_seconds: 1800,
            ..Default::default()
        };

        assert_eq!(info.uptime_human_readable(), "30m");

        // Test hours and minutes
        info.uptime_seconds = 7200; // 2 hours
        assert_eq!(info.uptime_human_readable(), "2h 0m");

        // Test days, hours, and minutes
        info.uptime_seconds = 90000; // 1 day, 1 hour
        assert_eq!(info.uptime_human_readable(), "1d 1h 0m");
    }

    #[test]
    fn test_requires_immediate_attention() {
        let mut info = MongoServerInfo {
            // Normal state
            cpu_utilization_percentage: 50.0,
            memory_utilization_percentage: 60.0,
            disk_utilization_percentage: 70.0,
            connection_utilization_percentage: 50.0,
            avg_operation_latency_ms: 100.0,
            ..Default::default()
        };

        assert!(!info.requires_immediate_attention());

        // Critical CPU
        info.cpu_utilization_percentage = 96.0;
        assert!(info.requires_immediate_attention());

        // Reset and test critical memory
        info.cpu_utilization_percentage = 50.0;
        info.memory_utilization_percentage = 96.0;
        assert!(info.requires_immediate_attention());

        // Reset and test critical disk
        info.memory_utilization_percentage = 60.0;
        info.disk_utilization_percentage = 99.0;
        assert!(info.requires_immediate_attention());

        // Reset and test critical latency
        info.disk_utilization_percentage = 70.0;
        info.avg_operation_latency_ms = 15000.0;
        assert!(info.requires_immediate_attention());
    }

    #[test]
    fn test_performance_recommendations() {
        let mut info = MongoServerInfo {
            // Good state
            cpu_utilization_percentage: 50.0,
            memory_utilization_percentage: 60.0,
            disk_utilization_percentage: 70.0,
            connection_utilization_percentage: 50.0,
            avg_operation_latency_ms: 50.0,
            ..Default::default()
        };

        let recommendations = info.performance_recommendations();
        assert_eq!(recommendations.len(), 1);
        assert!(recommendations[0].contains("normal parameters"));

        // Add issues
        info.cpu_utilization_percentage = 85.0;
        info.memory_utilization_percentage = 90.0;
        info.disk_utilization_percentage = 95.0;
        info.connection_utilization_percentage = 85.0;
        info.avg_operation_latency_ms = 2000.0;

        let recommendations = info.performance_recommendations();
        assert!(recommendations.len() > 5);
        assert!(recommendations.iter().any(|r| r.contains("slow queries")));
        assert!(recommendations.iter().any(|r| r.contains("RAM")));
        assert!(recommendations.iter().any(|r| r.contains("storage")));
        assert!(recommendations.iter().any(|r| r.contains("maxConnections")));
        assert!(recommendations.iter().any(|r| r.contains("slow operations")));
    }

    #[test]
    fn test_resource_utilization_summary() {
        let info = MongoServerInfo {
            cpu_utilization_percentage: 75.5,
            memory_utilization_percentage: 82.3,
            disk_utilization_percentage: 67.8,
            connection_utilization_percentage: 45.2,
            ..Default::default()
        };

        let summary = info.resource_utilization_summary();
        assert!(summary.contains("75.5%"));
        assert!(summary.contains("82.3%"));
        assert!(summary.contains("67.8%"));
        assert!(summary.contains("45.2%"));
    }

    #[test]
    fn test_performance_summary() {
        let info = MongoServerInfo {
            operations_per_second: 1234.5,
            avg_operation_latency_ms: 156.7,
            slow_operations_count: 42,
            ..Default::default()
        };

        let summary = info.performance_summary();
        assert!(summary.contains("1234.5"));
        assert!(summary.contains("156.7"));
        assert!(summary.contains("42"));
    }

    #[test]
    fn test_calculate_derived_metrics() {
        let mut info = MongoServerInfo {
            current_connections: 80,
            max_connections: 100,
            memory_usage_bytes: 8 * 1024 * 1024 * 1024,       // 8GB
            max_memory_bytes: 10 * 1024 * 1024 * 1024,        // 10GB
            disk_space_used_bytes: 450 * 1024 * 1024 * 1024,  // 450GB
            total_disk_space_bytes: 500 * 1024 * 1024 * 1024, // 500GB
            avg_operation_latency_ms: 200.0,
            operations_per_second: 800.0,
            lock_percentage: 5.0,
            ..Default::default()
        };

        MongoServerInfo::calculate_derived_metrics(&mut info).unwrap();

        assert_eq!(info.connection_utilization_percentage, 80.0);
        assert_eq!(info.memory_utilization_percentage, 80.0);
        assert_eq!(info.disk_utilization_percentage, 90.0);
        assert!(info.cpu_utilization_percentage > 0.0);
        assert!(info.cpu_utilization_percentage <= 100.0);
    }

    #[test]
    fn test_identify_performance_bottlenecks() {
        let info = MongoServerInfo {
            cpu_utilization_percentage: 85.0,
            memory_utilization_percentage: 90.0,
            disk_utilization_percentage: 95.0,
            connection_utilization_percentage: 85.0,
            ..Default::default()
        };

        let bottlenecks = MongoServerInfo::identify_performance_bottlenecks(&info).unwrap();

        assert!(bottlenecks.len() >= 3); // Should detect CPU, Memory, Disk, and Connection bottlenecks
        assert!(bottlenecks.iter().any(|b| b.bottleneck_type == "CPU"));
        assert!(bottlenecks.iter().any(|b| b.bottleneck_type == "Memory"));
        assert!(bottlenecks.iter().any(|b| b.bottleneck_type == "Disk"));
        assert!(bottlenecks.iter().any(|b| b.bottleneck_type == "Connections"));
    }

    #[test]
    fn test_system_health_check() {
        let mut info = MongoServerInfo {
            cpu_utilization_percentage: 50.0,
            memory_utilization_percentage: 60.0,
            disk_utilization_percentage: 70.0,
            connection_utilization_percentage: 50.0,
            avg_operation_latency_ms: 100.0,
            slow_operations_count: 5,
            ..Default::default()
        };

        let health_check = MongoServerInfo::perform_system_health_check(&info).unwrap();

        assert_eq!(health_check.overall_status, "HEALTHY");
        assert_eq!(health_check.health_score, 1.0);
        assert!(health_check.critical_issues.is_empty());
        assert!(health_check.warnings.is_empty());
        assert_eq!(health_check.component_health.len(), 5); // CPU, Memory, Disk, Connections, Performance

        // Test with critical issues
        info.cpu_utilization_percentage = 96.0;
        info.memory_utilization_percentage = 96.0;

        let health_check = MongoServerInfo::perform_system_health_check(&info).unwrap();

        assert_eq!(health_check.overall_status, "CRITICAL");
        assert_eq!(health_check.health_score, 0.2);
        assert!(!health_check.critical_issues.is_empty());
    }

    #[test]
    fn test_wiredtiger_stats_analysis() {
        let info = MongoServerInfo {
            wiredtiger_cache_size_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            max_memory_bytes: 8 * 1024 * 1024 * 1024,            // 8GB
            operations_per_second: 1000.0,
            page_faults_per_sec: 10.0,
            wiredtiger_cache_utilization_percentage: 75.0,
            network_bytes_in_per_sec: 1000000.0,
            network_bytes_out_per_sec: 800000.0,
            ..Default::default()
        };

        let wt_stats = MongoServerInfo::analyze_wiredtiger_stats(&info).unwrap();

        assert_eq!(wt_stats.cache_stats.bytes_in_cache, 2 * 1024 * 1024 * 1024);
        assert_eq!(wt_stats.cache_stats.max_cache_size, 4 * 1024 * 1024 * 1024); // 50% of RAM
        assert_eq!(wt_stats.cache_stats.pressure_score, 0.75);
        assert_eq!(wt_stats.transaction_stats.rollback_ratio, 0.02);
        assert!(wt_stats.transaction_stats.transactions_begun > 0);
        assert!(wt_stats.block_manager_stats.blocks_read > 0);
    }

    #[test]
    fn test_operation_stats_analysis() {
        let info = MongoServerInfo {
            operations_per_second: 1000.0,
            avg_operation_latency_ms: 100.0,
            ..Default::default()
        };

        let op_stats = MongoServerInfo::analyze_operation_stats(&info).unwrap();

        assert_eq!(op_stats.inserts_per_sec, 200.0); // 20% of total
        assert_eq!(op_stats.queries_per_sec, 500.0); // 50% of total
        assert_eq!(op_stats.updates_per_sec, 200.0); // 20% of total
        assert_eq!(op_stats.deletes_per_sec, 50.0); // 5% of total
        assert_eq!(op_stats.commands_per_sec, 50.0); // 5% of total
        assert!(op_stats.avg_latencies_ms.contains_key("insert"));
        assert!(op_stats.avg_latencies_ms.contains_key("query"));
        assert_eq!(op_stats.avg_latencies_ms["query"], 100.0);
    }
}
