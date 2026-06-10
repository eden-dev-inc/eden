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

use super::utils::{DocAccessor, execute_admin_command_as_profiled, execute_current_op_as_profiled};

/// MongoDB Lock statistics and contention metrics
///
/// Comprehensive struct containing essential metrics about locking
/// behavior, contention patterns, and performance impacts.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockInfo {
    /// Total number of lock acquisitions across all types
    pub total_lock_acquisitions: u64,
    /// Total time spent waiting for locks (microseconds)
    pub total_lock_wait_time_us: u64,
    /// Average lock wait time (microseconds)
    pub avg_lock_wait_time_us: f64,
    /// Maximum lock wait time observed (microseconds)
    pub max_lock_wait_time_us: u64,
    /// Minimum lock wait time observed (microseconds)
    pub min_lock_wait_time_us: u64,
    /// Global lock acquisitions (read)
    pub global_read_locks: u64,
    /// Global lock acquisitions (write)
    pub global_write_locks: u64,
    /// Database lock acquisitions (read)
    pub database_read_locks: u64,
    /// Database lock acquisitions (write)
    pub database_write_locks: u64,
    /// Collection lock acquisitions (read)
    pub collection_read_locks: u64,
    /// Collection lock acquisitions (write)
    pub collection_write_locks: u64,
    /// Document lock acquisitions (read)
    pub document_read_locks: u64,
    /// Document lock acquisitions (write)
    pub document_write_locks: u64,
    /// Intent shared locks
    pub intent_shared_locks: u64,
    /// Intent exclusive locks
    pub intent_exclusive_locks: u64,
    /// Number of deadlocks detected
    pub deadlocks_detected: u64,
    /// Number of lock timeouts
    pub lock_timeouts: u64,
    /// Number of operations currently waiting for locks
    pub operations_waiting: u64,
    /// Total number of yielded operations due to lock pressure
    pub operations_yielded: u64,
    /// Lock contention ratio (0.0 to 1.0)
    pub lock_contention_ratio: f64,
    /// Average queue depth for lock requests
    pub avg_lock_queue_depth: f64,
    /// Peak lock queue depth observed
    pub peak_lock_queue_depth: u64,
    /// Lock escalation events (collection to database locks)
    pub lock_escalations: u64,
    /// Long-running transactions holding locks
    pub long_running_transactions: u64,
    /// Average transaction lock hold time (milliseconds)
    pub avg_transaction_lock_time_ms: f64,
    /// Lock efficiency ratio (useful work vs wait time)
    pub lock_efficiency_ratio: f64,
    /// Write conflicts per second
    pub write_conflicts_per_sec: f64,
    /// Read-write conflicts per second
    pub read_write_conflicts_per_sec: f64,
    /// Lock memory usage (bytes)
    pub lock_memory_usage_bytes: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoLockDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockDetailedMetrics {
    /// Operations experiencing high lock contention
    pub contended_operations: Vec<MongoContendedOperation>,
    /// Deadlock incidents with details
    pub deadlock_incidents: Vec<MongoDeadlockIncident>,
    /// Long-running operations holding locks
    pub long_running_lock_holders: Vec<MongoLongRunningLockHolder>,
    /// Lock bottlenecks by resource
    pub lock_bottlenecks: Vec<MongoLockBottleneck>,
    /// Lock escalation events
    pub escalation_events: Vec<MongoLockEscalationEvent>,
    /// Lock performance issues
    pub performance_issues: Option<Vec<MongoLockPerformanceIssue>>,
    /// Lock usage patterns by database/collection
    pub resource_usage: Option<Vec<MongoLockResourceUsage>>,
    /// Lock optimization recommendations
    pub optimization_recommendations: Option<Vec<MongoLockOptimization>>,
}

/// Information about operations experiencing high lock contention
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoContendedOperation {
    pub operation_id: String,
    pub operation_type: String,
    pub namespace: String,
    pub lock_type: String,
    pub wait_time_ms: f64,
    pub queue_position: u32,
    pub contention_level: String, // Low, Medium, High, Critical
    pub blocked_by_operation: Option<String>,
    pub start_time: DateTimeWrapper,
    pub client_info: String,
    pub impact_assessment: String,
    pub recommended_action: String,
}

/// Information about deadlock incidents
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDeadlockIncident {
    pub incident_id: String,
    pub detection_time: DateTimeWrapper,
    pub involved_operations: Vec<String>,
    pub lock_cycle: String,
    pub victim_operation: String,
    pub resolution_method: String,
    pub impact_duration_ms: f64,
    pub affected_collections: Vec<String>,
    pub root_cause_analysis: String,
    pub prevention_recommendation: String,
}

/// Information about long-running operations holding locks
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLongRunningLockHolder {
    pub operation_id: String,
    pub operation_type: String,
    pub namespace: String,
    pub lock_type: String,
    pub hold_duration_ms: f64,
    pub start_time: DateTimeWrapper,
    pub client_info: String,
    pub locks_held: Vec<String>,
    pub blocking_operations_count: u32,
    pub transaction_size: String, // Small, Medium, Large, Massive
    pub recommended_action: String,
    pub urgency_level: String,
}

/// Information about lock bottlenecks by resource
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockBottleneck {
    pub resource_type: String, // Global, Database, Collection, Document
    pub resource_name: String,
    pub bottleneck_severity: String,
    pub queue_depth: u32,
    pub avg_wait_time_ms: f64,
    pub operations_affected: u64,
    pub peak_contention_time: DateTimeWrapper,
    pub contention_pattern: String, // Burst, Sustained, Periodic
    pub optimization_suggestion: String,
    pub priority_level: String,
}

/// Information about lock escalation events
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockEscalationEvent {
    pub escalation_id: String,
    pub escalation_time: DateTimeWrapper,
    pub from_lock_type: String,
    pub to_lock_type: String,
    pub trigger_reason: String,
    pub affected_namespace: String,
    pub operations_impacted: u32,
    pub performance_impact: String,
    pub duration_ms: f64,
    pub prevention_strategy: String,
}

/// Lock performance bottlenecks and issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockPerformanceIssue {
    pub issue_type: String,
    pub severity: String, // Critical, High, Medium, Low
    pub affected_operations: u64,
    pub avg_impact_ms: f64,
    pub frequency_per_hour: u64,
    pub description: String,
    pub root_cause: String,
    pub recommended_solution: String,
    pub estimated_improvement: String,
}

/// Lock usage statistics by resource
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockResourceUsage {
    pub resource_type: String,
    pub resource_name: String,
    pub total_acquisitions: u64,
    pub avg_hold_time_ms: f64,
    pub max_hold_time_ms: f64,
    pub contention_percentage: f64,
    pub most_frequent_lock_type: String,
    pub peak_usage_time: DateTimeWrapper,
    pub optimization_opportunity: String,
}

/// Lock optimization recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLockOptimization {
    pub optimization_type: String,
    pub target_resource: String,
    pub current_performance: String,
    pub expected_improvement: String,
    pub implementation_effort: String, // Low, Medium, High
    pub priority: String,
    pub detailed_steps: Vec<String>,
    pub risk_assessment: String,
    pub success_metrics: Vec<String>,
}

impl MetadataCollection for MongoLockInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "server_status".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.serverStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(20)),
                ),
            ),
            (
                "current_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.currentOp": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "lock_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "locks": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(500)),
                ),
            ),
            (
                "slow_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 1000 },
                        "$or": [
                            { "locks": { "$exists": true } },
                            { "waitingForLock": true }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(60)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(200)),
                ),
            ),
            (
                "transaction_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.commitTransaction": { "$exists": true },
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
        "Return comprehensive lock contention and performance metrics"
    }

    fn category(&self) -> &'static str {
        "locks"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High // Locks change rapidly, need frequent monitoring
    }
}

use function_name::named;
use std::time::Duration;

#[allow(dead_code)]
impl MongoLockInfo {
    const HIGH_CONTENTION_THRESHOLD_MS: f64 = 100.0; // 100ms
    const LONG_RUNNING_THRESHOLD_MS: f64 = 30000.0; // 30 seconds
    const DEADLOCK_DETECTION_THRESHOLD: u64 = 1; // Any deadlock is significant
    const LOCK_TIMEOUT_THRESHOLD: u64 = 5; // 5 timeouts is concerning
    const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
    const MAX_DETAILED_RESULTS: usize = 100;
    const CRITICAL_QUEUE_DEPTH: u32 = 10;
    const HIGH_CONTENTION_RATIO: f64 = 0.3; // 30%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut lock_stats = MongoLockInfo::default();

        // Execute serverStatus directly - contains lock data
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;
        Self::parse_server_status(&mut lock_stats, &server_status_docs)?;

        // Execute currentOp to get active operations and locks
        let current_ops_docs = execute_current_op_as_profiled(doc! {}, context.clone(), Self::QUERY_TIMEOUT).await?;
        Self::parse_current_operations(&mut lock_stats, &current_ops_docs)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut lock_stats)?;

        // Detailed metrics temporarily disabled during refactor
        lock_stats.detailed_metrics = None;

        Ok(lock_stats)
    }

    fn parse_server_status(stats: &mut MongoLockInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(global_lock) = result.child("globalLock") {
                    if let Some(current_queue) = global_lock.child("currentQueue") {
                        if let Some(total) = current_queue.opt_i64("total") {
                            stats.operations_waiting = total as u64;
                        }
                        if let Some(readers) = current_queue.opt_i64("readers") {
                            stats.avg_lock_queue_depth = readers as f64;
                        }
                        if let Some(writers) = current_queue.opt_i64("writers") {
                            stats.peak_lock_queue_depth = std::cmp::max(stats.peak_lock_queue_depth, writers as u64);
                        }
                    }

                    if let Some(active_clients) = global_lock.child("activeClients")
                        && let Some(total) = active_clients.opt_i64("total")
                    {
                        stats.total_lock_acquisitions += total as u64;
                    }
                }

                if let Some(locks) = result.child("locks") {
                    Self::parse_lock_stats(stats, locks.raw())?;
                }

                if let Some(mem) = result.child("mem")
                    && let Some(mapped) = mem.opt_i64("mapped")
                {
                    stats.lock_memory_usage_bytes = (mapped * 1024 * 1024) as u64;
                }
            }
        }

        Ok(())
    }

    fn parse_lock_stats(stats: &mut MongoLockInfo, locks_doc: &Document) -> ResultEP<()> {
        for (lock_type, lock_data) in locks_doc {
            let lock_info = match lock_data.as_document() {
                Some(doc) => DocAccessor::new(doc),
                None => continue,
            };

            if let Ok(acquire_count) = lock_info.raw().get_document("acquireCount") {
                for (mode, count) in acquire_count {
                    if let Some(count_val) = count.as_i64() {
                        match (lock_type.as_str(), mode.as_str()) {
                            ("Global", "r") => stats.global_read_locks += count_val as u64,
                            ("Global", "w") => stats.global_write_locks += count_val as u64,
                            ("Database", "r") => stats.database_read_locks += count_val as u64,
                            ("Database", "w") => stats.database_write_locks += count_val as u64,
                            ("Collection", "r") => stats.collection_read_locks += count_val as u64,
                            ("Collection", "w") => stats.collection_write_locks += count_val as u64,
                            ("oplog", "r") | ("document", "r") => stats.document_read_locks += count_val as u64,
                            ("oplog", "w") | ("document", "w") => stats.document_write_locks += count_val as u64,
                            (_, "R") => stats.intent_shared_locks += count_val as u64,
                            (_, "W") => stats.intent_exclusive_locks += count_val as u64,
                            _ => {}
                        }
                    }
                }
            }

            if let Ok(wait_count) = lock_info.raw().get_document("acquireWaitCount") {
                for (_, count) in wait_count {
                    if let Some(count_val) = count.as_i64() {
                        stats.total_lock_acquisitions += count_val as u64;
                    }
                }
            }

            if let Ok(time_acquiring) = lock_info.raw().get_document("timeAcquiringMicros") {
                for (_, time_val) in time_acquiring {
                    if let Some(time_us) = time_val.as_i64() {
                        stats.total_lock_wait_time_us += time_us as u64;
                        stats.max_lock_wait_time_us = std::cmp::max(stats.max_lock_wait_time_us, time_us as u64);
                        if stats.min_lock_wait_time_us == 0 || (time_us as u64) < stats.min_lock_wait_time_us {
                            stats.min_lock_wait_time_us = time_us as u64;
                        }
                    }
                }
            }

            if let Some(deadlock_count) = lock_info.opt_i64("deadlockCount") {
                stats.deadlocks_detected += deadlock_count as u64;
            }
        }

        Ok(())
    }

    fn parse_current_operations(stats: &mut MongoLockInfo, docs: &[Document]) -> ResultEP<()> {
        let mut waiting_operations = 0;
        let mut long_running_count = 0;
        let mut transaction_times = Vec::new();

        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result")
                && let Ok(in_prog) = result.raw().get_array("inprog")
            {
                for op_value in in_prog {
                    let op_acc = match op_value.as_document() {
                        Some(doc) => DocAccessor::new(doc),
                        None => continue,
                    };

                    if op_acc.opt_bool("waitingForLock").unwrap_or(false) {
                        waiting_operations += 1;
                    }

                    if let Some(secs_running) = op_acc.opt_i64("secs_running") {
                        if secs_running > (Self::LONG_RUNNING_THRESHOLD_MS / 1000.0) as i64 {
                            long_running_count += 1;
                        }
                        transaction_times.push(secs_running as f64 * 1000.0);
                    }

                    if let Some(locks) = op_acc.child("locks") {
                        let lock_count = locks.raw().len();
                        if lock_count > 3 {
                            stats.lock_escalations += 1;
                        }
                    }
                }
            }
        }

        stats.operations_waiting = waiting_operations;
        stats.long_running_transactions = long_running_count;

        if !transaction_times.is_empty() {
            stats.avg_transaction_lock_time_ms = transaction_times.iter().sum::<f64>() / transaction_times.len() as f64;
        }

        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoLockInfo) -> ResultEP<()> {
        // Calculate average queue depth
        if stats.operations_waiting > 0 {
            stats.avg_lock_queue_depth = stats.operations_waiting as f64 / 2.0; // Simplified average
        }

        // Calculate lock efficiency ratio
        if stats.total_lock_wait_time_us > 0 && stats.total_lock_acquisitions > 0 {
            let total_operation_time = stats.total_lock_acquisitions as f64 * 1000.0; // Assume 1ms avg operation
            let wait_time_ms = stats.total_lock_wait_time_us as f64 / 1000.0;
            stats.lock_efficiency_ratio = total_operation_time / (total_operation_time + wait_time_ms);
        } else {
            stats.lock_efficiency_ratio = 1.0; // No contention
        }

        // Update peak queue depth if current waiting is higher
        stats.peak_lock_queue_depth = std::cmp::max(stats.peak_lock_queue_depth, stats.operations_waiting);

        Ok(())
    }
}
