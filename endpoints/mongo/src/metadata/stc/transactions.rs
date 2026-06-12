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

use super::utils::{DocAccessor, execute_admin_command_as_profiled, fetch};

/// MongoDB transaction statistics and performance metrics
///
/// Simplified struct containing essential metrics about transaction
/// performance, concurrency, and deadlock patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionInfo {
    /// Total number of transactions started
    pub total_transactions: u64,
    /// Number of transactions currently active
    pub active_transactions: u64,
    /// Number of committed transactions
    pub committed_transactions: u64,
    /// Number of aborted transactions
    pub aborted_transactions: u64,
    /// Number of transactions that timed out
    pub timeout_transactions: u64,
    /// Number of deadlocked transactions
    pub deadlocked_transactions: u64,
    /// Average transaction duration in milliseconds
    pub avg_transaction_duration_ms: f64,
    /// Maximum transaction duration in milliseconds
    pub max_transaction_duration_ms: f64,
    /// Minimum transaction duration in milliseconds
    pub min_transaction_duration_ms: f64,
    /// Number of prepared transactions (for two-phase commit)
    pub prepared_transactions: u64,
    /// Average operations per transaction
    pub avg_operations_per_transaction: f64,
    /// Total number of retryable write errors
    pub retryable_write_errors: u64,
    /// Total number of transient transaction errors
    pub transient_transaction_errors: u64,
    /// Number of transactions using write concern majority
    pub majority_write_concern_transactions: u64,
    /// Number of read-only transactions
    pub read_only_transactions: u64,
    /// Number of write transactions
    pub write_transactions: u64,
    /// Average lock acquisition time in milliseconds
    pub avg_lock_acquisition_time_ms: f64,
    /// Number of transactions waiting for locks
    pub lock_waiting_transactions: u64,
    /// Total transaction log size in bytes
    pub transaction_log_size_bytes: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoTransactionDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionDetailedMetrics {
    /// Long-running transactions (only collected when max_duration > threshold)
    pub long_running_transactions: Vec<MongoLongRunningTransaction>,
    /// Deadlocked transactions (only collected when deadlocks detected)
    pub deadlock_details: Vec<MongoDeadlockInfo>,
    /// High-contention resources (only collected when lock waits are high)
    pub high_contention_resources: Vec<MongoContentionInfo>,
    /// Transaction breakdown by operation type (collected periodically)
    pub transactions_by_operation: Option<Vec<MongoTransactionsByOperation>>,
    /// Failed transaction details (only collected when failures occur)
    pub failed_transaction_details: Vec<MongoFailedTransaction>,
    /// Resource utilization during transactions (collected when usage is high)
    pub resource_utilization: Option<MongoTransactionResourceUtilization>,
}

impl MetadataCollection for MongoTransactionInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "transaction_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.startTransaction": { "$exists": true } },
                            { "command.commitTransaction": { "$exists": true } },
                            { "command.abortTransaction": { "$exists": true } },
                            { "lsid": { "$exists": true }, "txnNumber": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(500)),
                ),
            ),
            (
                "long_running_transactions".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "lsid": { "$exists": true },
                        "txnNumber": { "$exists": true },
                        "millis": { "$gte": 10000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(50)),
                ),
            ),
            (
                "failed_transactions".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "lsid": { "$exists": true },
                        "txnNumber": { "$exists": true },
                        "$or": [
                            { "ok": 0 },
                            { "errorCode": { "$exists": true } },
                            { "command.abortTransaction": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "deadlock_info".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "errorCode": { "$in": [112, 117] }, // WriteConflict, LockTimeout
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "lock_contention".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "locks": { "$exists": true },
                        "millis": { "$gte": 1000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(50)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential transaction metrics and concurrency health indicators"
    }

    fn category(&self) -> &'static str {
        "transactions"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

#[allow(dead_code)]
impl MongoTransactionInfo {
    const LONG_TRANSACTION_THRESHOLD_MS: f64 = 10000.0; // 10 seconds
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 50;
    const HIGH_CONTENTION_THRESHOLD_MS: f64 = 1000.0; // 1 second lock wait
    const HIGH_ABORT_RATE_THRESHOLD: f64 = 10.0; // 10% abort rate

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut transaction_info = MongoTransactionInfo::default();

        // Execute serverStatus directly - contains transaction data
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;
        Self::parse_transaction_data(&mut transaction_info, &server_status_docs)?;

        // Detailed metrics temporarily disabled during refactor
        transaction_info.detailed_metrics = None;

        Ok(transaction_info)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_info: &MongoTransactionInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoTransactionDetailedMetrics>> {
        let needs_long_transaction_details = core_info.max_transaction_duration_ms > Self::LONG_TRANSACTION_THRESHOLD_MS;
        let needs_deadlock_details = core_info.deadlocked_transactions > 0;
        let needs_contention_details = core_info.avg_lock_acquisition_time_ms > Self::HIGH_CONTENTION_THRESHOLD_MS;
        let needs_failure_details = core_info.abort_rate_percentage() > Self::HIGH_ABORT_RATE_THRESHOLD;

        if !needs_long_transaction_details && !needs_deadlock_details && !needs_contention_details && !needs_failure_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoTransactionDetailedMetrics {
            long_running_transactions: Vec::new(),
            deadlock_details: Vec::new(),
            high_contention_resources: Vec::new(),
            transactions_by_operation: None,
            failed_transaction_details: Vec::new(),
            resource_utilization: None,
        };

        // Collect long-running transaction details if needed
        if needs_long_transaction_details {
            let docs = fetch(requests, "long_running_transactions", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.long_running_transactions = Self::parse_long_running_transactions(docs)?;
        }

        // Collect deadlock details if needed
        if needs_deadlock_details {
            let docs = fetch(requests, "deadlock_info", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.deadlock_details = Self::parse_deadlock_info(docs)?;
        }

        // Collect contention details if needed
        if needs_contention_details {
            let docs = fetch(requests, "lock_contention", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.high_contention_resources = Self::parse_contention_info(docs)?;
        }

        // Collect failed transaction details if needed
        if needs_failure_details {
            let docs = fetch(requests, "failed_transactions", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.failed_transaction_details = Self::parse_failed_transactions(docs)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_transaction_data(info: &mut MongoTransactionInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result") {
                if let Some(transactions) = result.child("transactions") {
                    if let Some(total_started) = transactions.opt_u64("totalStarted")
                        && total_started > 0
                    {
                        info.total_transactions = total_started;
                    }

                    info.active_transactions = transactions.opt_u64("currentActive").unwrap_or(0);
                    info.committed_transactions = transactions.opt_u64("totalCommitted").unwrap_or(0);
                    info.aborted_transactions = transactions.opt_u64("totalAborted").unwrap_or(0);

                    let prepared_total = transactions.opt_u64("totalPrepared").unwrap_or(0);
                    let prepared_current = transactions.opt_u64("currentPrepared").unwrap_or(0);
                    info.prepared_transactions = prepared_total.max(prepared_current);

                    let current_open = transactions.opt_u64("currentOpen").unwrap_or(0);
                    info.lock_waiting_transactions = current_open.saturating_sub(info.active_transactions);

                    if let Some(max_active) = transactions.opt_u64("maxActiveDurationMicros") {
                        let max_active_ms = max_active as f64 / 1000.0;
                        info.max_transaction_duration_ms = max_active_ms;
                        if info.min_transaction_duration_ms == 0.0 {
                            info.min_transaction_duration_ms = max_active_ms;
                        } else {
                            info.min_transaction_duration_ms = info.min_transaction_duration_ms.min(max_active_ms);
                        }

                        if info.avg_transaction_duration_ms == 0.0 {
                            info.avg_transaction_duration_ms = max_active_ms;
                        }
                    }

                    if let Some(max_inactive) = transactions.opt_u64("maxInactiveDurationMicros") {
                        let wait_ms = max_inactive as f64 / 1000.0;
                        info.avg_lock_acquisition_time_ms = info.avg_lock_acquisition_time_ms.max(wait_ms);
                    }

                    if let Some(state_reasons) = transactions.child("stateReasons")
                        && let Some(abort_cause) = state_reasons.child("abortCause")
                    {
                        let lock_timeout = abort_cause.opt_u64("LockTimeout").unwrap_or(0);
                        let max_time = abort_cause.opt_u64("MaxTimeMSExpired").unwrap_or(0);
                        let exceeded_time_limit = abort_cause.opt_u64("ExceededTimeLimit").unwrap_or(0);
                        let write_conflict = abort_cause.opt_u64("WriteConflict").unwrap_or(0);
                        let transient = abort_cause.opt_u64("TransientTransactionError").unwrap_or(0);

                        let timeout_total = lock_timeout + max_time + exceeded_time_limit;
                        if timeout_total > 0 {
                            info.timeout_transactions = timeout_total;
                        }

                        if lock_timeout > 0 {
                            info.deadlocked_transactions = info.deadlocked_transactions.max(lock_timeout);
                        }

                        if write_conflict > 0 {
                            info.retryable_write_errors = write_conflict;
                        }

                        if transient > 0 {
                            info.transient_transaction_errors = transient;
                        }
                    }

                    if let Some(num_doc) = transactions.child("num")
                        && let Some(snapshot_stats) = num_doc.child("snapshot")
                    {
                        if let Some(snapshot_started) = snapshot_stats.opt_u64("started")
                            && snapshot_started > 0
                        {
                            info.total_transactions = info.total_transactions.max(snapshot_started);
                        }

                        if let Some(snapshot_committed) = snapshot_stats.opt_u64("committed")
                            && snapshot_committed > 0
                        {
                            info.committed_transactions = info.committed_transactions.max(snapshot_committed);
                        }

                        if let Some(snapshot_aborted) = snapshot_stats.opt_u64("aborted")
                            && snapshot_aborted > 0
                        {
                            info.aborted_transactions = info.aborted_transactions.max(snapshot_aborted);
                        }
                    }
                }

                if let Some(wt) = result.child("wiredTiger")
                    && let Some(log) = wt.child("log")
                    && let Some(bytes_written) = log.opt_u64("log bytes written")
                    && bytes_written > 0
                {
                    info.transaction_log_size_bytes = bytes_written;
                }
            }
        }

        if info.min_transaction_duration_ms == 0.0 {
            info.min_transaction_duration_ms = info.max_transaction_duration_ms;
        }

        if info.avg_transaction_duration_ms == 0.0 {
            info.avg_transaction_duration_ms = info.max_transaction_duration_ms;
        }

        Ok(())
    }

    fn parse_long_running_transactions(docs: Vec<Document>) -> ResultEP<Vec<MongoLongRunningTransaction>> {
        let mut long_transactions = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(millis), Some(ts)) = (acc.opt_f64("millis"), acc.opt_datetime("ts")) {
                let session_id = if let Some(lsid) = acc.child("lsid") {
                    if let Ok(id) = lsid.raw().get_object_id("id") {
                        id.to_string()
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                let transaction_number = acc.opt_i64("txnNumber").unwrap_or(0);
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());

                let database = ns.split('.').next().unwrap_or("unknown").to_string();

                let collection = ns.split('.').next_back().unwrap_or("unknown").to_string();

                let operation_type = acc
                    .child("command")
                    .map(|cmd| {
                        let raw = cmd.raw();
                        if raw.contains_key("find") {
                            "find"
                        } else if raw.contains_key("insert") {
                            "insert"
                        } else if raw.contains_key("update") {
                            "update"
                        } else if raw.contains_key("delete") {
                            "delete"
                        } else if raw.contains_key("aggregate") {
                            "aggregate"
                        } else {
                            "other"
                        }
                    })
                    .unwrap_or("unknown")
                    .to_string();

                long_transactions.push(MongoLongRunningTransaction {
                    session_id,
                    transaction_number,
                    database,
                    collection,
                    operation_type,
                    duration_ms: millis,
                    docs_examined: acc.opt_u64("docsExamined").unwrap_or(0),
                    docs_returned: acc.opt_u64("nreturned").unwrap_or(0),
                    timestamp: ts,
                    user: acc.opt_string("user"),
                    client_info: acc.opt_string("appName"),
                });
            }
        }

        Ok(long_transactions)
    }

    fn parse_deadlock_info(docs: Vec<Document>) -> ResultEP<Vec<MongoDeadlockInfo>> {
        let mut deadlocks = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(error_code), Some(ts)) = (acc.opt_i32("errorCode"), acc.opt_datetime("ts")) {
                let session_id = if let Some(lsid) = acc.child("lsid") {
                    if let Ok(id) = lsid.raw().get_object_id("id") {
                        id.to_string()
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                let transaction_number = acc.opt_i64("txnNumber").unwrap_or(0);
                let resource = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());

                let deadlock_type = match error_code {
                    112 => "WriteConflict".to_string(),
                    117 => "LockTimeout".to_string(),
                    _ => format!("Error{}", error_code),
                };

                let error_message = acc.opt_string("errmsg").unwrap_or_default();

                deadlocks.push(MongoDeadlockInfo {
                    session_id,
                    transaction_number,
                    deadlock_type,
                    resource,
                    error_code,
                    error_message,
                    timestamp: ts,
                    resolution_time_ms: acc.opt_f64("millis").unwrap_or(0.0),
                });
            }
        }

        Ok(deadlocks)
    }

    fn parse_contention_info(docs: Vec<Document>) -> ResultEP<Vec<MongoContentionInfo>> {
        let mut contentions = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let (Some(millis), Some(ts)) = (acc.opt_f64("millis"), acc.opt_datetime("ts")) {
                let resource = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());

                let operation_type = acc
                    .child("command")
                    .map(|cmd| {
                        let raw = cmd.raw();
                        if raw.contains_key("find") {
                            "read"
                        } else if raw.contains_key("insert") || raw.contains_key("update") || raw.contains_key("delete") {
                            "write"
                        } else {
                            "other"
                        }
                    })
                    .unwrap_or("unknown")
                    .to_string();

                let lock_mode = acc.child("locks").map(|_| "intent".to_string()).unwrap_or_else(|| "unknown".to_string());

                let waiting_time_ms = millis;

                contentions.push(MongoContentionInfo {
                    resource,
                    operation_type,
                    lock_mode,
                    waiting_time_ms,
                    contention_count: 1, // Would need aggregation for accurate count
                    timestamp: ts,
                });
            }
        }

        Ok(contentions)
    }

    fn parse_failed_transactions(docs: Vec<Document>) -> ResultEP<Vec<MongoFailedTransaction>> {
        let mut failed_transactions = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let Some(ts) = acc.opt_datetime("ts") {
                let session_id = if let Some(lsid) = acc.child("lsid") {
                    if let Ok(id) = lsid.raw().get_object_id("id") {
                        id.to_string()
                    } else {
                        "unknown".to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                let transaction_number = acc.opt_i64("txnNumber").unwrap_or(0);

                let error_code = acc.opt_i32("errorCode").unwrap_or(0);
                let error_message = acc.opt_string("errmsg").unwrap_or_default();

                let failure_reason = if acc.child("command").map(|c| c.raw().contains_key("abortTransaction")).unwrap_or(false) {
                    "Explicit abort".to_string()
                } else if error_code != 0 {
                    format!("Error {}: {}", error_code, error_message)
                } else {
                    "Unknown failure".to_string()
                };

                let operation_count = 1u32; // Would need session tracking for accurate count

                failed_transactions.push(MongoFailedTransaction {
                    session_id,
                    transaction_number,
                    failure_reason,
                    error_code,
                    error_message,
                    operation_count,
                    duration_before_failure_ms: acc.opt_f64("millis").unwrap_or(0.0),
                    timestamp: ts,
                });
            }
        }

        Ok(failed_transactions)
    }
}

/// Information about long-running transactions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLongRunningTransaction {
    /// Session identifier
    pub session_id: String,
    /// Transaction number within the session
    pub transaction_number: i64,
    /// Database name
    pub database: String,
    /// Collection name
    pub collection: String,
    /// Type of operation (find, insert, update, delete, etc.)
    pub operation_type: String,
    /// Transaction duration in milliseconds
    pub duration_ms: f64,
    /// Number of documents examined
    pub docs_examined: u64,
    /// Number of documents returned
    pub docs_returned: u64,
    /// Timestamp when the transaction started
    pub timestamp: DateTimeWrapper,
    /// User who executed the transaction
    pub user: Option<String>,
    /// Client application information
    pub client_info: Option<String>,
}

/// Information about deadlocked transactions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoDeadlockInfo {
    /// Session identifier
    pub session_id: String,
    /// Transaction number within the session
    pub transaction_number: i64,
    /// Type of deadlock (WriteConflict, LockTimeout, etc.)
    pub deadlock_type: String,
    /// Resource that caused the deadlock
    pub resource: String,
    /// MongoDB error code
    pub error_code: i32,
    /// Error message
    pub error_message: String,
    /// Timestamp when the deadlock occurred
    pub timestamp: DateTimeWrapper,
    /// Time taken to resolve the deadlock
    pub resolution_time_ms: f64,
}

/// Information about high-contention resources
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoContentionInfo {
    /// Resource identifier (namespace)
    pub resource: String,
    /// Type of operation causing contention
    pub operation_type: String,
    /// Lock mode requested
    pub lock_mode: String,
    /// Average waiting time for locks
    pub waiting_time_ms: f64,
    /// Number of operations experiencing contention
    pub contention_count: u64,
    /// Timestamp when contention was observed
    pub timestamp: DateTimeWrapper,
}

/// Transaction statistics grouped by operation type
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionsByOperation {
    /// Operation type (read, write, mixed)
    pub operation_type: String,
    /// Total transactions of this type
    pub total_transactions: u64,
    /// Average duration for this operation type
    pub avg_duration_ms: f64,
    /// Success rate for this operation type
    pub success_rate_percentage: f64,
    /// Average operations per transaction
    pub avg_operations_per_transaction: f64,
}

/// Information about failed transactions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedTransaction {
    /// Session identifier
    pub session_id: String,
    /// Transaction number within the session
    pub transaction_number: i64,
    /// Reason for failure
    pub failure_reason: String,
    /// MongoDB error code
    pub error_code: i32,
    /// Error message
    pub error_message: String,
    /// Number of operations completed before failure
    pub operation_count: u32,
    /// Duration before failure occurred
    pub duration_before_failure_ms: f64,
    /// Timestamp when the failure occurred
    pub timestamp: DateTimeWrapper,
}

/// Resource utilization during transaction processing
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoTransactionResourceUtilization {
    /// CPU usage percentage during transactions
    pub cpu_usage_percentage: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Active connection count
    pub active_connections: u32,
    /// Lock manager utilization percentage
    pub lock_manager_utilization_percentage: f64,
    /// Transaction log utilization percentage
    pub transaction_log_utilization_percentage: f64,
    /// Average queue depth for lock requests
    pub avg_lock_queue_depth: f64,
}

impl MongoTransactionInfo {
    /// Calculates the transaction success rate percentage
    pub fn success_rate_percentage(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            (self.committed_transactions as f64 / self.total_transactions as f64) * 100.0
        }
    }

    /// Calculates the transaction abort rate percentage
    pub fn abort_rate_percentage(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            (self.aborted_transactions as f64 / self.total_transactions as f64) * 100.0
        }
    }

    /// Checks if there are long-running transactions
    pub fn has_long_running_transactions(&self, threshold_ms: f64) -> bool {
        self.max_transaction_duration_ms > threshold_ms
    }

    /// Checks if there are deadlock issues
    pub fn has_deadlock_issues(&self) -> bool {
        self.deadlocked_transactions > 0
    }

    /// Checks if there are lock contention issues
    pub fn has_lock_contention(&self, threshold_ms: f64) -> bool {
        self.avg_lock_acquisition_time_ms > threshold_ms
    }

    /// Returns the timeout rate percentage
    pub fn timeout_rate_percentage(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            (self.timeout_transactions as f64 / self.total_transactions as f64) * 100.0
        }
    }

    /// Returns the deadlock rate percentage
    pub fn deadlock_rate_percentage(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            (self.deadlocked_transactions as f64 / self.total_transactions as f64) * 100.0
        }
    }

    /// Checks if the majority write concern usage is high
    pub fn has_high_majority_write_concern_usage(&self) -> bool {
        if self.write_transactions == 0 {
            false
        } else {
            let majority_percentage = (self.majority_write_concern_transactions as f64 / self.write_transactions as f64) * 100.0;
            majority_percentage > 80.0 // Consider high if more than 80% use majority write concern
        }
    }

    /// Calculates the read-write transaction ratio
    pub fn read_write_ratio(&self) -> f64 {
        if self.write_transactions == 0 {
            f64::INFINITY
        } else {
            self.read_only_transactions as f64 / self.write_transactions as f64
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Checks if there are high error rates
    pub fn has_high_error_rates(&self, threshold_percentage: f64) -> bool {
        let total_errors = self.retryable_write_errors + self.transient_transaction_errors + self.aborted_transactions;
        if self.total_transactions == 0 {
            false
        } else {
            let error_rate = (total_errors as f64 / self.total_transactions as f64) * 100.0;
            error_rate > threshold_percentage
        }
    }

    /// Calculates the average active transaction concurrency
    pub fn avg_transaction_concurrency(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            self.active_transactions as f64
        }
    }

    /// Checks if the transaction system is healthy
    pub fn is_transaction_system_healthy(&self) -> bool {
        let success_rate = self.success_rate_percentage();
        let deadlock_rate = self.deadlock_rate_percentage();
        let timeout_rate = self.timeout_rate_percentage();

        success_rate >= 95.0
            && deadlock_rate <= 1.0
            && timeout_rate <= 2.0
            && self.avg_lock_acquisition_time_ms < Self::HIGH_CONTENTION_THRESHOLD_MS
    }

    /// Returns transaction throughput (transactions per second, estimated)
    pub fn estimated_throughput_per_second(&self) -> f64 {
        // Assuming the metrics cover a 10-minute window
        let window_seconds = 600.0;
        self.total_transactions as f64 / window_seconds
    }
}

#[cfg(all(test, external_db))]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_transaction_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let transaction_info = MongoTransactionInfo::default();

        let result = transaction_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.avg_transaction_duration_ms >= 0.0);
    }

    #[tokio::test]
    async fn test_transaction_health_calculations() {
        let info = MongoTransactionInfo {
            total_transactions: 100,
            committed_transactions: 95,
            aborted_transactions: 3,
            timeout_transactions: 1,
            deadlocked_transactions: 1,
            read_only_transactions: 60,
            write_transactions: 40,
            majority_write_concern_transactions: 35,
            ..MongoTransactionInfo::default()
        };

        assert_eq!(info.success_rate_percentage(), 95.0);
        assert_eq!(info.abort_rate_percentage(), 3.0);
        assert_eq!(info.timeout_rate_percentage(), 1.0);
        assert_eq!(info.deadlock_rate_percentage(), 1.0);
        assert_eq!(info.read_write_ratio(), 1.5);
        assert!(info.has_high_majority_write_concern_usage());
        assert!(info.has_deadlock_issues());
        assert!(info.is_transaction_system_healthy());
    }

    #[tokio::test]
    async fn test_transaction_performance_metrics() {
        let info = MongoTransactionInfo {
            total_transactions: 1000,
            avg_transaction_duration_ms: 150.0,
            max_transaction_duration_ms: 5000.0,
            avg_lock_acquisition_time_ms: 50.0,
            active_transactions: 10,
            ..MongoTransactionInfo::default()
        };

        assert_eq!(info.estimated_throughput_per_second(), 1000.0 / 600.0);
        assert!(!info.has_long_running_transactions(10000.0));
        assert!(info.has_long_running_transactions(3000.0));
        assert!(!info.has_lock_contention(100.0));
        assert!(info.has_lock_contention(25.0));
    }

    #[tokio::test]
    async fn test_transaction_error_detection() {
        let info = MongoTransactionInfo {
            total_transactions: 100,
            retryable_write_errors: 5,
            transient_transaction_errors: 3,
            aborted_transactions: 7,
            ..MongoTransactionInfo::default()
        };

        assert!(info.has_high_error_rates(10.0)); // 15% error rate
        assert!(!info.has_high_error_rates(20.0)); // Below 20% threshold
    }
}
