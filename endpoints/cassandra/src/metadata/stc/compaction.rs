use crate::api::lib::QueryUnpagedInput;
use crate::metadata::capabilities::CASSANDRA_HAS_VIRTUAL_TABLES;
use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use chrono::Utc;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{
    DEFAULT_QUERY_TIMEOUT, get_f64_or_zero, get_string, get_string_or, get_u64_or_zero, map_rows, query, query_map, run_named_query,
    run_optional_named_query, run_optional_query,
};

/// Cassandra compaction status: pending/active/completed counts, throughput
/// and per-keyspace breakdown.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraCompactionInfo {
    /// Total number of pending compactions across all keyspaces
    pub total_pending_compactions: u64,
    /// Total number of completed compactions in the last period
    pub completed_compactions: u64,
    /// Number of active (running) compactions
    pub active_compactions: u64,
    /// Total bytes pending compaction across cluster
    pub pending_bytes: u64,
    /// Total bytes compacted in the last period
    pub compacted_bytes: u64,
    /// Average compaction rate in MB/s
    pub avg_compaction_rate_mb_per_sec: f64,
    /// Number of tables with high pending compaction load
    pub tables_with_high_pending: u64,
    /// Number of failed compactions
    pub failed_compactions: u64,
    /// Compaction strategy distribution
    pub strategy_distribution: HashMap<String, u64>,
    /// Compaction metrics by keyspace
    pub keyspace_metrics: Vec<CassandraKeyspaceCompactionMetrics>,
    /// Detailed compaction task information (only when high load detected)
    pub detailed_tasks: Option<Vec<CassandraCompactionTask>>,
}

/// Compaction metrics for a specific keyspace
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceCompactionMetrics {
    /// Keyspace name
    pub keyspace_name: String,
    /// Number of pending compactions in this keyspace
    pub pending_compactions: u64,
    /// Bytes pending compaction in this keyspace
    pub pending_bytes: u64,
    /// Number of active compactions in this keyspace
    pub active_compactions: u64,
    /// Average compaction rate for this keyspace (MB/s)
    pub avg_rate_mb_per_sec: f64,
    /// Primary compaction strategy used
    pub primary_strategy: String,
    /// Number of tables in this keyspace
    pub table_count: u64,
    /// Most recent compaction timestamp
    pub last_compaction_time: Option<String>,
}

/// Detailed information about a specific compaction task
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraCompactionTask {
    /// Unique task identifier
    pub task_id: String,
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Compaction type (MAJOR, MINOR, USER_DEFINED etc.)
    pub compaction_type: String,
    /// Current task status (PENDING, ACTIVE, COMPLETED, FAILED)
    pub status: String,
    /// Progress percentage (0-100)
    pub progress_percentage: f64,
    /// Total bytes to compact
    pub total_bytes: u64,
    /// Bytes already compacted
    pub compacted_bytes: u64,
    /// Estimated time remaining (seconds)
    pub estimated_remaining_seconds: Option<u64>,
    /// Compaction start time
    pub start_time: Option<String>,
    /// Current compaction rate (MB/s)
    pub current_rate_mb_per_sec: f64,
    /// Number of SSTables involved
    pub sstable_count: u64,
}

impl MetadataCollection for CassandraCompactionInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                "compaction_history",
                query(
                    "SELECT keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged
                     FROM system.compaction_history",
                ),
            ),
            (
                "pending_compactions",
                query(
                    "SELECT keyspace_name, table_name, task_type, total, completed, unit, compaction_id
                     FROM system.compactions_in_progress",
                ),
            ),
            (
                "table_stats",
                query(
                    "SELECT keyspace_name, table_name, compaction
                     FROM system_schema.tables",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return Cassandra compaction metrics and task information"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "compaction"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl CassandraCompactionInfo {
    const HIGH_PENDING_THRESHOLD: u64 = 10;
    const HIGH_LOAD_TOTAL_THRESHOLD: u64 = 50;
    const BYTES_TO_MB: f64 = 1024.0 * 1024.0;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let requests = self.request();

        // compaction_history and table_stats are critical; pending_compactions is soft-fail
        // because system.compactions_in_progress may not exist on older Cassandra versions.
        let (compaction_history, table_stats) = tokio::try_join!(
            run_named_query(&requests, "compaction_history", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "table_stats", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        let pending_compactions = run_optional_named_query(&requests, "pending_compactions", context.clone(), DEFAULT_QUERY_TIMEOUT)
            .await
            .unwrap_or(Value::Array(Vec::new()));

        // Compute the cutoff timestamp for filtering compaction history entries.
        let one_hour_ago = Utc::now() - chrono::Duration::hours(1);

        let mut info = CassandraCompactionInfo::default();

        Self::process_compaction_history(&mut info, &compaction_history, one_hour_ago);
        Self::process_pending_compactions(&mut info, &pending_compactions);
        Self::process_table_statistics(&mut info, &table_stats);

        info.keyspace_metrics = Self::build_keyspace_metrics(&compaction_history, &pending_compactions, &table_stats, one_hour_ago);

        // Query the Cassandra 4.0+ virtual table for live SSTable task data.
        // This is the only reliable source for `active_compactions`; on older
        // clusters the virtual table does not exist and we leave the count at 0.
        let sstable_tasks = if capabilities.has(&CASSANDRA_HAS_VIRTUAL_TABLES) {
            run_optional_query(
                "SELECT keyspace_name, table_name, task_id, kind, progress, total, unit \
                 FROM system_views.sstable_tasks",
                context.clone(),
                DEFAULT_QUERY_TIMEOUT,
                "sstable_tasks",
            )
            .await
            .unwrap_or(Value::Array(Vec::new()))
        } else {
            Value::Array(Vec::new())
        };

        Self::process_sstable_tasks(&mut info, &sstable_tasks);

        if info.total_pending_compactions > Self::HIGH_LOAD_TOTAL_THRESHOLD {
            // Prefer virtual-table rows when available because they carry real
            // progress data; fall back to compactions_in_progress otherwise.
            let tasks = match sstable_tasks.as_array().map(|r| r.is_empty()) {
                Some(false) => Self::build_detailed_tasks_from_virtual(&sstable_tasks),
                _ => Self::build_detailed_tasks(&pending_compactions),
            };
            info.detailed_tasks = Some(tasks);
        }

        // failed_compactions is not tracked in standard system tables; leave as 0.
        info.failed_compactions = 0;

        Ok(info)
    }

    /// Populate completed-compaction counts and byte totals from `system.compaction_history`.
    ///
    /// Only rows whose `compacted_at` timestamp falls within the last hour (relative to
    /// `cutoff`) are counted toward the "last period" metrics.  Rows without a parseable
    /// timestamp are included unconditionally so we never silently drop data.
    fn process_compaction_history(info: &mut CassandraCompactionInfo, history_data: &Value, cutoff: chrono::DateTime<Utc>) {
        let Value::Array(rows) = history_data else {
            return;
        };

        let mut completed = 0u64;
        let mut total_bytes_out = 0u64;

        for row in rows {
            // Apply time filter when the field is present and parseable.
            if let Some(ts_str) = get_string(row, "compacted_at")
                && let Ok(ts) = ts_str.parse::<chrono::DateTime<Utc>>()
                && ts < cutoff
            {
                continue;
            }

            completed += 1;
            total_bytes_out += get_u64_or_zero(row, "bytes_out");
        }

        info.completed_compactions = completed;
        info.compacted_bytes = total_bytes_out;

        // Rate is expressed as MB compacted over the observation window (1 hour = 3600 s).
        if completed > 0 {
            info.avg_compaction_rate_mb_per_sec = (total_bytes_out as f64 / Self::BYTES_TO_MB) / 3600.0;
        }
    }

    /// Populate pending-compaction counts and byte totals from `system.compactions_in_progress`.
    fn process_pending_compactions(info: &mut CassandraCompactionInfo, pending_data: &Value) {
        let Value::Array(rows) = pending_data else {
            return;
        };

        info.total_pending_compactions = rows.len() as u64;

        let mut pending_bytes = 0u64;
        let mut table_pending_counts: HashMap<String, u64> = HashMap::new();

        for row in rows {
            pending_bytes += get_u64_or_zero(row, "total");

            if let (Some(keyspace), Some(table)) = (get_string(row, "keyspace_name"), get_string(row, "table_name")) {
                *table_pending_counts.entry(format!("{keyspace}.{table}")).or_insert(0) += 1;
            }
        }

        info.pending_bytes = pending_bytes;
        info.tables_with_high_pending = table_pending_counts.values().filter(|&&count| count > Self::HIGH_PENDING_THRESHOLD).count() as u64;

        // active_compactions cannot be reliably derived from compactions_in_progress alone
        // (the table lists tasks that are queued, not necessarily running). Leave as 0
        // rather than fabricate a value.
        info.active_compactions = 0;
    }

    /// Populate `strategy_distribution` from `system_schema.tables`.
    fn process_table_statistics(info: &mut CassandraCompactionInfo, table_data: &Value) {
        let Value::Array(rows) = table_data else {
            return;
        };

        for row in rows {
            let strategy = Self::extract_compaction_strategy(row);
            *info.strategy_distribution.entry(strategy).or_insert(0) += 1;
        }
    }

    /// Build per-keyspace compaction metrics by joining the three query result sets.
    fn build_keyspace_metrics(
        history_data: &Value,
        pending_data: &Value,
        table_data: &Value,
        cutoff: chrono::DateTime<Utc>,
    ) -> Vec<CassandraKeyspaceCompactionMetrics> {
        let mut map: HashMap<String, CassandraKeyspaceCompactionMetrics> = HashMap::new();

        // Seed the map from table_stats so every keyspace with tables is represented.
        if let Value::Array(table_rows) = table_data {
            for row in table_rows {
                let Some(keyspace) = get_string(row, "keyspace_name") else {
                    continue;
                };
                let entry = map.entry(keyspace.clone()).or_insert_with(|| CassandraKeyspaceCompactionMetrics {
                    keyspace_name: keyspace.clone(),
                    pending_compactions: 0,
                    pending_bytes: 0,
                    active_compactions: 0,
                    avg_rate_mb_per_sec: 0.0,
                    primary_strategy: "Unknown".to_string(),
                    table_count: 0,
                    last_compaction_time: None,
                });

                entry.table_count += 1;

                entry.primary_strategy = Self::extract_compaction_strategy(row);
            }
        }

        // Accumulate pending-compaction counts and bytes per keyspace.
        if let Value::Array(pending_rows) = pending_data {
            for row in pending_rows {
                let Some(keyspace) = get_string(row, "keyspace_name") else {
                    continue;
                };
                let Some(entry) = map.get_mut(&keyspace) else {
                    continue;
                };
                entry.pending_compactions += 1;
                entry.pending_bytes += get_u64_or_zero(row, "total");
            }
        }

        // Record the most recent compaction timestamp per keyspace from history.
        if let Value::Array(history_rows) = history_data {
            for row in history_rows {
                let Some(keyspace) = get_string(row, "keyspace_name") else {
                    continue;
                };

                // Apply the same time filter used in process_compaction_history.
                if let Some(ts_str) = get_string(row, "compacted_at") {
                    if let Ok(ts) = ts_str.parse::<chrono::DateTime<Utc>>()
                        && ts < cutoff
                    {
                        continue;
                    }

                    let Some(entry) = map.get_mut(&keyspace) else {
                        continue;
                    };

                    // Keep the first (most recent) timestamp we encounter per keyspace.
                    if entry.last_compaction_time.is_none() {
                        entry.last_compaction_time = Some(ts_str);
                    }
                }
            }
        }

        map.into_values().collect()
    }

    /// Build detailed task list from `system.compactions_in_progress`.
    fn build_detailed_tasks(pending_data: &Value) -> Vec<CassandraCompactionTask> {
        map_rows(pending_data, |row| {
            let keyspace_name = get_string(row, "keyspace_name")?;
            let table_name = get_string(row, "table_name")?;

            let total_bytes = get_u64_or_zero(row, "total");
            let compacted_bytes = get_u64_or_zero(row, "completed");

            let progress_percentage = if total_bytes > 0 {
                (compacted_bytes as f64 / total_bytes as f64) * 100.0
            } else {
                0.0
            };

            Some(CassandraCompactionTask {
                task_id: get_string_or(row, "compaction_id", ""),
                keyspace_name,
                table_name,
                compaction_type: get_string_or(row, "task_type", "UNKNOWN"),
                status: "PENDING".to_string(),
                progress_percentage,
                total_bytes,
                compacted_bytes,
                estimated_remaining_seconds: None,
                start_time: None,
                current_rate_mb_per_sec: 0.0,
                sstable_count: 0,
            })
        })
    }

    /// Update `active_compactions` from `system_views.sstable_tasks`.
    ///
    /// Only rows whose `kind` equals `"compaction"` are counted as active
    /// compactions; other kinds (cleanup, scrub, upgrade, …) are ignored for
    /// this counter so it stays semantically consistent with its field doc.
    fn process_sstable_tasks(info: &mut CassandraCompactionInfo, tasks_data: &Value) {
        let Value::Array(rows) = tasks_data else {
            return;
        };

        info.active_compactions = rows.iter().filter(|row| get_string(row, "kind").as_deref() == Some("compaction")).count() as u64;
    }

    /// Build detailed task list from `system_views.sstable_tasks`.
    ///
    /// Each row from the virtual table carries a live `progress` percentage and
    /// a `total` byte count, giving much richer data than the legacy
    /// `compactions_in_progress` table.
    fn build_detailed_tasks_from_virtual(tasks_data: &Value) -> Vec<CassandraCompactionTask> {
        map_rows(tasks_data, |row| {
            let keyspace_name = get_string(row, "keyspace_name")?;
            let table_name = get_string(row, "table_name")?;

            let progress_percentage = get_f64_or_zero(row, "progress");
            let total_bytes = get_u64_or_zero(row, "total");

            // Derive compacted_bytes from the reported percentage and total.
            let compacted_bytes = if total_bytes > 0 {
                ((progress_percentage / 100.0) * total_bytes as f64) as u64
            } else {
                0
            };

            let kind = get_string_or(row, "kind", "UNKNOWN").to_uppercase();

            Some(CassandraCompactionTask {
                task_id: get_string_or(row, "task_id", ""),
                keyspace_name,
                table_name,
                compaction_type: kind,
                status: "ACTIVE".to_string(),
                progress_percentage,
                total_bytes,
                compacted_bytes,
                estimated_remaining_seconds: None,
                start_time: None,
                current_rate_mb_per_sec: 0.0,
                sstable_count: 0,
            })
        })
    }
}

impl CassandraCompactionInfo {
    /// Checks if the cluster has high compaction load
    pub fn has_high_compaction_load(&self) -> bool {
        self.total_pending_compactions > Self::HIGH_LOAD_TOTAL_THRESHOLD || self.tables_with_high_pending > 5
    }

    /// Gets the compaction efficiency ratio (output bytes / input bytes)
    pub fn compaction_efficiency_ratio(&self) -> f64 {
        if self.pending_bytes == 0 {
            1.0
        } else {
            self.compacted_bytes as f64 / self.pending_bytes as f64
        }
    }

    /// Checks if there are any failed compactions
    pub fn has_failed_compactions(&self) -> bool {
        self.failed_compactions > 0
    }

    /// Gets the most common compaction strategy
    pub fn primary_compaction_strategy(&self) -> Option<String> {
        self.strategy_distribution.iter().max_by_key(|&(_, &count)| count).map(|(strategy, _)| strategy.clone())
    }

    /// Calculates the average progress of detailed tasks
    pub fn average_task_progress(&self) -> f64 {
        let tasks = self.detailed_tasks.as_deref().unwrap_or(&[]);
        if tasks.is_empty() {
            return 0.0;
        }
        let total: f64 = tasks.iter().map(|t| t.progress_percentage).sum();
        total / tasks.len() as f64
    }

    /// Gets keyspace with highest pending compactions
    pub fn keyspace_with_highest_pending(&self) -> Option<&CassandraKeyspaceCompactionMetrics> {
        self.keyspace_metrics.iter().max_by_key(|k| k.pending_compactions)
    }

    /// Calculates total pending bytes in MB
    pub fn pending_bytes_mb(&self) -> f64 {
        self.pending_bytes as f64 / Self::BYTES_TO_MB
    }

    /// Calculates total compacted bytes in MB
    pub fn compacted_bytes_mb(&self) -> f64 {
        self.compacted_bytes as f64 / Self::BYTES_TO_MB
    }

    /// Extract compaction strategy class name from the `compaction` frozen map column.
    fn extract_compaction_strategy(row: &Value) -> String {
        if let Some(obj) = row.get("compaction").and_then(|v| v.as_object())
            && let Some(class_str) = obj.get("class").and_then(|v| v.as_str())
        {
            return class_str.split('.').next_back().unwrap_or(class_str).to_string();
        }
        "Unknown".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn cutoff_now() -> chrono::DateTime<Utc> {
        Utc::now() - chrono::Duration::hours(1)
    }

    #[test]
    fn test_compaction_load_detection() {
        let mut compaction_info = CassandraCompactionInfo {
            total_pending_compactions: 60,
            tables_with_high_pending: 3,
            ..Default::default()
        };

        assert!(compaction_info.has_high_compaction_load());

        compaction_info.total_pending_compactions = 10;
        compaction_info.tables_with_high_pending = 6;
        assert!(compaction_info.has_high_compaction_load());
    }

    #[test]
    fn test_compaction_efficiency() {
        let mut compaction_info = CassandraCompactionInfo {
            pending_bytes: 1000,
            compacted_bytes: 800,
            ..Default::default()
        };

        assert_eq!(compaction_info.compaction_efficiency_ratio(), 0.8);

        compaction_info.pending_bytes = 0;
        assert_eq!(compaction_info.compaction_efficiency_ratio(), 1.0);
    }

    #[test]
    fn test_strategy_distribution() {
        let mut strategy_distribution = HashMap::new();
        strategy_distribution.insert("SizeTieredCompactionStrategy".to_string(), 10);
        strategy_distribution.insert("LeveledCompactionStrategy".to_string(), 5);
        strategy_distribution.insert("TimeWindowCompactionStrategy".to_string(), 3);

        let compaction_info = CassandraCompactionInfo { strategy_distribution, ..Default::default() };

        assert_eq!(compaction_info.primary_compaction_strategy(), Some("SizeTieredCompactionStrategy".to_string()));
    }

    #[test]
    fn test_bytes_conversion() {
        let compaction_info = CassandraCompactionInfo {
            pending_bytes: 1024 * 1024 * 100,
            compacted_bytes: 1024 * 1024 * 80,
            ..Default::default()
        };

        assert_eq!(compaction_info.pending_bytes_mb(), 100.0);
        assert_eq!(compaction_info.compacted_bytes_mb(), 80.0);
    }

    #[test]
    fn test_process_compaction_history_recent_rows() {
        // Use timestamps that are clearly within the last hour.
        let recent = Utc::now().to_rfc3339();
        let history_data = json!([
            {
                "keyspace_name": "test_ks",
                "table_name": "test_table",
                "bytes_in": 1000000_u64,
                "bytes_out": 800000_u64,
                "compacted_at": recent
            },
            {
                "keyspace_name": "test_ks2",
                "table_name": "test_table2",
                "bytes_in": 2000000_u64,
                "bytes_out": 1600000_u64,
                "compacted_at": recent
            }
        ]);

        let mut info = CassandraCompactionInfo::default();
        CassandraCompactionInfo::process_compaction_history(&mut info, &history_data, cutoff_now());

        assert_eq!(info.completed_compactions, 2);
        assert_eq!(info.compacted_bytes, 2400000); // 800000 + 1600000
        assert!(info.avg_compaction_rate_mb_per_sec > 0.0);
    }

    #[test]
    fn test_process_compaction_history_old_rows_filtered() {
        // Timestamps clearly older than one hour should be excluded.
        let old = "2000-01-01T00:00:00Z";
        let history_data = json!([
            {
                "keyspace_name": "test_ks",
                "table_name": "test_table",
                "bytes_in": 1000000_u64,
                "bytes_out": 800000_u64,
                "compacted_at": old
            }
        ]);

        let mut info = CassandraCompactionInfo::default();
        CassandraCompactionInfo::process_compaction_history(&mut info, &history_data, cutoff_now());

        assert_eq!(info.completed_compactions, 0);
        assert_eq!(info.compacted_bytes, 0);
    }

    #[test]
    fn test_keyspace_metrics_building() {
        let table_data = json!([
            {
                "keyspace_name": "test_ks",
                "table_name": "table1",
                "compaction": {"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"}
            },
            {
                "keyspace_name": "test_ks",
                "table_name": "table2",
                "compaction": {"class": "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"}
            }
        ]);

        let pending_data = json!([
            {
                "keyspace_name": "test_ks",
                "table_name": "table1",
                "total": 1000_u64,
                "completed": 100_u64
            }
        ]);

        let history_data = json!([]);

        let metrics = CassandraCompactionInfo::build_keyspace_metrics(&history_data, &pending_data, &table_data, cutoff_now());

        assert_eq!(metrics.len(), 1);
        let ks = &metrics[0];
        assert_eq!(ks.keyspace_name, "test_ks");
        assert_eq!(ks.table_count, 2);
        assert_eq!(ks.pending_compactions, 1);
        assert_eq!(ks.pending_bytes, 1000);
    }

    #[test]
    fn test_build_detailed_tasks_progress() {
        let pending_data = json!([
            {
                "keyspace_name": "ks1",
                "table_name": "t1",
                "task_type": "COMPACTION",
                "compaction_id": "abc-123",
                "total": 1000_u64,
                "completed": 500_u64
            },
            {
                "keyspace_name": "ks2",
                "table_name": "t2",
                "task_type": "COMPACTION",
                "compaction_id": "def-456",
                "total": 0_u64,
                "completed": 0_u64
            }
        ]);

        let tasks = CassandraCompactionInfo::build_detailed_tasks(&pending_data);

        assert_eq!(tasks.len(), 2);

        let t1 = &tasks[0];
        assert_eq!(t1.task_id, "abc-123");
        assert_eq!(t1.keyspace_name, "ks1");
        assert_eq!(t1.total_bytes, 1000);
        assert_eq!(t1.compacted_bytes, 500);
        assert_eq!(t1.progress_percentage, 50.0);

        let t2 = &tasks[1];
        assert_eq!(t2.progress_percentage, 0.0);
    }

    #[test]
    fn test_process_sstable_tasks_counts_only_compaction_kind() {
        let tasks_data = json!([
            {"keyspace_name": "ks1", "table_name": "t1", "task_id": "id-1",
             "kind": "compaction", "progress": 45.0_f64, "total": 2000_u64, "unit": "bytes"},
            {"keyspace_name": "ks1", "table_name": "t2", "task_id": "id-2",
             "kind": "cleanup",    "progress": 10.0_f64, "total": 500_u64,  "unit": "bytes"},
            {"keyspace_name": "ks2", "table_name": "t3", "task_id": "id-3",
             "kind": "compaction", "progress": 80.0_f64, "total": 4000_u64, "unit": "bytes"},
            {"keyspace_name": "ks2", "table_name": "t4", "task_id": "id-4",
             "kind": "scrub",      "progress": 0.0_f64,  "total": 1000_u64, "unit": "bytes"},
        ]);

        let mut info = CassandraCompactionInfo::default();
        CassandraCompactionInfo::process_sstable_tasks(&mut info, &tasks_data);

        // Only the two "compaction" rows should be counted.
        assert_eq!(info.active_compactions, 2);
    }

    #[test]
    fn test_process_sstable_tasks_empty_array() {
        let tasks_data = json!([]);
        let mut info = CassandraCompactionInfo::default();
        CassandraCompactionInfo::process_sstable_tasks(&mut info, &tasks_data);
        assert_eq!(info.active_compactions, 0);
    }

    #[test]
    fn test_process_sstable_tasks_non_array_is_noop() {
        let tasks_data = json!(null);
        let mut info = CassandraCompactionInfo::default();
        CassandraCompactionInfo::process_sstable_tasks(&mut info, &tasks_data);
        assert_eq!(info.active_compactions, 0);
    }

    #[test]
    fn test_build_detailed_tasks_from_virtual() {
        let tasks_data = json!([
            {
                "keyspace_name": "ks1",
                "table_name": "t1",
                "task_id": "uuid-aaa",
                "kind": "compaction",
                "progress": 50.0_f64,
                "total": 8000_u64,
                "unit": "bytes"
            },
            {
                "keyspace_name": "ks2",
                "table_name": "t2",
                "task_id": "uuid-bbb",
                "kind": "cleanup",
                "progress": 0.0_f64,
                "total": 0_u64,
                "unit": "bytes"
            }
        ]);

        let tasks = CassandraCompactionInfo::build_detailed_tasks_from_virtual(&tasks_data);

        assert_eq!(tasks.len(), 2);

        let t1 = &tasks[0];
        assert_eq!(t1.task_id, "uuid-aaa");
        assert_eq!(t1.keyspace_name, "ks1");
        assert_eq!(t1.table_name, "t1");
        assert_eq!(t1.compaction_type, "COMPACTION");
        assert_eq!(t1.status, "ACTIVE");
        assert_eq!(t1.progress_percentage, 50.0);
        assert_eq!(t1.total_bytes, 8000);
        // 50 % of 8000 = 4000
        assert_eq!(t1.compacted_bytes, 4000);

        let t2 = &tasks[1];
        assert_eq!(t2.task_id, "uuid-bbb");
        assert_eq!(t2.compaction_type, "CLEANUP");
        assert_eq!(t2.progress_percentage, 0.0);
        assert_eq!(t2.total_bytes, 0);
        assert_eq!(t2.compacted_bytes, 0);
    }
}
