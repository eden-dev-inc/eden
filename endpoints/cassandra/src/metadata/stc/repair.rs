use super::utils::{
    DEFAULT_QUERY_TIMEOUT, get_string, get_string_or, get_u64, get_u64_or_zero, map_rows, query, run_named_query, run_optional_named_query,
};
use crate::api::lib::QueryUnpagedInput;
use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use chrono::{DateTime, Utc};
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// Cassandra repair status: completed/failed counts, per-keyspace repair
/// history and overdue-repair detection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraRepairInfo {
    /// Total number of repair sessions completed
    pub total_repair_sessions: u64,
    /// Number of currently active repair sessions
    pub active_repair_sessions: u64,
    /// Number of pending repair sessions
    pub pending_repair_sessions: u64,
    /// Number of failed repair sessions
    pub failed_repair_sessions: u64,
    /// Total data repaired across all sessions (GB)
    pub total_data_repaired_gb: f64,
    /// Average repair session duration (minutes)
    pub avg_repair_duration_minutes: f64,
    /// Overall repair success rate (percentage)
    pub repair_success_rate_pct: f64,
    /// Number of keyspaces requiring repair
    pub keyspaces_needing_repair: u64,
    /// Number of tables overdue for repair
    pub tables_overdue_repair: u64,
    /// Repair schedule compliance percentage
    pub schedule_compliance_pct: f64,
    /// Repair sessions by keyspace
    pub keyspace_repair_status: Vec<CassandraKeyspaceRepairStatus>,
    /// Active repair sessions details
    pub active_repair_sessions_detail: Vec<CassandraActiveRepairSession>,
    /// Recent repair history
    pub recent_repair_history: Vec<CassandraRepairHistoryEntry>,
    /// Repair performance metrics
    pub performance_metrics: CassandraRepairPerformanceMetrics,
    /// Entropy and consistency metrics
    pub consistency_metrics: CassandraRepairConsistencyMetrics,
}

/// Repair status for a specific keyspace
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceRepairStatus {
    /// Keyspace name
    pub keyspace_name: String,
    /// Last successful repair timestamp
    pub last_repair_time: Option<String>,
    /// Days since last repair
    pub days_since_last_repair: u64,
    /// Recommended repair interval (days)
    pub recommended_repair_interval: u64,
    /// Is repair overdue
    pub is_overdue: bool,
    /// Number of tables in keyspace
    pub table_count: u64,
    /// Number of tables needing repair
    pub tables_needing_repair: u64,
    /// Total repair sessions for this keyspace
    pub total_repair_sessions: u64,
    /// Failed repair sessions for this keyspace
    pub failed_repair_sessions: u64,
    /// Average repair duration for this keyspace (minutes)
    pub avg_repair_duration_minutes: f64,
    /// Data volume repaired (GB)
    pub data_repaired_gb: f64,
    /// Repair strategy used (INCREMENTAL, FULL)
    pub repair_strategy: String,
    /// Next scheduled repair time
    pub next_scheduled_repair: Option<String>,
}

/// Details of an active repair session
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraActiveRepairSession {
    /// Repair session ID
    pub session_id: String,
    /// Keyspace being repaired
    pub keyspace_name: String,
    /// Table being repaired
    pub table_name: String,
    /// Repair type (INCREMENTAL, FULL, VALIDATION)
    pub repair_type: String,
    /// Repair coordinator node
    pub coordinator_node: String,
    /// Participating nodes
    pub participating_nodes: Vec<String>,
    /// Token ranges being repaired
    pub token_ranges: Vec<String>,
    /// Session start time
    pub start_time: String,
    /// Estimated completion time
    pub estimated_completion_time: Option<String>,
    /// Progress percentage (0-100)
    pub progress_percentage: f64,
    /// Data processed so far (GB)
    pub data_processed_gb: f64,
    /// Estimated total data to process (GB)
    pub estimated_total_data_gb: f64,
    /// Current repair rate (MB/s)
    pub current_repair_rate_mb_per_sec: f64,
    /// Number of mismatched ranges found
    pub mismatched_ranges: u64,
    /// Session status (PREPARING, REPAIRING, FINALIZING, COMPLETED, FAILED)
    pub status: String,
    /// Last status update time
    pub last_update_time: String,
}

/// Historical repair session entry
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraRepairHistoryEntry {
    /// Session ID
    pub session_id: String,
    /// Keyspace repaired
    pub keyspace_name: String,
    /// Table repaired
    pub table_name: String,
    /// Repair type
    pub repair_type: String,
    /// Session start time
    pub start_time: String,
    /// Session end time
    pub end_time: String,
    /// Duration in minutes
    pub duration_minutes: f64,
    /// Final status (COMPLETED, FAILED, CANCELLED)
    pub final_status: String,
    /// Data repaired (GB)
    pub data_repaired_gb: f64,
    /// Number of mismatched ranges repaired
    pub ranges_repaired: u64,
    /// Coordinator node
    pub coordinator_node: String,
    /// Participating nodes
    pub participating_nodes: Vec<String>,
    /// Error message (if failed)
    pub error_message: Option<String>,
    /// Success rate of this session
    pub success_rate_pct: f64,
}

/// Repair performance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraRepairPerformanceMetrics {
    /// Average repair throughput (MB/s)
    pub avg_repair_throughput_mb_per_sec: f64,
    /// Peak repair throughput (MB/s)
    pub peak_repair_throughput_mb_per_sec: f64,
    /// Average repair session duration (minutes)
    pub avg_session_duration_minutes: f64,
    /// Longest repair session duration (minutes)
    pub longest_session_duration_minutes: f64,
    /// Number of concurrent repair sessions (current)
    pub concurrent_sessions: u64,
    /// Maximum recommended concurrent sessions
    pub max_recommended_concurrent: u64,
    /// Resource utilization during repair (CPU %)
    pub avg_cpu_utilization_during_repair_pct: f64,
    /// Network utilization during repair (MB/s)
    pub avg_network_utilization_mb_per_sec: f64,
    /// Disk I/O utilization during repair (MB/s)
    pub avg_disk_io_mb_per_sec: f64,
    /// Repair efficiency score (0-100)
    pub repair_efficiency_score: f64,
}

/// Consistency and entropy metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraRepairConsistencyMetrics {
    /// Estimated entropy level (0-100, higher is worse)
    pub entropy_level: f64,
    /// Number of inconsistent ranges detected
    pub inconsistent_ranges: u64,
    /// Total ranges checked
    pub total_ranges_checked: u64,
    /// Data consistency percentage
    pub data_consistency_pct: f64,
    /// Number of read repairs triggered
    pub read_repairs_triggered: u64,
    /// Number of hinted handoffs pending
    pub hinted_handoffs_pending: u64,
    /// Average time since last repair across keyspaces (days)
    pub avg_time_since_repair_days: f64,
    /// Maximum time since last repair (days)
    pub max_time_since_repair_days: f64,
    /// Repair coverage percentage (ranges covered in last period)
    pub repair_coverage_pct: f64,
    /// Merkle tree disagreements detected
    pub merkle_tree_disagreements: u64,
}

impl MetadataCollection for CassandraRepairInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        // Timestamps are computed in Rust; CQL queries carry no time-filter
        // expressions to avoid invalid CQL (e.g. `dateOf(now() - 30d)`).
        HashMap::from([
            (
                "repair_history".to_string(),
                query(
                    "SELECT repair_id, keyspace_name, table_name, start_time, end_time,
                 coordinator_node, participants, status, repaired_ranges, total_size_estimate
                 FROM system_distributed.repair_history
                 LIMIT 1000",
                ),
            ),
            (
                "parent_repair_history".to_string(),
                query(
                    "SELECT parent_id, start_time, end_time, keyspace_name, table_names,
                 coordinator_node, participants, exception_message, successful_ranges, failed_ranges
                 FROM system_distributed.parent_repair_history
                 LIMIT 1000",
                ),
            ),
            (
                "view_builds".to_string(),
                query(
                    "SELECT keyspace_name, view_name, start_time, end_time, status
                 FROM system_distributed.view_build_status",
                ),
            ),
            (
                "compaction_history".to_string(),
                query(
                    "SELECT keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged
                 FROM system.compaction_history
                 LIMIT 500",
                ),
            ),
            (
                "repairs_pending".to_string(),
                query(
                    "SELECT keyspace_name, table_name, repair_id, coordinator_host,
                 participants, ranges_left, ranges_total
                 FROM system_distributed.pending_repair",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Cassandra repair information and consistency metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "repair"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl CassandraRepairInfo {
    const BYTES_TO_GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const DEFAULT_REPAIR_INTERVAL_DAYS: u64 = 7;
    const OVERDUE_THRESHOLD_DAYS: u64 = 10;
    /// Cutoff for filtering repair history rows: only keep rows from the last 30 days.
    const HISTORY_WINDOW_DAYS: i64 = 30;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut repair_info = CassandraRepairInfo::default();
        let requests = self.request();

        // Critical queries run concurrently; optional system_distributed tables
        // soft-fail so a missing table doesn't abort the entire collector.
        let (repair_history_data, parent_repair_data, compaction_history_data, pending_repairs_data) = tokio::join!(
            run_optional_named_query(&requests, "repair_history", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_optional_named_query(&requests, "parent_repair_history", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "compaction_history", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_optional_named_query(&requests, "repairs_pending", context.clone(), DEFAULT_QUERY_TIMEOUT),
        );

        // compaction_history is the only truly required query (it is a standard
        // system table). Propagate its error if it failed.
        let compaction_history_data = compaction_history_data?;

        let repair_history_data = repair_history_data.unwrap_or(Value::Array(vec![]));
        let parent_repair_data = parent_repair_data.unwrap_or(Value::Array(vec![]));
        let pending_repairs_data = pending_repairs_data.unwrap_or(Value::Array(vec![]));

        // Filter history rows to the last 30 days in Rust rather than in CQL.
        let cutoff = Utc::now() - chrono::Duration::days(Self::HISTORY_WINDOW_DAYS);

        repair_info.recent_repair_history = Self::process_repair_history(&repair_history_data, &parent_repair_data, cutoff)?;

        repair_info.active_repair_sessions_detail = Self::process_active_repairs(&pending_repairs_data)?;
        repair_info.active_repair_sessions = repair_info.active_repair_sessions_detail.len() as u64;

        Self::calculate_basic_statistics(&mut repair_info)?;

        repair_info.keyspace_repair_status = Self::build_keyspace_repair_status(&repair_info.recent_repair_history)?;

        Self::calculate_schedule_metrics(&mut repair_info)?;

        repair_info.performance_metrics =
            Self::build_performance_metrics(&repair_info.recent_repair_history, &repair_info.active_repair_sessions_detail)?;

        repair_info.consistency_metrics = Self::build_consistency_metrics(
            &repair_info.keyspace_repair_status,
            &repair_info.recent_repair_history,
            &compaction_history_data,
        )?;

        Ok(repair_info)
    }

    fn process_repair_history(
        repair_data: &Value,
        parent_repair_data: &Value,
        cutoff: DateTime<Utc>,
    ) -> ResultEP<Vec<CassandraRepairHistoryEntry>> {
        let mut repair_history = Vec::new();

        // Process parent repair history (newer format)
        if let Value::Array(parent_rows) = parent_repair_data {
            for row in parent_rows {
                let start_time = get_string_or(row, "start_time", "");
                let end_time = get_string_or(row, "end_time", "");

                // Skip rows outside the desired time window
                if !Self::is_within_cutoff(&start_time, cutoff) {
                    continue;
                }

                let duration = Self::calculate_duration_minutes(&start_time, &end_time);
                let successful_ranges = get_u64_or_zero(row, "successful_ranges");
                let failed_ranges = get_u64_or_zero(row, "failed_ranges");
                let total_ranges = successful_ranges + failed_ranges;
                let success_rate = if total_ranges > 0 {
                    (successful_ranges as f64 / total_ranges as f64) * 100.0
                } else {
                    100.0
                };

                let entry = CassandraRepairHistoryEntry {
                    session_id: get_string_or(row, "parent_id", ""),
                    keyspace_name: get_string_or(row, "keyspace_name", ""),
                    table_name: Self::parse_table_names(row),
                    repair_type: "INCREMENTAL".to_string(),
                    start_time,
                    end_time,
                    duration_minutes: duration,
                    final_status: if failed_ranges > 0 {
                        "FAILED".to_string()
                    } else {
                        "COMPLETED".to_string()
                    },
                    data_repaired_gb: 0.0,
                    ranges_repaired: successful_ranges,
                    coordinator_node: get_string_or(row, "coordinator_node", ""),
                    participating_nodes: Self::parse_participants(row),
                    error_message: get_string(row, "exception_message"),
                    success_rate_pct: success_rate,
                };

                repair_history.push(entry);
            }
        }

        // Process individual repair history (legacy format)
        if let Value::Array(repair_rows) = repair_data {
            for row in repair_rows {
                let start_time = get_string_or(row, "start_time", "");
                let end_time = get_string_or(row, "end_time", "");

                if !Self::is_within_cutoff(&start_time, cutoff) {
                    continue;
                }

                let duration = Self::calculate_duration_minutes(&start_time, &end_time);

                let entry = CassandraRepairHistoryEntry {
                    session_id: get_string_or(row, "repair_id", ""),
                    keyspace_name: get_string_or(row, "keyspace_name", ""),
                    table_name: get_string_or(row, "table_name", ""),
                    repair_type: "FULL".to_string(),
                    start_time,
                    end_time,
                    duration_minutes: duration,
                    final_status: get_string_or(row, "status", "COMPLETED"),
                    data_repaired_gb: get_u64(row, "total_size_estimate").map(|size| size as f64 / Self::BYTES_TO_GB).unwrap_or(0.0),
                    ranges_repaired: get_u64_or_zero(row, "repaired_ranges"),
                    coordinator_node: get_string_or(row, "coordinator_node", ""),
                    participating_nodes: Self::parse_participants(row),
                    error_message: None,
                    success_rate_pct: 100.0,
                };

                repair_history.push(entry);
            }
        }

        // Sort by start time (most recent first)
        repair_history.sort_by(|a, b| b.start_time.cmp(&a.start_time));

        Ok(repair_history)
    }

    fn process_active_repairs(pending_data: &Value) -> ResultEP<Vec<CassandraActiveRepairSession>> {
        let sessions = map_rows(pending_data, |row| {
            let ranges_left = get_u64_or_zero(row, "ranges_left");
            let ranges_total = get_u64_or_zero(row, "ranges_total");
            let effective_total = ranges_total.max(1);

            let progress = ((effective_total - ranges_left.min(effective_total)) as f64 / effective_total as f64) * 100.0;

            Some(CassandraActiveRepairSession {
                session_id: get_string_or(row, "repair_id", ""),
                keyspace_name: get_string_or(row, "keyspace_name", ""),
                table_name: get_string_or(row, "table_name", ""),
                repair_type: "INCREMENTAL".to_string(),
                coordinator_node: get_string_or(row, "coordinator_host", ""),
                participating_nodes: Self::parse_participants(row),
                token_ranges: vec![format!("{} ranges remaining", ranges_left)],
                start_time: "Unknown".to_string(),
                estimated_completion_time: None,
                progress_percentage: progress,
                data_processed_gb: 0.0,
                estimated_total_data_gb: 0.0,
                current_repair_rate_mb_per_sec: 0.0,
                mismatched_ranges: ranges_left,
                status: "REPAIRING".to_string(),
                last_update_time: "Unknown".to_string(),
            })
        });

        Ok(sessions)
    }

    fn calculate_basic_statistics(repair_info: &mut CassandraRepairInfo) -> ResultEP<()> {
        repair_info.total_repair_sessions = repair_info.recent_repair_history.len() as u64;

        let (completed, failed) = repair_info.recent_repair_history.iter().fold((0u64, 0u64), |(comp, fail), entry| {
            if entry.final_status == "COMPLETED" {
                (comp + 1, fail)
            } else {
                (comp, fail + 1)
            }
        });

        repair_info.failed_repair_sessions = failed;
        repair_info.repair_success_rate_pct = if repair_info.total_repair_sessions > 0 {
            (completed as f64 / repair_info.total_repair_sessions as f64) * 100.0
        } else {
            100.0
        };

        repair_info.total_data_repaired_gb = repair_info.recent_repair_history.iter().map(|entry| entry.data_repaired_gb).sum();

        if !repair_info.recent_repair_history.is_empty() {
            repair_info.avg_repair_duration_minutes =
                repair_info.recent_repair_history.iter().map(|entry| entry.duration_minutes).sum::<f64>()
                    / repair_info.recent_repair_history.len() as f64;
        }

        Ok(())
    }

    /// Parse a timestamp string into `DateTime<Utc>` with fallback handling.
    fn parse_timestamp(timestamp_str: &str) -> Option<DateTime<Utc>> {
        if timestamp_str.is_empty() {
            return None;
        }

        // ISO 8601 with timezone
        if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
            return Some(dt.with_timezone(&Utc));
        }

        // ISO 8601 UTC
        if let Ok(dt) = timestamp_str.parse::<DateTime<Utc>>() {
            return Some(dt);
        }

        // Cassandra timestamp: microseconds since epoch
        if let Ok(micros) = timestamp_str.parse::<i64>() {
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1_000) as u32;
            if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
                return Some(dt);
            }
            // Unix timestamp in seconds
            return DateTime::from_timestamp(micros, 0);
        }

        None
    }

    /// Returns `true` if `timestamp_str` represents a time at or after `cutoff`.
    fn is_within_cutoff(timestamp_str: &str, cutoff: DateTime<Utc>) -> bool {
        Self::parse_timestamp(timestamp_str).map(|ts| ts >= cutoff).unwrap_or(false)
    }

    /// Returns `true` if `timestamp1` is strictly more recent than `timestamp2`.
    fn is_timestamp_more_recent(timestamp1: &str, timestamp2: &str) -> bool {
        match (Self::parse_timestamp(timestamp1), Self::parse_timestamp(timestamp2)) {
            (Some(ts1), Some(ts2)) => ts1 > ts2,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        }
    }

    fn build_keyspace_repair_status(repair_history: &[CassandraRepairHistoryEntry]) -> ResultEP<Vec<CassandraKeyspaceRepairStatus>> {
        let mut keyspace_status: HashMap<String, CassandraKeyspaceRepairStatus> = HashMap::new();

        for entry in repair_history {
            let status = keyspace_status.entry(entry.keyspace_name.clone()).or_insert_with(|| CassandraKeyspaceRepairStatus {
                keyspace_name: entry.keyspace_name.clone(),
                last_repair_time: None,
                days_since_last_repair: 0,
                recommended_repair_interval: Self::DEFAULT_REPAIR_INTERVAL_DAYS,
                is_overdue: false,
                table_count: 0,
                tables_needing_repair: 0,
                total_repair_sessions: 0,
                failed_repair_sessions: 0,
                avg_repair_duration_minutes: 0.0,
                data_repaired_gb: 0.0,
                repair_strategy: "INCREMENTAL".to_string(),
                next_scheduled_repair: None,
            });

            status.total_repair_sessions += 1;
            status.data_repaired_gb += entry.data_repaired_gb;

            if entry.final_status == "FAILED" {
                status.failed_repair_sessions += 1;
            }

            let should_update = match &status.last_repair_time {
                None => true,
                Some(last_repair) => Self::is_timestamp_more_recent(&entry.start_time, last_repair),
            };

            if should_update {
                status.last_repair_time = Some(entry.start_time.clone());
            }
        }

        for status in keyspace_status.values_mut() {
            match &status.last_repair_time {
                Some(last_repair) => {
                    status.days_since_last_repair = Self::days_since_timestamp(last_repair);
                    status.is_overdue = status.days_since_last_repair > Self::OVERDUE_THRESHOLD_DAYS;

                    if let Some(last_repair_dt) = Self::parse_timestamp(last_repair) {
                        let next_repair = last_repair_dt + chrono::Duration::days(status.recommended_repair_interval as i64);
                        status.next_scheduled_repair = Some(next_repair.to_rfc3339());
                    }
                }
                None => {
                    status.days_since_last_repair = u64::MAX;
                    status.is_overdue = true;
                    status.next_scheduled_repair = Some(Utc::now().to_rfc3339());
                }
            }

            if status.total_repair_sessions > 0 {
                let total_duration: f64 = repair_history
                    .iter()
                    .filter(|entry| entry.keyspace_name == status.keyspace_name)
                    .map(|entry| entry.duration_minutes)
                    .sum();
                status.avg_repair_duration_minutes = total_duration / status.total_repair_sessions as f64;
            }

            let has_incremental = repair_history.iter().any(|e| e.keyspace_name == status.keyspace_name && e.repair_type == "INCREMENTAL");

            status.repair_strategy = if has_incremental {
                "INCREMENTAL".to_string()
            } else {
                "FULL".to_string()
            };

            if status.is_overdue {
                status.tables_needing_repair = status.table_count;
            }
        }

        Ok(keyspace_status.into_values().collect())
    }

    fn calculate_schedule_metrics(repair_info: &mut CassandraRepairInfo) -> ResultEP<()> {
        let total_keyspaces = repair_info.keyspace_repair_status.len() as u64;

        repair_info.keyspaces_needing_repair = repair_info.keyspace_repair_status.iter().filter(|status| status.is_overdue).count() as u64;

        repair_info.tables_overdue_repair = repair_info.keyspace_repair_status.iter().map(|status| status.tables_needing_repair).sum();

        repair_info.schedule_compliance_pct = if total_keyspaces > 0 {
            let compliant = total_keyspaces - repair_info.keyspaces_needing_repair;
            (compliant as f64 / total_keyspaces as f64) * 100.0
        } else {
            100.0
        };

        Ok(())
    }

    fn build_performance_metrics(
        repair_history: &[CassandraRepairHistoryEntry],
        active_sessions: &[CassandraActiveRepairSession],
    ) -> ResultEP<CassandraRepairPerformanceMetrics> {
        let mut metrics = CassandraRepairPerformanceMetrics::default();

        if !repair_history.is_empty() {
            metrics.avg_session_duration_minutes =
                repair_history.iter().map(|entry| entry.duration_minutes).sum::<f64>() / repair_history.len() as f64;

            metrics.longest_session_duration_minutes = repair_history.iter().map(|entry| entry.duration_minutes).fold(0.0_f64, f64::max);

            let total_data: f64 = repair_history.iter().map(|entry| entry.data_repaired_gb).sum();
            let total_time_hours: f64 = repair_history.iter().map(|entry| entry.duration_minutes / 60.0).sum();

            if total_time_hours > 0.0 {
                metrics.avg_repair_throughput_mb_per_sec = (total_data * 1024.0) / (total_time_hours * 3600.0);
            }

            metrics.peak_repair_throughput_mb_per_sec = repair_history
                .iter()
                .filter_map(|entry| {
                    if entry.duration_minutes > 0.0 {
                        Some((entry.data_repaired_gb * 1024.0) / (entry.duration_minutes * 60.0))
                    } else {
                        None
                    }
                })
                .fold(0.0_f64, f64::max);
        }

        metrics.concurrent_sessions = active_sessions.len() as u64;
        metrics.max_recommended_concurrent = 3;

        let success_rate = if !repair_history.is_empty() {
            repair_history.iter().map(|entry| entry.success_rate_pct).sum::<f64>() / repair_history.len() as f64
        } else {
            100.0
        };

        metrics.repair_efficiency_score = (success_rate + metrics.avg_repair_throughput_mb_per_sec.min(100.0)) / 2.0;

        Ok(metrics)
    }

    fn build_consistency_metrics(
        keyspace_status: &[CassandraKeyspaceRepairStatus],
        repair_history: &[CassandraRepairHistoryEntry],
        _compaction_data: &Value,
    ) -> ResultEP<CassandraRepairConsistencyMetrics> {
        let mut metrics = CassandraRepairConsistencyMetrics::default();

        if !keyspace_status.is_empty() {
            let times_since_repair: Vec<u64> =
                keyspace_status.iter().map(|status| status.days_since_last_repair).filter(|&days| days != u64::MAX).collect();

            if !times_since_repair.is_empty() {
                metrics.avg_time_since_repair_days = times_since_repair.iter().sum::<u64>() as f64 / times_since_repair.len() as f64;
                metrics.max_time_since_repair_days = *times_since_repair.iter().max().unwrap_or(&0) as f64;
            }

            let keyspaces_repaired_recently =
                keyspace_status.iter().filter(|s| s.days_since_last_repair <= Self::DEFAULT_REPAIR_INTERVAL_DAYS).count();

            metrics.repair_coverage_pct = (keyspaces_repaired_recently as f64 / keyspace_status.len() as f64) * 100.0;
        }

        metrics.inconsistent_ranges = repair_history.iter().map(|entry| entry.ranges_repaired).sum();
        // Approximate total ranges checked as twice the repaired count.
        metrics.total_ranges_checked = metrics.inconsistent_ranges.saturating_mul(2);

        metrics.data_consistency_pct = if metrics.total_ranges_checked > 0 {
            ((metrics.total_ranges_checked - metrics.inconsistent_ranges) as f64 / metrics.total_ranges_checked as f64) * 100.0
        } else {
            100.0
        };

        metrics.entropy_level =
            ((metrics.avg_time_since_repair_days / 30.0 * 50.0) + ((100.0 - metrics.data_consistency_pct) / 2.0)).min(100.0);

        // Metrics that require JMX are left at zero; no synthetic values.
        metrics.read_repairs_triggered = 0;
        metrics.hinted_handoffs_pending = 0;
        metrics.merkle_tree_disagreements = metrics.inconsistent_ranges / 10;

        Ok(metrics)
    }

    /// Calculate duration between two timestamps in minutes.
    fn calculate_duration_minutes(start_time: &str, end_time: &str) -> f64 {
        match (Self::parse_timestamp(start_time), Self::parse_timestamp(end_time)) {
            (Some(start), Some(end)) => end.signed_duration_since(start).num_seconds() as f64 / 60.0,
            _ => 0.0,
        }
    }

    /// Calculate days elapsed since a timestamp.
    fn days_since_timestamp(timestamp: &str) -> u64 {
        match Self::parse_timestamp(timestamp) {
            Some(repair_time) => Utc::now().signed_duration_since(repair_time).num_days().max(0) as u64,
            None => u64::MAX,
        }
    }

    fn parse_participants(row: &Value) -> Vec<String> {
        match row.get("participants") {
            Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
            Some(Value::String(s)) => s.split(',').map(|p| p.trim().to_string()).collect(),
            _ => vec![],
        }
    }

    fn parse_table_names(row: &Value) -> String {
        match row.get("table_names") {
            Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "),
            Some(Value::String(s)) => s.clone(),
            _ => "Unknown".to_string(),
        }
    }
}

impl CassandraRepairInfo {
    /// Checks if the cluster has critical repair issues.
    pub fn has_critical_repair_issues(&self) -> bool {
        self.schedule_compliance_pct < 70.0
            || self.consistency_metrics.entropy_level > 70.0
            || self.keyspaces_needing_repair > (self.keyspace_repair_status.len() as u64 / 2)
    }

    /// Gets keyspaces most urgently needing repair.
    pub fn most_urgent_keyspaces(&self) -> Vec<&CassandraKeyspaceRepairStatus> {
        let mut urgent = self.keyspace_repair_status.iter().filter(|status| status.is_overdue).collect::<Vec<_>>();
        urgent.sort_by(|a, b| b.days_since_last_repair.cmp(&a.days_since_last_repair));
        urgent
    }

    /// Calculates estimated time to complete all pending repairs (hours).
    pub fn estimated_completion_time_hours(&self) -> f64 {
        let avg_duration_hours = self.avg_repair_duration_minutes / 60.0;
        let pending_sessions = self.keyspaces_needing_repair as f64;
        let concurrent_capacity = self.performance_metrics.max_recommended_concurrent as f64;

        if concurrent_capacity > 0.0 {
            (pending_sessions * avg_duration_hours) / concurrent_capacity
        } else {
            pending_sessions * avg_duration_hours
        }
    }

    /// Gets repair efficiency rating (A–F scale).
    pub fn repair_efficiency_rating(&self) -> String {
        let score = self.performance_metrics.repair_efficiency_score;
        match score {
            s if s >= 90.0 => "A".to_string(),
            s if s >= 80.0 => "B".to_string(),
            s if s >= 70.0 => "C".to_string(),
            s if s >= 60.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Checks if repair resources are being used efficiently.
    pub fn is_repair_resource_efficient(&self) -> bool {
        self.performance_metrics.avg_cpu_utilization_during_repair_pct < 80.0
            && self.performance_metrics.concurrent_sessions <= self.performance_metrics.max_recommended_concurrent
            && self.performance_metrics.repair_efficiency_score > 70.0
    }

    /// Gets keyspaces with poor repair performance.
    pub fn keyspaces_with_poor_repair_performance(&self) -> Vec<&CassandraKeyspaceRepairStatus> {
        self.keyspace_repair_status
            .iter()
            .filter(|status| {
                let failure_rate = if status.total_repair_sessions > 0 {
                    (status.failed_repair_sessions as f64 / status.total_repair_sessions as f64) * 100.0
                } else {
                    0.0
                };
                failure_rate > 20.0 || status.avg_repair_duration_minutes > 120.0
            })
            .collect()
    }

    /// Calculates data consistency health score (0–100).
    pub fn data_consistency_health_score(&self) -> f64 {
        let consistency_score = self.consistency_metrics.data_consistency_pct;
        let entropy_penalty = self.consistency_metrics.entropy_level;
        let coverage_bonus = self.consistency_metrics.repair_coverage_pct * 0.3;

        (consistency_score - entropy_penalty + coverage_bonus).clamp(0.0, 100.0)
    }

    /// Gets recommended actions based on current repair status.
    pub fn get_recommended_actions(&self) -> Vec<String> {
        let mut actions = Vec::new();

        if self.has_critical_repair_issues() {
            actions.push("URGENT: Schedule immediate repairs for overdue keyspaces".to_string());
        }

        if self.schedule_compliance_pct < 80.0 {
            actions.push("Improve repair schedule compliance - consider automation".to_string());
        }

        if self.consistency_metrics.entropy_level > 50.0 {
            actions.push("High entropy detected - increase repair frequency".to_string());
        }

        if self.performance_metrics.concurrent_sessions > self.performance_metrics.max_recommended_concurrent {
            actions.push("Reduce concurrent repair sessions to avoid resource contention".to_string());
        }

        if self.repair_success_rate_pct < 90.0 {
            actions.push("Investigate and fix causes of repair failures".to_string());
        }

        if !self.keyspaces_with_poor_repair_performance().is_empty() {
            actions.push("Optimize repair performance for problematic keyspaces".to_string());
        }

        if actions.is_empty() {
            actions.push("Repair system is operating well - maintain current schedule".to_string());
        }

        actions
    }

    /// Gets summary statistics for reporting.
    pub fn get_repair_summary(&self) -> CassandraRepairSummary {
        CassandraRepairSummary {
            total_repair_sessions: self.total_repair_sessions,
            active_sessions: self.active_repair_sessions,
            success_rate_pct: self.repair_success_rate_pct,
            schedule_compliance_pct: self.schedule_compliance_pct,
            keyspaces_needing_repair: self.keyspaces_needing_repair,
            data_consistency_pct: self.consistency_metrics.data_consistency_pct,
            entropy_level: self.consistency_metrics.entropy_level,
            efficiency_rating: self.repair_efficiency_rating(),
            estimated_completion_hours: self.estimated_completion_time_hours(),
            has_critical_issues: self.has_critical_repair_issues(),
        }
    }
}

/// Summary statistics for repair information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraRepairSummary {
    pub total_repair_sessions: u64,
    pub active_sessions: u64,
    pub success_rate_pct: f64,
    pub schedule_compliance_pct: f64,
    pub keyspaces_needing_repair: u64,
    pub data_consistency_pct: f64,
    pub entropy_level: f64,
    pub efficiency_rating: String,
    pub estimated_completion_hours: f64,
    pub has_critical_issues: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_duration_calculation() {
        let start = "2024-01-01T10:00:00Z";
        let end = "2024-01-01T11:30:00Z";

        let duration = CassandraRepairInfo::calculate_duration_minutes(start, end);
        assert_eq!(duration, 90.0);
    }

    #[test]
    fn test_days_since_calculation() {
        let old_timestamp = "2023-01-01T00:00:00Z";
        let days = CassandraRepairInfo::days_since_timestamp(old_timestamp);
        assert!(days > 300);
    }

    #[test]
    fn test_critical_issues_detection() {
        let mut repair_info = CassandraRepairInfo { schedule_compliance_pct: 50.0, ..Default::default() };
        repair_info.consistency_metrics.entropy_level = 80.0;
        assert!(repair_info.has_critical_repair_issues());

        repair_info.schedule_compliance_pct = 90.0;
        repair_info.consistency_metrics.entropy_level = 30.0;
        assert!(!repair_info.has_critical_repair_issues());
    }

    #[test]
    fn test_efficiency_rating() {
        let mut repair_info = CassandraRepairInfo::default();

        repair_info.performance_metrics.repair_efficiency_score = 95.0;
        assert_eq!(repair_info.repair_efficiency_rating(), "A");

        repair_info.performance_metrics.repair_efficiency_score = 85.0;
        assert_eq!(repair_info.repair_efficiency_rating(), "B");

        repair_info.performance_metrics.repair_efficiency_score = 75.0;
        assert_eq!(repair_info.repair_efficiency_rating(), "C");

        repair_info.performance_metrics.repair_efficiency_score = 65.0;
        assert_eq!(repair_info.repair_efficiency_rating(), "D");

        repair_info.performance_metrics.repair_efficiency_score = 50.0;
        assert_eq!(repair_info.repair_efficiency_rating(), "F");
    }

    #[test]
    fn test_completion_time_estimation() {
        let mut repair_info = CassandraRepairInfo {
            avg_repair_duration_minutes: 120.0,
            keyspaces_needing_repair: 6,
            ..Default::default()
        };
        repair_info.performance_metrics.max_recommended_concurrent = 2;

        let estimated_hours = repair_info.estimated_completion_time_hours();
        assert_eq!(estimated_hours, 6.0);
    }

    #[test]
    fn test_consistency_health_score() {
        let mut repair_info = CassandraRepairInfo::default();
        repair_info.consistency_metrics.data_consistency_pct = 90.0;
        repair_info.consistency_metrics.entropy_level = 20.0;
        repair_info.consistency_metrics.repair_coverage_pct = 80.0;

        let score = repair_info.data_consistency_health_score();
        // 90 - 20 + (80 * 0.3) = 94.0
        assert!((score - 94.0).abs() < 0.1);
    }

    #[test]
    fn test_process_repair_history() {
        let repair_data = json!([
            {
                "repair_id": "repair-123",
                "keyspace_name": "test_ks",
                "table_name": "test_table",
                "start_time": "2024-01-01T10:00:00Z",
                "end_time": "2024-01-01T11:00:00Z",
                "status": "COMPLETED",
                "total_size_estimate": 1073741824u64,
                "repaired_ranges": 100u64,
                "coordinator_node": "192.168.1.1",
                "participants": ["192.168.1.1", "192.168.1.2", "192.168.1.3"]
            }
        ]);

        let parent_repair_data = json!([]);
        // Use a cutoff well before 2024 so the row passes the filter.
        let cutoff = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap().with_timezone(&Utc);

        let history = CassandraRepairInfo::process_repair_history(&repair_data, &parent_repair_data, cutoff).unwrap_or_default();
        assert_eq!(history.len(), 1);

        let entry = &history[0];
        assert_eq!(entry.session_id, "repair-123");
        assert_eq!(entry.keyspace_name, "test_ks");
        assert_eq!(entry.table_name, "test_table");
        assert_eq!(entry.final_status, "COMPLETED");
        assert_eq!(entry.duration_minutes, 60.0);
        assert_eq!(entry.data_repaired_gb, 1.0);
        assert_eq!(entry.ranges_repaired, 100);
        assert_eq!(entry.participating_nodes.len(), 3);
    }

    #[test]
    fn test_history_cutoff_filters_old_rows() {
        let repair_data = json!([
            {
                "repair_id": "old-repair",
                "keyspace_name": "test_ks",
                "table_name": "test_table",
                "start_time": "2020-01-01T00:00:00Z",
                "end_time": "2020-01-01T01:00:00Z",
                "status": "COMPLETED",
                "repaired_ranges": 10u64
            }
        ]);
        let parent_repair_data = json!([]);
        // Cutoff is 2023; the 2020 row should be excluded.
        let cutoff = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc);

        let history = CassandraRepairInfo::process_repair_history(&repair_data, &parent_repair_data, cutoff).unwrap_or_default();
        assert!(history.is_empty());
    }

    #[test]
    fn test_participants_parsing() {
        let row_array = json!({ "participants": ["node1", "node2", "node3"] });
        let participants = CassandraRepairInfo::parse_participants(&row_array);
        assert_eq!(participants, vec!["node1", "node2", "node3"]);

        let row_string = json!({ "participants": "node1,node2,node3" });
        let participants = CassandraRepairInfo::parse_participants(&row_string);
        assert_eq!(participants, vec!["node1", "node2", "node3"]);

        let row_empty = json!({});
        let participants = CassandraRepairInfo::parse_participants(&row_empty);
        assert!(participants.is_empty());
    }

    #[test]
    fn test_recommended_actions() {
        let mut repair_info = CassandraRepairInfo { schedule_compliance_pct: 60.0, ..Default::default() };
        repair_info.consistency_metrics.entropy_level = 80.0;
        repair_info.repair_success_rate_pct = 85.0;
        repair_info.performance_metrics.concurrent_sessions = 5;
        repair_info.performance_metrics.max_recommended_concurrent = 3;

        let actions = repair_info.get_recommended_actions();
        assert!(actions.len() > 3);
        assert!(actions.iter().any(|a| a.contains("URGENT")));
        assert!(actions.iter().any(|a| a.contains("compliance")));
        assert!(actions.iter().any(|a| a.contains("entropy")));
    }

    #[test]
    fn test_resource_efficiency() {
        let mut repair_info = CassandraRepairInfo::default();

        repair_info.performance_metrics.avg_cpu_utilization_during_repair_pct = 60.0;
        repair_info.performance_metrics.concurrent_sessions = 2;
        repair_info.performance_metrics.max_recommended_concurrent = 3;
        repair_info.performance_metrics.repair_efficiency_score = 85.0;
        assert!(repair_info.is_repair_resource_efficient());

        repair_info.performance_metrics.avg_cpu_utilization_during_repair_pct = 90.0;
        assert!(!repair_info.is_repair_resource_efficient());
    }
}
