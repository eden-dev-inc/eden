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

use super::utils::{DEFAULT_QUERY_TIMEOUT, get_string, map_rows, row_count, run_named_query, run_optional_named_query};

/// Cassandra snapshot inventory, storage totals and backup-coverage metrics.
///
/// # Availability Note
///
/// Cassandra does not expose snapshot state through standard CQL tables.
/// Snapshot metadata requires `nodetool listsnapshots` or JMX access,
/// neither of which is available via CQL. This collector gathers
/// keyspace/table topology from `system_schema` and compaction history
/// from `system.compaction_history` as proxies for backup coverage
/// assessment. All snapshot-specific fields default to zero/empty when
/// no data is available.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotInfo {
    /// Total number of snapshots across all nodes
    pub total_snapshots: u64,
    /// Total storage used by all snapshots (GB)
    pub total_snapshot_storage_gb: f64,
    /// Number of keyspaces with snapshots
    pub keyspaces_with_snapshots: u64,
    /// Number of tables with snapshots
    pub tables_with_snapshots: u64,
    /// Average snapshot age (days)
    pub avg_snapshot_age_days: f64,
    /// Oldest snapshot age (days)
    pub oldest_snapshot_age_days: f64,
    /// Newest snapshot age (days)
    pub newest_snapshot_age_days: f64,
    /// Number of snapshots older than retention policy
    pub snapshots_exceeding_retention: u64,
    /// Total storage that could be reclaimed (GB)
    pub reclaimable_storage_gb: f64,
    /// Snapshot creation rate (snapshots per day)
    pub snapshot_creation_rate: f64,
    /// Backup coverage percentage
    pub backup_coverage_percentage: f64,
    /// Detailed snapshot information by keyspace
    pub keyspace_snapshots: Vec<CassandraKeyspaceSnapshotInfo>,
    /// Individual snapshot details
    pub snapshot_details: Vec<CassandraSnapshotDetail>,
    /// Snapshot storage metrics
    pub storage_metrics: CassandraSnapshotStorageMetrics,
    /// Snapshot health and compliance metrics
    pub health_metrics: CassandraSnapshotHealthMetrics,
    /// Retention policy compliance
    pub retention_compliance: CassandraSnapshotRetentionCompliance,
}

/// Snapshot information for a specific keyspace
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceSnapshotInfo {
    /// Keyspace name
    pub keyspace_name: String,
    /// Number of snapshots for this keyspace
    pub snapshot_count: u64,
    /// Total storage used by keyspace snapshots (GB)
    pub total_storage_gb: f64,
    /// Average snapshot size for this keyspace (GB)
    pub avg_snapshot_size_gb: f64,
    /// Most recent snapshot timestamp
    pub latest_snapshot_time: Option<String>,
    /// Oldest snapshot timestamp
    pub oldest_snapshot_time: Option<String>,
    /// Number of tables in keyspace with snapshots
    pub tables_with_snapshots: u64,
    /// Total number of tables in keyspace
    pub total_tables: u64,
    /// Backup coverage for this keyspace (percentage)
    pub backup_coverage_pct: f64,
    /// Snapshots exceeding retention for this keyspace
    pub snapshots_exceeding_retention: u64,
    /// Storage reclaimable from old snapshots (GB)
    pub reclaimable_storage_gb: f64,
    /// Snapshot frequency pattern (DAILY, WEEKLY, MANUAL etc.)
    pub snapshot_pattern: String,
    /// Last successful backup time
    pub last_successful_backup: Option<String>,
}

/// Detailed information about an individual snapshot
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotDetail {
    /// Snapshot name/tag
    pub snapshot_name: String,
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Node where snapshot was taken
    pub node_address: String,
    /// Snapshot creation timestamp
    pub created_at: String,
    /// Snapshot size (GB)
    pub size_gb: f64,
    /// Number of SSTables in snapshot
    pub sstable_count: u64,
    /// Snapshot type (MANUAL, SCHEDULED, BACKUP)
    pub snapshot_type: String,
    /// Age in days
    pub age_days: f64,
    /// Is snapshot expired according to retention policy
    pub is_expired: bool,
    /// Snapshot status (ACTIVE, CORRUPTED, INCOMPLETE)
    pub status: String,
    /// Storage location/path
    pub storage_path: Option<String>,
    /// Snapshot metadata
    pub metadata: HashMap<String, String>,
    /// Compression ratio if applicable
    pub compression_ratio: Option<f64>,
    /// Associated backup job ID
    pub backup_job_id: Option<String>,
}

/// Storage metrics for snapshots
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotStorageMetrics {
    /// Storage usage by snapshot type
    pub storage_by_type: HashMap<String, f64>,
    /// Storage usage by keyspace
    pub storage_by_keyspace: HashMap<String, f64>,
    /// Storage usage by node
    pub storage_by_node: HashMap<String, f64>,
    /// Storage usage by age ranges
    pub storage_by_age_ranges: HashMap<String, f64>,
    /// Average storage per snapshot (GB)
    pub avg_storage_per_snapshot_gb: f64,
    /// Largest snapshot size (GB)
    pub largest_snapshot_gb: f64,
    /// Smallest snapshot size (GB)
    pub smallest_snapshot_gb: f64,
    /// Storage growth rate (GB per day)
    pub storage_growth_rate_gb_per_day: f64,
    /// Compression efficiency across snapshots
    pub avg_compression_ratio: f64,
    /// Storage utilization percentage
    pub storage_utilization_pct: f64,
}

/// Health and compliance metrics for snapshots
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotHealthMetrics {
    /// Number of corrupted snapshots
    pub corrupted_snapshots: u64,
    /// Number of incomplete snapshots
    pub incomplete_snapshots: u64,
    /// Number of orphaned snapshots (no matching table)
    pub orphaned_snapshots: u64,
    /// Number of snapshots missing on some nodes
    pub inconsistent_snapshots: u64,
    /// Snapshot creation success rate (percentage)
    pub creation_success_rate_pct: f64,
    /// Average time to create snapshot (minutes)
    pub avg_creation_time_minutes: f64,
    /// Backup verification success rate
    pub verification_success_rate_pct: f64,
    /// Number of snapshots with missing metadata
    pub snapshots_missing_metadata: u64,
    /// Overall snapshot health score (0-100)
    pub health_score: f64,
    /// Data protection coverage score (0-100)
    pub data_protection_score: f64,
}

/// Retention policy compliance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotRetentionCompliance {
    /// Configured retention period (days)
    pub retention_period_days: u64,
    /// Number of snapshots compliant with retention
    pub compliant_snapshots: u64,
    /// Number of snapshots violating retention
    pub non_compliant_snapshots: u64,
    /// Compliance percentage
    pub compliance_percentage: f64,
    /// Storage used by non-compliant snapshots (GB)
    pub non_compliant_storage_gb: f64,
    /// Estimated cost savings from cleanup (arbitrary units)
    pub potential_cleanup_savings: f64,
    /// Last cleanup operation timestamp
    pub last_cleanup_time: Option<String>,
    /// Automatic cleanup enabled
    pub auto_cleanup_enabled: bool,
    /// Cleanup frequency (DAILY, WEEKLY, MONTHLY)
    pub cleanup_frequency: String,
    /// Grace period for retention (days)
    pub grace_period_days: u64,
}

impl MetadataCollection for CassandraSnapshotInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "keyspaces".to_string(),
                QueryUnpagedInput::new("SELECT keyspace_name FROM system_schema.keyspaces".to_string()),
            ),
            (
                "tables".to_string(),
                QueryUnpagedInput::new("SELECT keyspace_name, table_name FROM system_schema.tables".to_string()),
            ),
            (
                "local_info".to_string(),
                QueryUnpagedInput::new(
                    "SELECT broadcast_address, data_center, rack
                 FROM system.local"
                        .to_string(),
                ),
            ),
            (
                "compaction_history".to_string(),
                QueryUnpagedInput::new(
                    "SELECT keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out
                 FROM system.compaction_history"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return Cassandra snapshot topology and backup coverage metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "snapshot"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl CassandraSnapshotInfo {
    const DEFAULT_RETENTION_DAYS: u64 = 30;
    const SECONDS_PER_DAY: f64 = 86400.0;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let requests = self.request();

        // Required queries; failure aborts collection.
        let (keyspaces_data, tables_data) = tokio::try_join!(
            run_named_query(&requests, "keyspaces", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "tables", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        // Optional queries; failure produces empty data rather than aborting.
        let local_info_data = run_optional_named_query(&requests, "local_info", context.clone(), DEFAULT_QUERY_TIMEOUT).await;
        let compaction_history_data =
            run_optional_named_query(&requests, "compaction_history", context.clone(), DEFAULT_QUERY_TIMEOUT).await;

        let local_node_address = Self::extract_local_node_address(local_info_data.as_ref());

        // Cassandra does not expose snapshot state via standard CQL. The
        // snapshot_details list is left empty; callers that need snapshot
        // inventory should use `nodetool listsnapshots` or JMX.
        let mut snapshot_info = CassandraSnapshotInfo::default();

        Self::calculate_basic_statistics(&mut snapshot_info);

        snapshot_info.keyspace_snapshots =
            Self::build_keyspace_snapshot_info(&snapshot_info.snapshot_details, &keyspaces_data, &tables_data);

        snapshot_info.storage_metrics = Self::calculate_storage_metrics(&snapshot_info.snapshot_details);

        snapshot_info.health_metrics = Self::calculate_health_metrics(&snapshot_info.snapshot_details);

        snapshot_info.retention_compliance = Self::calculate_retention_compliance(&snapshot_info.snapshot_details);

        snapshot_info.backup_coverage_percentage = Self::calculate_backup_coverage(&snapshot_info.keyspace_snapshots, &tables_data);

        // Annotate keyspace info with last-compaction timestamp as a best-effort
        // proxy for "recently touched"; not a true backup timestamp.
        if let Some(ref compaction_data) = compaction_history_data {
            Self::annotate_last_compaction(&mut snapshot_info.keyspace_snapshots, compaction_data, &local_node_address);
        }

        Ok(snapshot_info)
    }

    // Data extraction

    fn extract_local_node_address(local_info_data: Option<&Value>) -> String {
        let data = match local_info_data {
            Some(v) => v,
            None => return "localhost".to_string(),
        };
        map_rows(data, |row| get_string(row, "broadcast_address")).into_iter().next().unwrap_or_else(|| "localhost".to_string())
    }

    // Statistics

    fn calculate_basic_statistics(snapshot_info: &mut CassandraSnapshotInfo) {
        snapshot_info.total_snapshots = snapshot_info.snapshot_details.len() as u64;

        if snapshot_info.snapshot_details.is_empty() {
            return;
        }

        snapshot_info.total_snapshot_storage_gb = snapshot_info.snapshot_details.iter().map(|s| s.size_gb).sum();

        let ages: Vec<f64> = snapshot_info.snapshot_details.iter().map(|s| s.age_days).collect();
        snapshot_info.avg_snapshot_age_days = ages.iter().sum::<f64>() / ages.len() as f64;
        snapshot_info.oldest_snapshot_age_days = ages.iter().cloned().fold(0.0_f64, f64::max);
        snapshot_info.newest_snapshot_age_days = ages.iter().cloned().fold(f64::INFINITY, f64::min);

        snapshot_info.snapshots_exceeding_retention = snapshot_info.snapshot_details.iter().filter(|s| s.is_expired).count() as u64;

        snapshot_info.reclaimable_storage_gb = snapshot_info.snapshot_details.iter().filter(|s| s.is_expired).map(|s| s.size_gb).sum();

        let unique_keyspaces: std::collections::HashSet<_> = snapshot_info.snapshot_details.iter().map(|s| &s.keyspace_name).collect();
        snapshot_info.keyspaces_with_snapshots = unique_keyspaces.len() as u64;

        let unique_tables: std::collections::HashSet<_> =
            snapshot_info.snapshot_details.iter().map(|s| format!("{}.{}", s.keyspace_name, s.table_name)).collect();
        snapshot_info.tables_with_snapshots = unique_tables.len() as u64;
    }

    // Keyspace-level aggregation

    fn build_keyspace_snapshot_info(
        snapshot_details: &[CassandraSnapshotDetail],
        keyspaces_data: &Value,
        tables_data: &Value,
    ) -> Vec<CassandraKeyspaceSnapshotInfo> {
        let mut keyspace_info_map: HashMap<String, CassandraKeyspaceSnapshotInfo> = HashMap::new();

        // Initialise one entry per known keyspace.
        map_rows(keyspaces_data, |row| get_string(row, "keyspace_name")).into_iter().for_each(|keyspace_name| {
            keyspace_info_map.insert(
                keyspace_name.clone(),
                CassandraKeyspaceSnapshotInfo {
                    keyspace_name,
                    snapshot_count: 0,
                    total_storage_gb: 0.0,
                    avg_snapshot_size_gb: 0.0,
                    latest_snapshot_time: None,
                    oldest_snapshot_time: None,
                    tables_with_snapshots: 0,
                    total_tables: 0,
                    backup_coverage_pct: 0.0,
                    snapshots_exceeding_retention: 0,
                    reclaimable_storage_gb: 0.0,
                    snapshot_pattern: "UNKNOWN".to_string(),
                    last_successful_backup: None,
                },
            );
        });

        // Count total tables per keyspace.
        map_rows(tables_data, |row| get_string(row, "keyspace_name")).into_iter().for_each(|keyspace_name| {
            if let Some(info) = keyspace_info_map.get_mut(&keyspace_name) {
                info.total_tables += 1;
            }
        });

        // Aggregate snapshot details into keyspace buckets.
        for snapshot in snapshot_details {
            let Some(info) = keyspace_info_map.get_mut(&snapshot.keyspace_name) else {
                continue;
            };

            info.snapshot_count += 1;
            info.total_storage_gb += snapshot.size_gb;

            if snapshot.is_expired {
                info.snapshots_exceeding_retention += 1;
                info.reclaimable_storage_gb += snapshot.size_gb;
            }

            let should_update_latest =
                info.latest_snapshot_time.as_deref().is_none_or(|latest| Self::is_timestamp_more_recent(&snapshot.created_at, latest));
            if should_update_latest {
                info.latest_snapshot_time = Some(snapshot.created_at.clone());
                if snapshot.status == "ACTIVE" {
                    info.last_successful_backup = Some(snapshot.created_at.clone());
                }
            }

            let should_update_oldest =
                info.oldest_snapshot_time.as_deref().is_none_or(|oldest| Self::is_timestamp_older(&snapshot.created_at, oldest));
            if should_update_oldest {
                info.oldest_snapshot_time = Some(snapshot.created_at.clone());
            }
        }

        // Compute derived metrics.
        for info in keyspace_info_map.values_mut() {
            if info.snapshot_count > 0 {
                info.avg_snapshot_size_gb = info.total_storage_gb / info.snapshot_count as f64;
            }

            let tables_with_snapshots: std::collections::HashSet<_> =
                snapshot_details.iter().filter(|s| s.keyspace_name == info.keyspace_name).map(|s| &s.table_name).collect();
            info.tables_with_snapshots = tables_with_snapshots.len() as u64;

            if info.total_tables > 0 {
                info.backup_coverage_pct = (info.tables_with_snapshots as f64 / info.total_tables as f64) * 100.0;
            }

            info.snapshot_pattern =
                Self::determine_snapshot_pattern(snapshot_details.iter().filter(|s| s.keyspace_name == info.keyspace_name));
        }

        keyspace_info_map.into_values().collect()
    }

    /// Annotate keyspace entries with the most-recent compaction timestamp as a
    /// best-effort proxy for "data was recently touched / backed up".
    ///
    /// This is not a substitute for real snapshot timestamps; it is surfaced
    /// only when actual snapshot data is unavailable.
    fn annotate_last_compaction(
        keyspace_snapshots: &mut [CassandraKeyspaceSnapshotInfo],
        compaction_data: &Value,
        _local_node_address: &str,
    ) {
        let Value::Array(rows) = compaction_data else {
            return;
        };

        // Build a map of keyspace_name -> latest compacted_at string.
        let mut latest_compaction: HashMap<String, String> = HashMap::new();
        for row in rows {
            let Some(ks) = get_string(row, "keyspace_name") else {
                continue;
            };
            let Some(compacted_at) = get_string(row, "compacted_at") else {
                continue;
            };
            let entry = latest_compaction.entry(ks).or_insert_with(|| compacted_at.clone());
            if Self::is_timestamp_more_recent(&compacted_at, entry) {
                *entry = compacted_at;
            }
        }

        for info in keyspace_snapshots.iter_mut() {
            if info.last_successful_backup.is_none()
                && let Some(ts) = latest_compaction.get(&info.keyspace_name)
            {
                info.last_successful_backup = Some(ts.clone());
            }
        }
    }

    // Storage metrics

    fn calculate_storage_metrics(snapshot_details: &[CassandraSnapshotDetail]) -> CassandraSnapshotStorageMetrics {
        let mut metrics = CassandraSnapshotStorageMetrics::default();

        if snapshot_details.is_empty() {
            return metrics;
        }

        for snapshot in snapshot_details {
            *metrics.storage_by_type.entry(snapshot.snapshot_type.clone()).or_insert(0.0) += snapshot.size_gb;
            *metrics.storage_by_keyspace.entry(snapshot.keyspace_name.clone()).or_insert(0.0) += snapshot.size_gb;
            *metrics.storage_by_node.entry(snapshot.node_address.clone()).or_insert(0.0) += snapshot.size_gb;

            let age_range = match snapshot.age_days {
                age if age <= 7.0 => "0-7 days",
                age if age <= 30.0 => "8-30 days",
                age if age <= 90.0 => "31-90 days",
                _ => "90+ days",
            };
            *metrics.storage_by_age_ranges.entry(age_range.to_string()).or_insert(0.0) += snapshot.size_gb;
        }

        let sizes: Vec<f64> = snapshot_details.iter().map(|s| s.size_gb).collect();
        metrics.avg_storage_per_snapshot_gb = sizes.iter().sum::<f64>() / sizes.len() as f64;
        metrics.largest_snapshot_gb = sizes.iter().cloned().fold(0.0_f64, f64::max);
        metrics.smallest_snapshot_gb = sizes.iter().cloned().fold(f64::INFINITY, f64::min);

        let compression_ratios: Vec<f64> = snapshot_details.iter().filter_map(|s| s.compression_ratio).collect();
        if !compression_ratios.is_empty() {
            metrics.avg_compression_ratio = compression_ratios.iter().sum::<f64>() / compression_ratios.len() as f64;
        }

        metrics
    }

    // Health metrics

    fn calculate_health_metrics(snapshot_details: &[CassandraSnapshotDetail]) -> CassandraSnapshotHealthMetrics {
        let mut metrics = CassandraSnapshotHealthMetrics::default();

        for snapshot in snapshot_details {
            match snapshot.status.as_str() {
                "CORRUPTED" => metrics.corrupted_snapshots += 1,
                "INCOMPLETE" => metrics.incomplete_snapshots += 1,
                _ => {}
            }
            if snapshot.metadata.is_empty() {
                metrics.snapshots_missing_metadata += 1;
            }
        }

        let total = snapshot_details.len() as f64;
        if total > 0.0 {
            let healthy = total - metrics.corrupted_snapshots as f64 - metrics.incomplete_snapshots as f64;
            metrics.health_score = (healthy / total * 100.0).clamp(0.0, 100.0);
            // Data protection score cannot be derived without real snapshot data.
            metrics.data_protection_score = 0.0;
        } else {
            // No CQL-accessible snapshot data; score is indeterminate.
            metrics.health_score = 0.0;
            metrics.data_protection_score = 0.0;
        }

        metrics
    }

    // Retention compliance

    fn calculate_retention_compliance(snapshot_details: &[CassandraSnapshotDetail]) -> CassandraSnapshotRetentionCompliance {
        let mut compliance = CassandraSnapshotRetentionCompliance {
            retention_period_days: Self::DEFAULT_RETENTION_DAYS,
            grace_period_days: 7,
            auto_cleanup_enabled: false,
            cleanup_frequency: "WEEKLY".to_string(),
            ..Default::default()
        };

        for snapshot in snapshot_details {
            if snapshot.is_expired {
                compliance.non_compliant_snapshots += 1;
                compliance.non_compliant_storage_gb += snapshot.size_gb;
            } else {
                compliance.compliant_snapshots += 1;
            }
        }

        let total = snapshot_details.len() as u64;
        compliance.compliance_percentage = if total > 0 {
            compliance.compliant_snapshots as f64 / total as f64 * 100.0
        } else {
            100.0
        };

        compliance
    }

    // Backup coverage

    fn calculate_backup_coverage(keyspace_snapshots: &[CassandraKeyspaceSnapshotInfo], tables_data: &Value) -> f64 {
        let total_tables = row_count(tables_data) as u64;
        let tables_with_snapshots: u64 = keyspace_snapshots.iter().map(|ks| ks.tables_with_snapshots).sum();

        if total_tables > 0 {
            tables_with_snapshots as f64 / total_tables as f64 * 100.0
        } else {
            0.0
        }
    }

    // Timestamp helpers

    fn parse_timestamp(timestamp_str: &str) -> Option<DateTime<Utc>> {
        if timestamp_str.is_empty() {
            return None;
        }

        // ISO 8601 with timezone offset.
        if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
            return Some(dt.with_timezone(&Utc));
        }

        // UTC string.
        if let Ok(dt) = timestamp_str.parse::<DateTime<Utc>>() {
            return Some(dt);
        }

        // Cassandra microseconds since epoch.
        if let Ok(micros) = timestamp_str.parse::<i64>() {
            return DateTime::from_timestamp(micros / 1_000_000, ((micros % 1_000_000) * 1000) as u32);
        }

        None
    }

    fn is_timestamp_more_recent(ts1: &str, ts2: &str) -> bool {
        match (Self::parse_timestamp(ts1), Self::parse_timestamp(ts2)) {
            (Some(a), Some(b)) => a > b,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn is_timestamp_older(ts1: &str, ts2: &str) -> bool {
        match (Self::parse_timestamp(ts1), Self::parse_timestamp(ts2)) {
            (Some(a), Some(b)) => a < b,
            (None, Some(_)) => true,
            _ => false,
        }
    }

    // Retained for use when snapshot data is fed from nodetool/JMX in future.
    #[allow(dead_code)]
    fn calculate_age_days(timestamp: &str) -> f64 {
        match Self::parse_timestamp(timestamp) {
            Some(created_time) => {
                let duration = Utc::now().signed_duration_since(created_time);
                duration.num_seconds() as f64 / Self::SECONDS_PER_DAY
            }
            None => 0.0,
        }
    }

    // Snapshot pattern inference

    // Retained for use when snapshot data is fed from nodetool/JMX in future.
    #[allow(dead_code)]
    fn determine_snapshot_type(snapshot_name: &str) -> String {
        if snapshot_name.contains("backup") || snapshot_name.contains("scheduled") {
            "SCHEDULED".to_string()
        } else if snapshot_name.contains("manual") || snapshot_name.contains("adhoc") {
            "MANUAL".to_string()
        } else if snapshot_name.contains("repair") {
            "REPAIR".to_string()
        } else {
            "BACKUP".to_string()
        }
    }

    fn determine_snapshot_pattern<'a, I>(snapshots: I) -> String
    where
        I: Iterator<Item = &'a CassandraSnapshotDetail>,
    {
        let snapshot_types: Vec<&str> = snapshots.map(|s| s.snapshot_type.as_str()).collect();

        if snapshot_types.contains(&"SCHEDULED") {
            if snapshot_types.len() > 7 {
                "DAILY".to_string()
            } else if snapshot_types.len() > 1 {
                "WEEKLY".to_string()
            } else {
                "MONTHLY".to_string()
            }
        } else {
            "MANUAL".to_string()
        }
    }
}

// Public query/analysis methods

impl CassandraSnapshotInfo {
    /// Checks if there are critical snapshot management issues
    pub fn has_critical_snapshot_issues(&self) -> bool {
        self.health_metrics.health_score < 70.0
            || self.retention_compliance.compliance_percentage < 50.0
            || self.backup_coverage_percentage < 80.0
            || self.health_metrics.corrupted_snapshots > 0
    }

    /// Gets snapshots that need immediate attention
    pub fn snapshots_needing_attention(&self) -> Vec<&CassandraSnapshotDetail> {
        self.snapshot_details
            .iter()
            .filter(|snapshot| {
                snapshot.status == "CORRUPTED"
                    || snapshot.status == "INCOMPLETE"
                    || (snapshot.is_expired && snapshot.age_days > 90.0)
                    || snapshot.size_gb > 50.0
            })
            .collect()
    }

    /// Gets keyspaces with poor backup coverage
    pub fn keyspaces_with_poor_coverage(&self) -> Vec<&CassandraKeyspaceSnapshotInfo> {
        self.keyspace_snapshots.iter().filter(|ks| ks.backup_coverage_pct < 80.0).collect()
    }

    /// Calculates storage efficiency score
    pub fn storage_efficiency_score(&self) -> f64 {
        if self.total_snapshot_storage_gb == 0.0 {
            return 100.0;
        }

        let compression_score = (self.storage_metrics.avg_compression_ratio - 1.0) * 25.0;
        let retention_score = self.retention_compliance.compliance_percentage / 2.0;

        (compression_score + retention_score).clamp(0.0, 100.0)
    }

    /// Gets snapshots grouped by age ranges for analysis
    pub fn snapshots_by_age_ranges(&self) -> HashMap<String, Vec<&CassandraSnapshotDetail>> {
        let mut ranges: HashMap<String, Vec<&CassandraSnapshotDetail>> = HashMap::new();

        for snapshot in &self.snapshot_details {
            let range = match snapshot.age_days {
                age if age <= 1.0 => "Last 24 hours",
                age if age <= 7.0 => "Last week",
                age if age <= 30.0 => "Last month",
                age if age <= 90.0 => "Last 3 months",
                _ => "Older than 3 months",
            };
            ranges.entry(range.to_string()).or_default().push(snapshot);
        }

        ranges
    }

    /// Estimates cost savings from cleanup
    pub fn estimated_cleanup_savings(&self) -> f64 {
        self.retention_compliance.potential_cleanup_savings
    }

    /// Gets the largest snapshots for optimization
    pub fn largest_snapshots(&self, limit: usize) -> Vec<&CassandraSnapshotDetail> {
        let mut snapshots: Vec<&CassandraSnapshotDetail> = self.snapshot_details.iter().collect();
        snapshots.sort_by(|a, b| b.size_gb.partial_cmp(&a.size_gb).unwrap_or(std::cmp::Ordering::Equal));
        snapshots.into_iter().take(limit).collect()
    }

    /// Gets the oldest snapshots for cleanup consideration
    pub fn oldest_snapshots(&self, limit: usize) -> Vec<&CassandraSnapshotDetail> {
        let mut snapshots: Vec<&CassandraSnapshotDetail> = self.snapshot_details.iter().collect();
        snapshots.sort_by(|a, b| b.age_days.partial_cmp(&a.age_days).unwrap_or(std::cmp::Ordering::Equal));
        snapshots.into_iter().take(limit).collect()
    }

    /// Gets backup health rating (A-F scale)
    pub fn backup_health_rating(&self) -> String {
        let combined_score =
            (self.health_metrics.health_score + self.health_metrics.data_protection_score + self.backup_coverage_percentage) / 3.0;

        match combined_score {
            s if s >= 90.0 => "A".to_string(),
            s if s >= 80.0 => "B".to_string(),
            s if s >= 70.0 => "C".to_string(),
            s if s >= 60.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Gets recommended snapshot management actions
    pub fn get_snapshot_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.has_critical_snapshot_issues() {
            recommendations.push("CRITICAL: Address snapshot management issues immediately".to_string());
        }

        if self.backup_coverage_percentage < 80.0 {
            recommendations.push(format!("Improve backup coverage - currently at {:.1}%", self.backup_coverage_percentage));
        }

        if self.retention_compliance.compliance_percentage < 70.0 {
            recommendations.push("Implement automated snapshot cleanup to improve retention compliance".to_string());
        }

        if self.health_metrics.corrupted_snapshots > 0 {
            recommendations.push(format!("Investigate and remove {} corrupted snapshots", self.health_metrics.corrupted_snapshots));
        }

        if self.reclaimable_storage_gb > 10.0 {
            recommendations.push(format!("Clean up expired snapshots to reclaim {:.1} GB of storage", self.reclaimable_storage_gb));
        }

        if self.storage_metrics.avg_compression_ratio < 2.0 {
            recommendations.push("Consider enabling compression for snapshots to save storage".to_string());
        }

        if self.snapshot_creation_rate < 0.5 {
            recommendations.push("Snapshot creation frequency is low - review backup schedule".to_string());
        }

        if !self.retention_compliance.auto_cleanup_enabled {
            recommendations.push("Enable automatic snapshot cleanup to maintain retention policies".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Snapshot management appears healthy - continue monitoring".to_string());
        }

        recommendations
    }

    /// Gets snapshot distribution statistics
    pub fn get_snapshot_distribution_stats(&self) -> CassandraSnapshotDistributionStats {
        let avg = self.storage_metrics.avg_storage_per_snapshot_gb;
        CassandraSnapshotDistributionStats {
            snapshots_by_type: self
                .storage_metrics
                .storage_by_type
                .iter()
                .map(|(k, v)| (k.clone(), if avg > 0.0 { (v / avg) as u64 } else { 0 }))
                .collect(),
            snapshots_by_keyspace: self.keyspace_snapshots.iter().map(|ks| (ks.keyspace_name.clone(), ks.snapshot_count)).collect(),
            snapshots_by_age_ranges: self.snapshots_by_age_ranges().iter().map(|(k, v)| (k.clone(), v.len() as u64)).collect(),
            storage_by_node: self.storage_metrics.storage_by_node.clone(),
        }
    }

    /// Gets summary for reporting
    pub fn get_snapshot_summary(&self) -> CassandraSnapshotSummary {
        CassandraSnapshotSummary {
            total_snapshots: self.total_snapshots,
            total_storage_gb: self.total_snapshot_storage_gb,
            backup_coverage_percentage: self.backup_coverage_percentage,
            health_score: self.health_metrics.health_score,
            health_rating: self.backup_health_rating(),
            retention_compliance_pct: self.retention_compliance.compliance_percentage,
            snapshots_exceeding_retention: self.snapshots_exceeding_retention,
            reclaimable_storage_gb: self.reclaimable_storage_gb,
            corrupted_snapshots: self.health_metrics.corrupted_snapshots,
            has_critical_issues: self.has_critical_snapshot_issues(),
        }
    }
}

/// Snapshot distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraSnapshotDistributionStats {
    pub snapshots_by_type: HashMap<String, u64>,
    pub snapshots_by_keyspace: HashMap<String, u64>,
    pub snapshots_by_age_ranges: HashMap<String, u64>,
    pub storage_by_node: HashMap<String, f64>,
}

/// Summary statistics for snapshot information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSnapshotSummary {
    pub total_snapshots: u64,
    pub total_storage_gb: f64,
    pub backup_coverage_percentage: f64,
    pub health_score: f64,
    pub health_rating: String,
    pub retention_compliance_pct: f64,
    pub snapshots_exceeding_retention: u64,
    pub reclaimable_storage_gb: f64,
    pub corrupted_snapshots: u64,
    pub has_critical_issues: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_type_determination() {
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_type("daily_backup_20240101"), "SCHEDULED");
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_type("manual_snapshot_user123"), "MANUAL");
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_type("repair_snapshot_table1"), "REPAIR");
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_type("snapshot_20240101"), "BACKUP");
    }

    #[test]
    fn test_age_calculation() {
        use chrono::Duration as ChronoDuration;

        let now = Utc::now();
        let yesterday = (now - ChronoDuration::days(1)).to_rfc3339();
        let age = CassandraSnapshotInfo::calculate_age_days(&yesterday);

        assert!((age - 1.0).abs() < 0.1);
    }

    #[test]
    fn test_critical_issues_detection() {
        let mut snapshot_info = CassandraSnapshotInfo::default();

        snapshot_info.health_metrics.health_score = 85.0;
        snapshot_info.retention_compliance.compliance_percentage = 90.0;
        snapshot_info.backup_coverage_percentage = 95.0;
        snapshot_info.health_metrics.corrupted_snapshots = 0;
        assert!(!snapshot_info.has_critical_snapshot_issues());

        snapshot_info.health_metrics.health_score = 60.0;
        assert!(snapshot_info.has_critical_snapshot_issues());

        snapshot_info.health_metrics.health_score = 85.0;
        snapshot_info.retention_compliance.compliance_percentage = 40.0;
        assert!(snapshot_info.has_critical_snapshot_issues());

        snapshot_info.retention_compliance.compliance_percentage = 90.0;
        snapshot_info.backup_coverage_percentage = 70.0;
        assert!(snapshot_info.has_critical_snapshot_issues());

        snapshot_info.backup_coverage_percentage = 95.0;
        snapshot_info.health_metrics.corrupted_snapshots = 1;
        assert!(snapshot_info.has_critical_snapshot_issues());
    }

    #[test]
    fn test_backup_health_rating() {
        let mut snapshot_info = CassandraSnapshotInfo::default();

        snapshot_info.health_metrics.health_score = 95.0;
        snapshot_info.health_metrics.data_protection_score = 90.0;
        snapshot_info.backup_coverage_percentage = 95.0;
        assert_eq!(snapshot_info.backup_health_rating(), "A");

        snapshot_info.health_metrics.health_score = 85.0;
        snapshot_info.health_metrics.data_protection_score = 80.0;
        snapshot_info.backup_coverage_percentage = 85.0;
        assert_eq!(snapshot_info.backup_health_rating(), "B");

        snapshot_info.health_metrics.health_score = 75.0;
        snapshot_info.health_metrics.data_protection_score = 70.0;
        snapshot_info.backup_coverage_percentage = 75.0;
        assert_eq!(snapshot_info.backup_health_rating(), "C");

        snapshot_info.health_metrics.health_score = 50.0;
        snapshot_info.health_metrics.data_protection_score = 50.0;
        snapshot_info.backup_coverage_percentage = 50.0;
        assert_eq!(snapshot_info.backup_health_rating(), "F");
    }

    #[test]
    fn test_storage_efficiency_calculation() {
        let mut snapshot_info = CassandraSnapshotInfo::default();

        assert_eq!(snapshot_info.storage_efficiency_score(), 100.0);

        snapshot_info.total_snapshot_storage_gb = 100.0;
        snapshot_info.storage_metrics.avg_compression_ratio = 3.0;
        snapshot_info.retention_compliance.compliance_percentage = 90.0;
        let score = snapshot_info.storage_efficiency_score();
        assert!(score > 80.0);

        snapshot_info.storage_metrics.avg_compression_ratio = 1.2;
        snapshot_info.retention_compliance.compliance_percentage = 30.0;
        let score = snapshot_info.storage_efficiency_score();
        assert!(score < 50.0);
    }

    #[test]
    fn test_snapshot_pattern_determination() {
        let make_snapshot = |snapshot_type: &str| CassandraSnapshotDetail {
            snapshot_name: "test".to_string(),
            keyspace_name: "test".to_string(),
            table_name: "table1".to_string(),
            node_address: "localhost".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            size_gb: 1.0,
            sstable_count: 10,
            snapshot_type: snapshot_type.to_string(),
            age_days: 1.0,
            is_expired: false,
            status: "ACTIVE".to_string(),
            storage_path: None,
            metadata: HashMap::new(),
            compression_ratio: None,
            backup_job_id: None,
        };

        let daily: Vec<_> = (0..8).map(|_| make_snapshot("SCHEDULED")).collect();
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_pattern(daily.iter()), "DAILY");

        let weekly: Vec<_> = (0..3).map(|_| make_snapshot("SCHEDULED")).collect();
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_pattern(weekly.iter()), "WEEKLY");

        let monthly = [make_snapshot("SCHEDULED")];
        assert_eq!(CassandraSnapshotInfo::determine_snapshot_pattern(monthly.iter()), "MONTHLY");
    }

    #[test]
    fn test_snapshots_by_age_ranges() {
        let make_snapshot = |name: &str, age_days: f64| CassandraSnapshotDetail {
            snapshot_name: name.to_string(),
            keyspace_name: "test".to_string(),
            table_name: name.to_string(),
            node_address: "localhost".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            size_gb: 1.0,
            sstable_count: 10,
            snapshot_type: "MANUAL".to_string(),
            age_days,
            is_expired: false,
            status: "ACTIVE".to_string(),
            storage_path: None,
            metadata: HashMap::new(),
            compression_ratio: None,
            backup_job_id: None,
        };

        let snapshot_info = CassandraSnapshotInfo {
            snapshot_details: vec![
                make_snapshot("recent", 0.5),
                make_snapshot("weekly", 5.0),
                make_snapshot("old", 120.0),
            ],
            ..Default::default()
        };

        let ranges = snapshot_info.snapshots_by_age_ranges();
        assert_eq!(ranges.get("Last 24 hours").map(|v| v.len()), Some(1));
        assert_eq!(ranges.get("Last week").map(|v| v.len()), Some(1));
        assert_eq!(ranges.get("Older than 3 months").map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_recommendations_generation() {
        let mut snapshot_info = CassandraSnapshotInfo { backup_coverage_percentage: 70.0, ..Default::default() };

        snapshot_info.retention_compliance.compliance_percentage = 60.0;
        snapshot_info.health_metrics.corrupted_snapshots = 2;
        snapshot_info.reclaimable_storage_gb = 15.0;
        snapshot_info.storage_metrics.avg_compression_ratio = 1.5;
        snapshot_info.snapshot_creation_rate = 0.3;
        snapshot_info.retention_compliance.auto_cleanup_enabled = false;

        let recommendations = snapshot_info.get_snapshot_recommendations();

        assert!(recommendations.len() >= 6);
        assert!(recommendations.iter().any(|r| r.contains("coverage")));
        assert!(recommendations.iter().any(|r| r.contains("retention compliance")));
        assert!(recommendations.iter().any(|r| r.contains("corrupted")));
        assert!(recommendations.iter().any(|r| r.contains("reclaim")));
        assert!(recommendations.iter().any(|r| r.contains("compression")));
        assert!(recommendations.iter().any(|r| r.contains("automatic")));
    }

    #[test]
    fn test_default_produces_empty_snapshot_details() {
        let info = CassandraSnapshotInfo::default();
        assert!(info.snapshot_details.is_empty());
        assert_eq!(info.total_snapshots, 0);
    }

    #[test]
    fn test_calculate_basic_statistics_empty() {
        let mut info = CassandraSnapshotInfo::default();
        CassandraSnapshotInfo::calculate_basic_statistics(&mut info);
        assert_eq!(info.total_snapshots, 0);
        assert_eq!(info.keyspaces_with_snapshots, 0);
    }
}
