use crate::api::lib::QueryUnpagedInput;
use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{self, DEFAULT_QUERY_TIMEOUT};

/// Cassandra tombstone information and monitoring
///
/// Tracks tombstone gc_grace configuration across keyspaces and tables.
///
/// Note: Cassandra does not expose tombstone counts, ratios or creation rates
/// via CQL system tables. Those metrics are only available through JMX
/// (`org.apache.cassandra.metrics:type=Table,name=TombstoneScannedHistogram`)
/// or `nodetool tablestats`. All tombstone-count and performance-impact fields
/// in this struct will be zero until a JMX or nodetool integration is added.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneInfo {
    /// Total number of tombstones across all tables
    pub total_tombstones: u64,
    /// Number of keyspaces monitored
    pub total_keyspaces: u64,
    /// Number of tables monitored
    pub total_tables: u64,
    /// Number of tables with high tombstone ratios
    pub high_tombstone_ratio_tables: u64,
    /// Number of tables with tombstone warnings
    pub tombstone_warning_tables: u64,
    /// Number of tables with critical tombstone levels
    pub tombstone_critical_tables: u64,
    /// Average tombstone ratio across all tables (percentage)
    pub avg_tombstone_ratio_pct: f64,
    /// Maximum tombstone ratio found (percentage)
    pub max_tombstone_ratio_pct: f64,
    /// Total live cells across all tables
    pub total_live_cells: u64,
    /// Estimated tombstone overhead in MB
    pub tombstone_overhead_mb: f64,
    /// Overall tombstone health score (0-100)
    pub overall_health_score: f64,
    /// Detailed information for each table
    pub table_details: Vec<CassandraTombstoneTableDetail>,
    /// Tombstone performance metrics
    pub performance_metrics: CassandraTombstonePerformanceMetrics,
    /// Tombstone health alerts and warnings
    pub health_alerts: CassandraTombstoneHealthAlerts,
    /// Tombstone cleanup recommendations
    pub cleanup_recommendations: Vec<CassandraTombstoneRecommendation>,
    /// GC grace settings analysis
    pub gc_grace_analysis: CassandraGcGraceAnalysis,
}

/// Detailed tombstone information for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneTableDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Total number of tombstones in the table
    pub tombstone_count: u64,
    /// Total number of live cells in the table
    pub live_cell_count: u64,
    /// Tombstone to live cell ratio (percentage)
    pub tombstone_ratio_pct: f64,
    /// Estimated size of tombstones in MB
    pub tombstone_size_mb: f64,
    /// Average age of tombstones in hours
    pub avg_tombstone_age_hours: f64,
    /// Maximum age of tombstones in hours
    pub max_tombstone_age_hours: f64,
    /// Number of tombstones older than GC grace
    pub expired_tombstones: u64,
    /// GC grace period in seconds
    pub gc_grace_seconds: u64,
    /// Tombstone creation rate (tombstones per hour)
    pub tombstone_creation_rate: f64,
    /// Health status (HEALTHY, WARNING, CRITICAL)
    pub health_status: String,
    /// Performance impact score (0-100)
    pub performance_impact_score: f64,
    /// Read performance degradation percentage
    pub read_performance_degradation_pct: f64,
    /// Recent tombstone metrics
    pub recent_metrics: Vec<CassandraTombstoneHistoricalMetric>,
    /// Active alerts for this table
    pub active_alerts: Vec<String>,
}

/// Historical tombstone metric for a table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneHistoricalMetric {
    /// Timestamp of the metric
    pub timestamp: String,
    /// Tombstone count at this time
    pub tombstone_count: u64,
    /// Live cell count at this time
    pub live_cell_count: u64,
    /// Tombstone ratio at this time
    pub tombstone_ratio_pct: f64,
    /// Creation rate at this time
    pub creation_rate: f64,
}

/// Overall tombstone performance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstonePerformanceMetrics {
    /// Total tombstone creation rate (tombstones/hour)
    pub total_creation_rate: f64,
    /// Average read latency impact from tombstones (ms)
    pub avg_read_latency_impact_ms: f64,
    /// 95th percentile read latency impact (ms)
    pub p95_read_latency_impact_ms: f64,
    /// Query timeout rate due to tombstones (percentage)
    pub query_timeout_rate_pct: f64,
    /// Compaction impact from tombstones (0-100)
    pub compaction_impact_score: f64,
    /// Storage overhead from tombstones (percentage)
    pub storage_overhead_pct: f64,
    /// Memory usage by tombstones (MB)
    pub tombstone_memory_usage_mb: f64,
    /// CPU overhead from tombstone processing
    pub cpu_overhead_pct: f64,
    /// Network overhead from tombstone transfers
    pub network_overhead_mb_per_sec: f64,
    /// Overall performance degradation score
    pub performance_degradation_score: f64,
}

/// Tombstone health alerts and warnings
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneHealthAlerts {
    /// Critical alerts requiring immediate attention
    pub critical_alerts: Vec<CassandraTombstoneAlert>,
    /// Warning alerts for monitoring
    pub warning_alerts: Vec<CassandraTombstoneAlert>,
    /// Informational alerts
    pub info_alerts: Vec<CassandraTombstoneAlert>,
    /// Number of tables in critical state
    pub critical_tables: u64,
    /// Number of tables in warning state
    pub warning_tables: u64,
    /// Alert summary by category
    pub alert_summary: HashMap<String, u64>,
    /// Recent alert history
    pub recent_alert_history: Vec<CassandraTombstoneAlertHistory>,
}

/// Individual tombstone alert
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneAlert {
    /// Alert severity (CRITICAL, WARNING, INFO)
    pub severity: String,
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Alert type
    pub alert_type: String,
    /// Alert message
    pub message: String,
    /// Current value triggering alert
    pub current_value: f64,
    /// Threshold value
    pub threshold_value: f64,
    /// Alert timestamp
    pub timestamp: String,
    /// Duration alert has been active
    pub duration_hours: f64,
    /// Recommended action
    pub recommended_action: String,
}

/// Historical alert information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneAlertHistory {
    /// Alert timestamp
    pub timestamp: String,
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Alert type
    pub alert_type: String,
    /// Alert severity
    pub severity: String,
    /// Alert resolved or still active
    pub is_resolved: bool,
    /// Resolution timestamp
    pub resolved_at: Option<String>,
}

/// Tombstone cleanup recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneRecommendation {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Recommendation type
    pub recommendation_type: String,
    /// Current configuration value
    pub current_value: String,
    /// Recommended configuration value
    pub recommended_value: String,
    /// Justification for recommendation
    pub justification: String,
    /// Expected improvement
    pub expected_improvement: String,
    /// Implementation priority (HIGH, MEDIUM, LOW)
    pub priority: String,
    /// Estimated cleanup time
    pub estimated_cleanup_time_hours: f64,
    /// Potential risks of cleanup
    pub risks: Vec<String>,
}

/// GC grace period analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraGcGraceAnalysis {
    /// Number of tables with default GC grace
    pub tables_with_default_gc_grace: u64,
    /// Number of tables with custom GC grace
    pub tables_with_custom_gc_grace: u64,
    /// Number of tables with too short GC grace
    pub tables_with_short_gc_grace: u64,
    /// Number of tables with too long GC grace
    pub tables_with_long_gc_grace: u64,
    /// Average GC grace period across tables (seconds)
    pub avg_gc_grace_seconds: f64,
    /// Recommended GC grace adjustments
    pub gc_grace_recommendations: Vec<CassandraGcGraceRecommendation>,
}

/// GC grace period recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraGcGraceRecommendation {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Current GC grace period (seconds)
    pub current_gc_grace_seconds: u64,
    /// Recommended GC grace period (seconds)
    pub recommended_gc_grace_seconds: u64,
    /// Reason for recommendation
    pub reason: String,
    /// Expected impact
    pub expected_impact: String,
}

impl MetadataCollection for CassandraTombstoneInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        utils::query_map([
            // Query real gc_grace_seconds per user table -- the only tombstone-related
            // configuration accessible via CQL. Tombstone counts, ratios and rates
            // are available only via JMX or nodetool and are not collected here.
            (
                "table_settings",
                utils::query(
                    "SELECT keyspace_name, table_name, gc_grace_seconds \
                     FROM system_schema.tables",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return Cassandra tombstone gc_grace configuration per table"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "tombstone"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl CassandraTombstoneInfo {
    const DEFAULT_GC_GRACE_SECONDS: u64 = 864000; // 10 days
    const MIN_RECOMMENDED_GC_GRACE: u64 = 86400; // 1 day
    const MAX_RECOMMENDED_GC_GRACE: u64 = 1728000; // 20 days

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let requests = self.request();
        let table_settings_data = utils::run_named_query(&requests, "table_settings", context, DEFAULT_QUERY_TIMEOUT).await?;

        let table_details = Self::build_table_details(&table_settings_data);
        let gc_grace_analysis = Self::analyze_gc_grace_settings(&table_details);

        let mut keyspaces = std::collections::HashSet::new();
        for t in &table_details {
            keyspaces.insert(t.keyspace_name.as_str());
        }

        Ok(CassandraTombstoneInfo {
            total_tables: table_details.len() as u64,
            total_keyspaces: keyspaces.len() as u64,
            gc_grace_analysis,
            table_details,
            // Tombstone counts and all derived metrics require JMX / nodetool data
            // that is not available via CQL. Return honest zero defaults.
            ..Default::default()
        })
    }

    /// Build per-table detail rows from the `system_schema.tables` query result.
    ///
    /// Only `gc_grace_seconds` is populated from real data. All tombstone-count
    /// and performance-impact fields default to zero because Cassandra does not
    /// expose them over CQL.
    const SYSTEM_KEYSPACES: &[&str] = &[
        "system",
        "system_schema",
        "system_auth",
        "system_distributed",
        "system_traces",
        "system_views",
        "system_virtual_schema",
    ];

    fn build_table_details(table_settings: &Value) -> Vec<CassandraTombstoneTableDetail> {
        utils::map_rows(table_settings, |row| {
            let keyspace_name = utils::get_string(row, "keyspace_name")?;
            if Self::SYSTEM_KEYSPACES.contains(&keyspace_name.as_str()) {
                return None;
            }
            let table_name = utils::get_string(row, "table_name")?;
            let gc_grace_seconds = utils::get_u64(row, "gc_grace_seconds").unwrap_or(Self::DEFAULT_GC_GRACE_SECONDS);

            Some(CassandraTombstoneTableDetail {
                keyspace_name,
                table_name,
                gc_grace_seconds,
                health_status: "UNKNOWN".to_string(),
                // All tombstone metrics require JMX/nodetool; zero until available.
                tombstone_count: 0,
                live_cell_count: 0,
                tombstone_ratio_pct: 0.0,
                tombstone_size_mb: 0.0,
                avg_tombstone_age_hours: 0.0,
                max_tombstone_age_hours: 0.0,
                expired_tombstones: 0,
                tombstone_creation_rate: 0.0,
                performance_impact_score: 0.0,
                read_performance_degradation_pct: 0.0,
                recent_metrics: Vec::new(),
                active_alerts: Vec::new(),
            })
        })
    }

    /// Analyze GC grace period settings, which are real CQL-queryable values.
    fn analyze_gc_grace_settings(table_details: &[CassandraTombstoneTableDetail]) -> CassandraGcGraceAnalysis {
        if table_details.is_empty() {
            return CassandraGcGraceAnalysis::default();
        }

        let mut analysis = CassandraGcGraceAnalysis::default();
        let mut gc_grace_sum: u64 = 0;
        let mut gc_grace_recommendations = Vec::new();

        for table in table_details {
            gc_grace_sum += table.gc_grace_seconds;

            if table.gc_grace_seconds == Self::DEFAULT_GC_GRACE_SECONDS {
                analysis.tables_with_default_gc_grace += 1;
            } else {
                analysis.tables_with_custom_gc_grace += 1;
            }

            if table.gc_grace_seconds < Self::MIN_RECOMMENDED_GC_GRACE {
                analysis.tables_with_short_gc_grace += 1;
                gc_grace_recommendations.push(CassandraGcGraceRecommendation {
                    keyspace_name: table.keyspace_name.clone(),
                    table_name: table.table_name.clone(),
                    current_gc_grace_seconds: table.gc_grace_seconds,
                    recommended_gc_grace_seconds: Self::MIN_RECOMMENDED_GC_GRACE,
                    reason: "Current GC grace is shorter than the minimum recommended period; \
                             this may cause data resurrection on node recovery."
                        .to_string(),
                    expected_impact: "Reduced risk of data inconsistency after node restarts.".to_string(),
                });
            } else if table.gc_grace_seconds > Self::MAX_RECOMMENDED_GC_GRACE {
                analysis.tables_with_long_gc_grace += 1;
                gc_grace_recommendations.push(CassandraGcGraceRecommendation {
                    keyspace_name: table.keyspace_name.clone(),
                    table_name: table.table_name.clone(),
                    current_gc_grace_seconds: table.gc_grace_seconds,
                    recommended_gc_grace_seconds: Self::DEFAULT_GC_GRACE_SECONDS,
                    reason: "GC grace exceeds the maximum recommended period, preventing \
                             efficient tombstone cleanup."
                        .to_string(),
                    expected_impact: "Faster tombstone cleanup and reduced storage overhead.".to_string(),
                });
            }
        }

        analysis.avg_gc_grace_seconds = gc_grace_sum as f64 / table_details.len() as f64;
        analysis.gc_grace_recommendations = gc_grace_recommendations;

        analysis
    }
}

impl CassandraTombstoneInfo {
    /// Checks if there are critical tombstone issues requiring immediate attention.
    ///
    /// Note: until JMX/nodetool tombstone metrics are integrated, this always
    /// returns `false` because all count and ratio fields are zero.
    pub fn has_critical_tombstone_issues(&self) -> bool {
        self.overall_health_score < 60.0
            || self.health_alerts.critical_tables > 0
            || self.max_tombstone_ratio_pct > 50.0
            || self.tombstone_critical_tables > 0
    }

    /// Gets tables that need immediate attention based on available data.
    pub fn tables_needing_immediate_attention(&self) -> Vec<&CassandraTombstoneTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                table.health_status == "CRITICAL"
                    || table.tombstone_ratio_pct > 50.0
                    || table.tombstone_creation_rate > 5000.0
                    || table.expired_tombstones > 1000
            })
            .collect()
    }

    /// Gets tables with performance issues based on available data.
    pub fn tables_with_performance_issues(&self) -> Vec<&CassandraTombstoneTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                table.performance_impact_score > 50.0 || table.read_performance_degradation_pct > 25.0 || !table.active_alerts.is_empty()
            })
            .collect()
    }

    /// Gets tables with highest tombstone ratios up to `limit` entries.
    pub fn highest_tombstone_ratio_tables(&self, limit: usize) -> Vec<&CassandraTombstoneTableDetail> {
        let mut tables = self.table_details.iter().collect::<Vec<_>>();
        tables.sort_by(|a, b| b.tombstone_ratio_pct.partial_cmp(&a.tombstone_ratio_pct).unwrap_or(std::cmp::Ordering::Equal));
        tables.into_iter().take(limit).collect()
    }

    /// Gets tables with highest creation rates up to `limit` entries.
    pub fn highest_creation_rate_tables(&self, limit: usize) -> Vec<&CassandraTombstoneTableDetail> {
        let mut tables = self.table_details.iter().collect::<Vec<_>>();
        tables.sort_by(|a, b| b.tombstone_creation_rate.partial_cmp(&a.tombstone_creation_rate).unwrap_or(std::cmp::Ordering::Equal));
        tables.into_iter().take(limit).collect()
    }

    /// Gets overall tombstone health rating (A-F scale).
    pub fn tombstone_health_rating(&self) -> String {
        match self.overall_health_score {
            s if s >= 90.0 => "A".to_string(),
            s if s >= 80.0 => "B".to_string(),
            s if s >= 70.0 => "C".to_string(),
            s if s >= 60.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Gets tombstone distribution statistics.
    pub fn get_tombstone_distribution_stats(&self) -> CassandraTombstoneDistributionStats {
        CassandraTombstoneDistributionStats {
            tables_by_ratio_ranges: self.get_tables_by_ratio_ranges(),
            tables_by_health_status: self.get_tables_by_health_status(),
            keyspaces_by_tombstone_count: self.get_keyspaces_by_tombstone_count(),
            alert_distribution: self.health_alerts.alert_summary.clone(),
        }
    }

    fn get_tables_by_ratio_ranges(&self) -> HashMap<String, u64> {
        let mut ranges = HashMap::from([
            ("0-10%".to_string(), 0u64),
            ("11-20%".to_string(), 0u64),
            ("21-50%".to_string(), 0u64),
            ("51%+".to_string(), 0u64),
        ]);

        for table in &self.table_details {
            let key = match table.tombstone_ratio_pct {
                r if r <= 10.0 => "0-10%",
                r if r <= 20.0 => "11-20%",
                r if r <= 50.0 => "21-50%",
                _ => "51%+",
            };
            if let Some(count) = ranges.get_mut(key) {
                *count += 1;
            }
        }

        ranges
    }

    fn get_tables_by_health_status(&self) -> HashMap<String, u64> {
        let mut status_counts: HashMap<String, u64> = HashMap::new();
        for table in &self.table_details {
            *status_counts.entry(table.health_status.clone()).or_insert(0) += 1;
        }
        status_counts
    }

    fn get_keyspaces_by_tombstone_count(&self) -> HashMap<String, u64> {
        let mut keyspace_counts: HashMap<String, u64> = HashMap::new();
        for table in &self.table_details {
            *keyspace_counts.entry(table.keyspace_name.clone()).or_insert(0) += table.tombstone_count;
        }
        keyspace_counts
    }

    /// Gets summary for reporting.
    pub fn get_tombstone_summary(&self) -> CassandraTombstoneSummary {
        CassandraTombstoneSummary {
            total_tombstones: self.total_tombstones,
            total_tables: self.total_tables,
            avg_tombstone_ratio_pct: self.avg_tombstone_ratio_pct,
            max_tombstone_ratio_pct: self.max_tombstone_ratio_pct,
            tables_with_issues: self.tombstone_warning_tables + self.tombstone_critical_tables,
            health_score: self.overall_health_score,
            health_rating: self.tombstone_health_rating(),
            critical_alerts: self.health_alerts.critical_alerts.len() as u64,
            tombstone_overhead_mb: self.tombstone_overhead_mb,
            has_critical_issues: self.has_critical_tombstone_issues(),
            avg_creation_rate: self.performance_metrics.total_creation_rate / self.total_tables.max(1) as f64,
        }
    }
}

/// Tombstone distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraTombstoneDistributionStats {
    pub tables_by_ratio_ranges: HashMap<String, u64>,
    pub tables_by_health_status: HashMap<String, u64>,
    pub keyspaces_by_tombstone_count: HashMap<String, u64>,
    pub alert_distribution: HashMap<String, u64>,
}

/// Summary statistics for tombstone information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTombstoneSummary {
    pub total_tombstones: u64,
    pub total_tables: u64,
    pub avg_tombstone_ratio_pct: f64,
    pub max_tombstone_ratio_pct: f64,
    pub tables_with_issues: u64,
    pub health_score: f64,
    pub health_rating: String,
    pub critical_alerts: u64,
    pub tombstone_overhead_mb: f64,
    pub has_critical_issues: bool,
    pub avg_creation_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_critical_issues_detection() {
        let mut tombstone_info = CassandraTombstoneInfo { overall_health_score: 80.0, ..Default::default() };

        // No issues
        tombstone_info.health_alerts.critical_tables = 0;
        tombstone_info.max_tombstone_ratio_pct = 15.0;
        tombstone_info.tombstone_critical_tables = 0;
        assert!(!tombstone_info.has_critical_tombstone_issues());

        // Low health score
        tombstone_info.overall_health_score = 50.0;
        assert!(tombstone_info.has_critical_tombstone_issues());

        // Reset and test critical tables
        tombstone_info.overall_health_score = 80.0;
        tombstone_info.health_alerts.critical_tables = 1;
        assert!(tombstone_info.has_critical_tombstone_issues());

        // Reset and test max tombstone ratio
        tombstone_info.health_alerts.critical_tables = 0;
        tombstone_info.max_tombstone_ratio_pct = 60.0;
        assert!(tombstone_info.has_critical_tombstone_issues());
    }

    #[test]
    fn test_health_rating() {
        let mut tombstone_info = CassandraTombstoneInfo { overall_health_score: 95.0, ..Default::default() };

        assert_eq!(tombstone_info.tombstone_health_rating(), "A");

        tombstone_info.overall_health_score = 85.0;
        assert_eq!(tombstone_info.tombstone_health_rating(), "B");

        tombstone_info.overall_health_score = 75.0;
        assert_eq!(tombstone_info.tombstone_health_rating(), "C");

        tombstone_info.overall_health_score = 65.0;
        assert_eq!(tombstone_info.tombstone_health_rating(), "D");

        tombstone_info.overall_health_score = 45.0;
        assert_eq!(tombstone_info.tombstone_health_rating(), "F");
    }

    #[test]
    fn test_ratio_ranges() {
        let tombstone_info = CassandraTombstoneInfo {
            table_details: vec![
                CassandraTombstoneTableDetail {
                    keyspace_name: "test".to_string(),
                    table_name: "low_ratio".to_string(),
                    tombstone_count: 100,
                    live_cell_count: 10000,
                    tombstone_ratio_pct: 5.0, // 0-10% range
                    tombstone_size_mb: 0.1,
                    avg_tombstone_age_hours: 24.0,
                    max_tombstone_age_hours: 48.0,
                    expired_tombstones: 0,
                    gc_grace_seconds: 864000,
                    tombstone_creation_rate: 10.0,
                    health_status: "HEALTHY".to_string(),
                    performance_impact_score: 5.0,
                    read_performance_degradation_pct: 2.5,
                    recent_metrics: vec![],
                    active_alerts: vec![],
                },
                CassandraTombstoneTableDetail {
                    keyspace_name: "test".to_string(),
                    table_name: "high_ratio".to_string(),
                    tombstone_count: 3000,
                    live_cell_count: 10000,
                    tombstone_ratio_pct: 30.0, // 21-50% range
                    tombstone_size_mb: 3.0,
                    avg_tombstone_age_hours: 72.0,
                    max_tombstone_age_hours: 120.0,
                    expired_tombstones: 50,
                    gc_grace_seconds: 864000,
                    tombstone_creation_rate: 100.0,
                    health_status: "WARNING".to_string(),
                    performance_impact_score: 45.0,
                    read_performance_degradation_pct: 15.0,
                    recent_metrics: vec![],
                    active_alerts: vec!["High tombstone ratio".to_string()],
                },
            ],
            ..Default::default()
        };

        let ranges = tombstone_info.get_tables_by_ratio_ranges();
        assert_eq!(ranges.get("0-10%"), Some(&1));
        assert_eq!(ranges.get("21-50%"), Some(&1));
        assert_eq!(ranges.get("11-20%"), Some(&0));
        assert_eq!(ranges.get("51%+"), Some(&0));
    }

    #[test]
    fn test_gc_grace_analysis_short_grace() {
        let tables = vec![CassandraTombstoneTableDetail {
            keyspace_name: "ks".to_string(),
            table_name: "t".to_string(),
            gc_grace_seconds: 3600, // 1 hour, too short
            tombstone_count: 0,
            live_cell_count: 0,
            tombstone_ratio_pct: 0.0,
            tombstone_size_mb: 0.0,
            avg_tombstone_age_hours: 0.0,
            max_tombstone_age_hours: 0.0,
            expired_tombstones: 0,
            tombstone_creation_rate: 0.0,
            health_status: "UNKNOWN".to_string(),
            performance_impact_score: 0.0,
            read_performance_degradation_pct: 0.0,
            recent_metrics: vec![],
            active_alerts: vec![],
        }];

        let analysis = CassandraTombstoneInfo::analyze_gc_grace_settings(&tables);
        assert_eq!(analysis.tables_with_short_gc_grace, 1);
        assert_eq!(analysis.tables_with_long_gc_grace, 0);
        assert_eq!(analysis.gc_grace_recommendations.len(), 1);
        assert_eq!(analysis.gc_grace_recommendations[0].recommended_gc_grace_seconds, 86400);
    }

    #[test]
    fn test_gc_grace_analysis_long_grace() {
        let tables = vec![CassandraTombstoneTableDetail {
            keyspace_name: "ks".to_string(),
            table_name: "t".to_string(),
            gc_grace_seconds: 2_000_000, // ~23 days, too long
            tombstone_count: 0,
            live_cell_count: 0,
            tombstone_ratio_pct: 0.0,
            tombstone_size_mb: 0.0,
            avg_tombstone_age_hours: 0.0,
            max_tombstone_age_hours: 0.0,
            expired_tombstones: 0,
            tombstone_creation_rate: 0.0,
            health_status: "UNKNOWN".to_string(),
            performance_impact_score: 0.0,
            read_performance_degradation_pct: 0.0,
            recent_metrics: vec![],
            active_alerts: vec![],
        }];

        let analysis = CassandraTombstoneInfo::analyze_gc_grace_settings(&tables);
        assert_eq!(analysis.tables_with_long_gc_grace, 1);
        assert_eq!(analysis.tables_with_short_gc_grace, 0);
        assert_eq!(analysis.gc_grace_recommendations.len(), 1);
        assert_eq!(analysis.gc_grace_recommendations[0].recommended_gc_grace_seconds, 864000);
    }

    #[test]
    fn test_gc_grace_analysis_default_grace() {
        let tables = vec![CassandraTombstoneTableDetail {
            keyspace_name: "ks".to_string(),
            table_name: "t".to_string(),
            gc_grace_seconds: 864000, // default 10 days
            tombstone_count: 0,
            live_cell_count: 0,
            tombstone_ratio_pct: 0.0,
            tombstone_size_mb: 0.0,
            avg_tombstone_age_hours: 0.0,
            max_tombstone_age_hours: 0.0,
            expired_tombstones: 0,
            tombstone_creation_rate: 0.0,
            health_status: "UNKNOWN".to_string(),
            performance_impact_score: 0.0,
            read_performance_degradation_pct: 0.0,
            recent_metrics: vec![],
            active_alerts: vec![],
        }];

        let analysis = CassandraTombstoneInfo::analyze_gc_grace_settings(&tables);
        assert_eq!(analysis.tables_with_default_gc_grace, 1);
        assert_eq!(analysis.tables_with_custom_gc_grace, 0);
        assert_eq!(analysis.tables_with_short_gc_grace, 0);
        assert_eq!(analysis.tables_with_long_gc_grace, 0);
        assert!(analysis.gc_grace_recommendations.is_empty());
        assert_eq!(analysis.avg_gc_grace_seconds, 864000.0);
    }

    #[test]
    fn test_build_table_details_parses_gc_grace() {
        use serde_json::json;

        let data = json!([
            {"keyspace_name": "app", "table_name": "events", "gc_grace_seconds": 172800},
            {"keyspace_name": "app", "table_name": "users"},
        ]);

        let details = CassandraTombstoneInfo::build_table_details(&data);
        assert_eq!(details.len(), 2);

        let events = &details[0];
        assert_eq!(events.keyspace_name, "app");
        assert_eq!(events.table_name, "events");
        assert_eq!(events.gc_grace_seconds, 172800);
        // Tombstone counts must be zero (not available via CQL).
        assert_eq!(events.tombstone_count, 0);

        let users = &details[1];
        // Falls back to default when gc_grace_seconds is absent.
        assert_eq!(users.gc_grace_seconds, 864000);
    }
}
