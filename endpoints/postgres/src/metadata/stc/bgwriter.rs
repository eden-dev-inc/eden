use crate::api::lib::query::QueryInput;
use crate::metadata::capabilities::PG_VERSION_17;
use crate::metadata::stc::utils::{run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL background writer (bgwriter) statistics and information
///
/// Simplified struct containing essential metrics about the background writer process,
/// checkpoint statistics, and buffer management performance.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresBgWriterInfo {
    /// Number of scheduled checkpoints performed
    pub checkpoints_timed: u64,
    /// Number of requested checkpoints performed
    pub checkpoints_req: u64,
    /// Total time spent writing files during checkpoints (milliseconds)
    pub checkpoint_write_time: f64,
    /// Total time spent syncing files during checkpoints (milliseconds)
    pub checkpoint_sync_time: f64,
    /// Number of buffers written during checkpoints
    pub buffers_checkpoint: u64,
    /// Number of buffers written by background writer
    pub buffers_clean: u64,
    /// Number of times background writer stopped due to too many buffers
    pub maxwritten_clean: u64,
    /// Number of buffers written directly by backends
    pub buffers_backend: u64,
    /// Number of times backends had to execute their own fsync
    pub buffers_backend_fsync: u64,
    /// Number of buffers allocated
    pub buffers_alloc: u64,
    /// When these statistics were last reset
    pub stats_reset: Option<DateTimeWrapper>,
    /// Percentage of checkpoints that were requested vs timed (0.0 to 100.0)
    pub requested_checkpoint_ratio: f64,
    /// Average checkpoint write time (milliseconds)
    pub avg_checkpoint_write_time: f64,
    /// Average checkpoint sync time (milliseconds)
    pub avg_checkpoint_sync_time: f64,
    /// Background writer efficiency percentage (0.0 to 100.0)
    pub bgwriter_efficiency: f64,
    /// Overall health score (0.0 to 100.0, higher is better)
    pub health_score: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresBgWriterDetailedMetrics>,
}

/// Detailed background writer metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresBgWriterDetailedMetrics {
    /// Configuration settings (collected when health_score < 70)
    pub checkpoint_settings: Option<Vec<PostgresConfigSetting>>,
    /// Background writer settings (collected when bgwriter_efficiency < 60)
    pub bgwriter_settings: Option<Vec<PostgresConfigSetting>>,
    /// Tuning recommendations based on current performance
    pub tuning_recommendations: Vec<String>,
}

/// PostgreSQL configuration setting
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConfigSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub setting: String,
    /// Unit of measurement (if any)
    pub unit: Option<String>,
}

impl MetadataCollection for PostgresBgWriterInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "bgwriter_stats".to_string(),
                QueryInput::new(
                    "SELECT
                        buffers_clean, maxwritten_clean, buffers_alloc, stats_reset
                    FROM pg_stat_bgwriter"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "checkpointer_stats".to_string(),
                QueryInput::new(
                    "SELECT
                        num_timed, num_requested, write_time, sync_time,
                        buffers_written, stats_reset
                    FROM pg_stat_checkpointer"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL background writer statistics with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "bgwriter"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresBgWriterInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const HEALTH_THRESHOLD: f64 = 70.0;
    const EFFICIENCY_THRESHOLD: f64 = 60.0;

    fn checkpointer_stats_query(capabilities: &dyn CapabilityChecker) -> QueryInput {
        let sql = if capabilities.has(&PG_VERSION_17) {
            "SELECT
                num_timed, num_requested, write_time, sync_time,
                buffers_written, stats_reset
            FROM pg_stat_checkpointer"
        } else {
            "SELECT
                checkpoints_timed AS num_timed,
                checkpoints_req AS num_requested,
                checkpoint_write_time AS write_time,
                checkpoint_sync_time AS sync_time,
                buffers_checkpoint AS buffers_written,
                buffers_backend,
                buffers_backend_fsync,
                stats_reset
            FROM pg_stat_bgwriter"
        };

        QueryInput::new(sql.to_string(), Vec::new())
    }

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut bgwriter_info = PostgresBgWriterInfo::default();
        let requests = self.request();

        // Execute bgwriter stats query (PG17+: only buffers_clean, maxwritten_clean, buffers_alloc, stats_reset)
        if let Some(row) = run_single_row(&requests, "bgwriter_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            bgwriter_info.buffers_clean = Self::safe_i64_to_u64(&row, "buffers_clean")?;
            bgwriter_info.maxwritten_clean = Self::safe_i64_to_u64(&row, "maxwritten_clean")?;
            bgwriter_info.buffers_alloc = Self::safe_i64_to_u64(&row, "buffers_alloc")?;
            bgwriter_info.stats_reset = Self::safe_get_optional_datetime(&row, "stats_reset")?;
        }

        // PG17 split checkpoint stats into pg_stat_checkpointer; older versions still expose them on pg_stat_bgwriter.
        let checkpointer_rows = run_query_with_timeout(
            &Self::checkpointer_stats_query(capabilities),
            context.clone(),
            Self::QUERY_TIMEOUT,
            "checkpointer_stats",
        )
        .await?;
        if let Some(row) = checkpointer_rows.first() {
            bgwriter_info.checkpoints_timed = Self::safe_i64_to_u64(row, "num_timed")?;
            bgwriter_info.checkpoints_req = Self::safe_i64_to_u64(row, "num_requested")?;
            bgwriter_info.checkpoint_write_time = Self::safe_get_f64(row, "write_time")?;
            bgwriter_info.checkpoint_sync_time = Self::safe_get_f64(row, "sync_time")?;
            bgwriter_info.buffers_checkpoint = Self::safe_i64_to_u64(row, "buffers_written")?;

            if !capabilities.has(&PG_VERSION_17) {
                bgwriter_info.buffers_backend = Self::safe_i64_to_u64(row, "buffers_backend")?;
                bgwriter_info.buffers_backend_fsync = Self::safe_i64_to_u64(row, "buffers_backend_fsync")?;
            }
        }

        // Calculate derived metrics
        bgwriter_info.requested_checkpoint_ratio = bgwriter_info.calculate_requested_checkpoint_ratio();
        bgwriter_info.avg_checkpoint_write_time = bgwriter_info.calculate_avg_checkpoint_write_time();
        bgwriter_info.avg_checkpoint_sync_time = bgwriter_info.calculate_avg_checkpoint_sync_time();
        bgwriter_info.bgwriter_efficiency = bgwriter_info.calculate_bgwriter_efficiency();
        bgwriter_info.health_score = bgwriter_info.calculate_health_score();

        // Conditionally collect detailed metrics only when problems are detected
        bgwriter_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&bgwriter_info, context).await?;

        Ok(bgwriter_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresBgWriterInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresBgWriterDetailedMetrics>> {
        let needs_checkpoint_config = core_info.health_score < Self::HEALTH_THRESHOLD;
        let needs_bgwriter_config = core_info.bgwriter_efficiency < Self::EFFICIENCY_THRESHOLD;

        if !needs_checkpoint_config && !needs_bgwriter_config {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresBgWriterDetailedMetrics {
            checkpoint_settings: None,
            bgwriter_settings: None,
            tuning_recommendations: core_info.generate_tuning_recommendations(),
        };

        // Collect checkpoint configuration if health is poor
        if needs_checkpoint_config {
            let checkpoint_config_input = QueryInput::new(
                "SELECT name, setting, unit
                FROM pg_settings
                WHERE name IN (
                    'checkpoint_timeout', 'checkpoint_completion_target', 'checkpoint_warning',
                    'max_wal_size', 'min_wal_size', 'checkpoint_flush_after'
                )
                ORDER BY name"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&checkpoint_config_input, context.clone(), Self::QUERY_TIMEOUT, "checkpoint_settings").await
            {
                detailed_metrics.checkpoint_settings = Some(Self::parse_config_settings(rows)?);
            }
        }

        // Collect bgwriter configuration if efficiency is poor
        if needs_bgwriter_config {
            let bgwriter_config_input = QueryInput::new(
                "SELECT name, setting, unit
                FROM pg_settings
                WHERE name IN (
                    'bgwriter_delay', 'bgwriter_lru_maxpages', 'bgwriter_lru_multiplier',
                    'bgwriter_flush_after'
                )
                ORDER BY name"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&bgwriter_config_input, context.clone(), Self::QUERY_TIMEOUT, "bgwriter_settings").await
            {
                detailed_metrics.bgwriter_settings = Some(Self::parse_config_settings(rows)?);
            }
        }

        Ok(Some(detailed_metrics))
    }

    // Helper functions for safe type conversion and extraction
    fn safe_i64_to_u64(row: &PgSimpleRow, column: &str) -> ResultEP<u64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        let value = text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))?;

        if value < 0 {
            return Err(EpError::metadata(format!("Negative value for {}: {}", column, value)));
        }
        Ok(value as u64)
    }

    fn safe_get_f64(row: &PgSimpleRow, column: &str) -> ResultEP<f64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<f64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_optional_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        match row.get(column) {
            Some(text) => {
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                Err(EpError::metadata(format!(
                    "Failed to get datetime column {column}: cannot parse '{text}' as timestamp"
                )))
            }
            None => Ok(None),
        }
    }

    fn parse_config_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresConfigSetting>> {
        let mut settings = Vec::with_capacity(rows.len());

        for row in rows {
            settings.push(PostgresConfigSetting {
                name: Self::safe_get_string(&row, "name")?,
                setting: Self::safe_get_string(&row, "setting")?,
                unit: Self::safe_get_optional_string(&row, "unit")?,
            });
        }

        Ok(settings)
    }
}

impl PostgresBgWriterInfo {
    /// Calculates the total number of checkpoints
    pub fn get_total_checkpoints(&self) -> u64 {
        self.checkpoints_timed + self.checkpoints_req
    }

    /// Calculates the ratio of requested to total checkpoints
    fn calculate_requested_checkpoint_ratio(&self) -> f64 {
        let total = self.get_total_checkpoints();
        if total == 0 {
            0.0
        } else {
            (self.checkpoints_req as f64 / total as f64) * 100.0
        }
    }

    /// Calculates average checkpoint write time
    fn calculate_avg_checkpoint_write_time(&self) -> f64 {
        let total = self.get_total_checkpoints();
        if total == 0 {
            0.0
        } else {
            self.checkpoint_write_time / total as f64
        }
    }

    /// Calculates average checkpoint sync time
    fn calculate_avg_checkpoint_sync_time(&self) -> f64 {
        let total = self.get_total_checkpoints();
        if total == 0 {
            0.0
        } else {
            self.checkpoint_sync_time / total as f64
        }
    }

    /// Calculates background writer efficiency
    fn calculate_bgwriter_efficiency(&self) -> f64 {
        let total_written = self.buffers_clean + self.buffers_backend;
        if total_written == 0 {
            100.0 // No writes means perfect efficiency
        } else {
            (self.buffers_clean as f64 / total_written as f64) * 100.0
        }
    }

    /// Calculates overall health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for too many requested checkpoints
        if self.requested_checkpoint_ratio > 20.0 {
            score -= (self.requested_checkpoint_ratio - 20.0).min(30.0);
        }

        // Deduct for low bgwriter efficiency
        if self.bgwriter_efficiency < 70.0 {
            score -= (70.0 - self.bgwriter_efficiency).min(25.0);
        }

        // Deduct for high backend fsync percentage
        let backend_fsync_pct = self.get_backend_fsync_percentage();
        if backend_fsync_pct > 10.0 {
            score -= (backend_fsync_pct - 10.0).min(20.0);
        }

        // Deduct for bgwriter throttling
        let throttle_pct = self.get_bgwriter_throttle_percentage();
        if throttle_pct > 30.0 {
            score -= (throttle_pct - 30.0).min(15.0);
        }

        // Deduct for slow checkpoint I/O
        if self.avg_checkpoint_write_time > 1000.0 || self.avg_checkpoint_sync_time > 5000.0 {
            score -= 20.0;
        }

        score.max(0.0)
    }

    /// Calculates the percentage of backend writes that required fsync
    pub fn get_backend_fsync_percentage(&self) -> f64 {
        if self.buffers_backend == 0 {
            0.0
        } else {
            (self.buffers_backend_fsync as f64 / self.buffers_backend as f64) * 100.0
        }
    }

    /// Calculates how often background writer hits maxwritten_clean limit
    pub fn get_bgwriter_throttle_percentage(&self) -> f64 {
        if self.buffers_clean == 0 {
            0.0
        } else {
            (self.maxwritten_clean as f64 / (self.buffers_clean as f64 + self.maxwritten_clean as f64)) * 100.0
        }
    }

    /// Calculates buffer write distribution
    pub fn get_buffer_write_distribution(&self) -> (f64, f64, f64) {
        let total = self.buffers_checkpoint + self.buffers_clean + self.buffers_backend;
        if total == 0 {
            return (0.0, 0.0, 0.0);
        }

        let checkpoint_pct = (self.buffers_checkpoint as f64 / total as f64) * 100.0;
        let bgwriter_pct = (self.buffers_clean as f64 / total as f64) * 100.0;
        let backend_pct = (self.buffers_backend as f64 / total as f64) * 100.0;

        (checkpoint_pct, bgwriter_pct, backend_pct)
    }

    /// Generates tuning recommendations based on current metrics
    fn generate_tuning_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.requested_checkpoint_ratio > 20.0 {
            recommendations.push("Consider increasing max_wal_size to reduce checkpoint frequency".to_string());
        }

        if self.bgwriter_efficiency < 70.0 {
            recommendations.push("Consider tuning bgwriter_lru_maxpages and bgwriter_lru_multiplier".to_string());
        }

        if self.get_backend_fsync_percentage() > 10.0 {
            recommendations.push("Increase bgwriter activity to reduce backend fsync load".to_string());
        }

        if self.get_bgwriter_throttle_percentage() > 30.0 {
            recommendations.push("Consider increasing bgwriter_lru_maxpages".to_string());
        }

        if self.avg_checkpoint_write_time > 1000.0 || self.avg_checkpoint_sync_time > 5000.0 {
            recommendations.push("Consider tuning checkpoint_completion_target or improving storage performance".to_string());
        }

        let (checkpoint_pct, bgwriter_pct, backend_pct) = self.get_buffer_write_distribution();
        if !(checkpoint_pct > 30.0 && checkpoint_pct < 70.0 && bgwriter_pct > 15.0 && backend_pct < 30.0) {
            recommendations.push("Review buffer write distribution - may need bgwriter tuning".to_string());
        }

        recommendations
    }

    /// Checks if there are excessive requested checkpoints
    pub fn has_excessive_requested_checkpoints(&self) -> bool {
        self.requested_checkpoint_ratio > 20.0
    }

    /// Checks if background writer is effective
    pub fn is_bgwriter_effective(&self) -> bool {
        self.bgwriter_efficiency >= 70.0
    }

    /// Checks if checkpoint I/O times are concerning
    pub fn has_slow_checkpoint_io(&self) -> bool {
        self.avg_checkpoint_write_time > 1000.0 || self.avg_checkpoint_sync_time > 5000.0
    }

    /// Checks if the system is checkpoint-bound
    pub fn is_checkpoint_bound(&self) -> bool {
        self.requested_checkpoint_ratio > 30.0 || self.avg_checkpoint_write_time > 2000.0 || self.avg_checkpoint_sync_time > 10000.0
    }

    /// Gets overall system write health summary
    pub fn get_write_system_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Write system is performing optimally".to_string(),
            75..=89 => "Good - Write system is performing well with minor issues".to_string(),
            60..=74 => "Fair - Write system has some performance issues that should be addressed".to_string(),
            40..=59 => "Poor - Write system has significant performance problems".to_string(),
            _ => "Critical - Write system requires immediate attention".to_string(),
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_types::metadata::{CapabilityChecker, CapabilityId};

    struct TestCapabilities {
        pg17: bool,
    }

    impl CapabilityChecker for TestCapabilities {
        fn has(&self, id: &CapabilityId) -> bool {
            id == &PG_VERSION_17 && self.pg17
        }
    }

    #[test]
    fn checkpointer_query_uses_pg17_view_when_available() {
        let query = PostgresBgWriterInfo::checkpointer_stats_query(&TestCapabilities { pg17: true });
        assert!(query.query().contains("pg_stat_checkpointer"));
    }

    #[test]
    fn checkpointer_query_falls_back_to_pg_stat_bgwriter_for_older_versions() {
        let query = PostgresBgWriterInfo::checkpointer_stats_query(&TestCapabilities { pg17: false });
        assert!(query.query().contains("pg_stat_bgwriter"));
        assert!(query.query().contains("buffers_backend"));
    }

    #[cfg(external_db)]
    #[tokio::test]
    async fn test_postgres_bgwriter_metadata() {
        use crate::test_utils::database_test_utils::connect_to_postgres;
        use endpoint_types::metadata::PermissiveCapabilities;
        use ep_core::GetPool;

        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let bgwriter_info = PostgresBgWriterInfo::default();

        let result = bgwriter_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok(), "sync_metadata failed: {:?}", result.as_ref().err());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
        assert!(info.bgwriter_efficiency >= 0.0);
        assert!(info.bgwriter_efficiency <= 100.0);
    }
}
