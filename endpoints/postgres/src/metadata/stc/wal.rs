use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::seconds_since;
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

/// PostgreSQL Write-Ahead Log (WAL) information and statistics
///
/// This struct contains comprehensive metrics about WAL generation, archiving,
/// and management. Critical for monitoring transaction log health, replication
/// readiness, and recovery capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresWalInfo {
    /// Current WAL LSN (Log Sequence Number) position
    pub current_wal_lsn: Option<String>,
    /// Current WAL insert LSN position
    pub current_wal_insert_lsn: Option<String>,
    /// Current WAL flush LSN position
    pub current_wal_flush_lsn: Option<String>,
    /// Rate of WAL generation (bytes per second)
    pub wal_generation_rate: f64,
    /// Number of WAL records generated
    pub wal_records: u64,
    /// Number of full page images in WAL
    pub wal_fpi: u64,
    /// Total bytes of WAL generated
    pub wal_bytes: u64,
    /// Number of WAL buffers that were full
    pub wal_buffers_full: u64,
    /// Number of WAL writes
    pub wal_write: u64,
    /// Number of WAL syncs
    pub wal_sync: u64,
    /// Time spent writing WAL (milliseconds)
    pub wal_write_time: f64,
    /// Time spent syncing WAL (milliseconds)
    pub wal_sync_time: f64,
    /// Number of WAL files currently on disk
    pub wal_file_count: u64,
    /// Total size of WAL files (bytes)
    pub total_wal_size_bytes: u64,
    /// Whether archiving is currently failing
    pub is_archiving_failing: bool,
    /// Archive success rate (percentage)
    pub archive_success_rate: f64,
    /// Current timeline ID
    pub timeline_id: i32,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresDetailedWalMetrics>,
}

/// Detailed WAL metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedWalMetrics {
    /// WAL archiving statistics
    pub archiving_stats: PostgresWalArchivingStats,
    /// Detailed WAL file information
    pub wal_files: PostgresWalFileInfo,
    /// WAL configuration settings
    pub wal_settings: PostgresWalSettings,
    /// Timeline information
    pub timeline_info: PostgresTimelineInfo,
    /// Checkpoint-related WAL statistics
    pub checkpoint_wal_stats: PostgresCheckpointWalStats,
}

impl MetadataCollection for PostgresWalInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "core_wal_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    pg_current_wal_lsn()::text as current_wal_lsn,
                    pg_current_wal_insert_lsn()::text as current_wal_insert_lsn,
                    pg_current_wal_flush_lsn()::text as current_wal_flush_lsn,
                    COALESCE(wal_records::bigint, 0::bigint) as wal_records,
                    COALESCE(wal_fpi::bigint, 0::bigint) as wal_fpi,
                    COALESCE(wal_bytes::bigint, 0::bigint) as wal_bytes,
                    COALESCE(wal_buffers_full::bigint, 0::bigint) as wal_buffers_full,
                    COALESCE(wal_write::bigint, 0::bigint) as wal_write,
                    COALESCE(wal_sync::bigint, 0::bigint) as wal_sync,
                    COALESCE(wal_write_time, 0) as wal_write_time,
                    COALESCE(wal_sync_time, 0) as wal_sync_time,
                    stats_reset
                FROM pg_stat_wal"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "basic_archiver_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    COALESCE(archived_count, 0) as archived_count,
                    COALESCE(failed_count, 0) as failed_count,
                    CASE WHEN archived_count + failed_count > 0 THEN
                        (archived_count::float / (archived_count + failed_count)::float) * 100
                    ELSE 100.0 END as success_rate,
                    CASE WHEN failed_count > 0 OR
                        (last_failed_time > last_archived_time AND last_failed_time IS NOT NULL)
                    THEN true ELSE false END as is_failing
                FROM pg_stat_archiver"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "wal_file_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as wal_file_count,
                    COALESCE(SUM(size), 0)::bigint as total_wal_size_bytes
                FROM pg_ls_waldir()
                WHERE name ~ '^[0-9A-F]{24}$'"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "timeline_id".to_string(),
                QueryInput::new("SELECT timeline_id FROM pg_control_checkpoint()".to_string(), Vec::new()),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL WAL information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "wal"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresWalInfo {
    const HIGH_WAL_FILE_THRESHOLD: u64 = 1000;
    const HIGH_FAILURE_RATE_THRESHOLD: f64 = 10.0; // 10% failure rate
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut wal_info = PostgresWalInfo::default();
        let requests = self.request();

        // Execute core WAL stats query
        if let Some(row) = run_single_row(&requests, "core_wal_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            wal_info.current_wal_lsn = Self::safe_get_optional_string(&row, "current_wal_lsn")?;
            wal_info.current_wal_insert_lsn = Self::safe_get_optional_string(&row, "current_wal_insert_lsn")?;
            wal_info.current_wal_flush_lsn = Self::safe_get_optional_string(&row, "current_wal_flush_lsn")?;
            wal_info.wal_records = Self::safe_i64_to_u64(&row, "wal_records")?;
            wal_info.wal_fpi = Self::safe_i64_to_u64(&row, "wal_fpi")?;
            wal_info.wal_bytes = Self::safe_i64_to_u64(&row, "wal_bytes")?;
            wal_info.wal_buffers_full = Self::safe_i64_to_u64(&row, "wal_buffers_full")?;
            wal_info.wal_write = Self::safe_i64_to_u64(&row, "wal_write")?;
            wal_info.wal_sync = Self::safe_i64_to_u64(&row, "wal_sync")?;
            wal_info.wal_write_time = Self::safe_get_f64(&row, "wal_write_time")?;
            wal_info.wal_sync_time = Self::safe_get_f64(&row, "wal_sync_time")?;

            let stats_reset = Self::safe_get_optional_datetime(&row, "stats_reset")?;
            wal_info.wal_generation_rate = seconds_since(stats_reset)
                .filter(|seconds| *seconds > 0.0)
                .map(|seconds| wal_info.wal_bytes as f64 / seconds)
                .unwrap_or(0.0);
        }

        // Get basic archiver statistics
        if let Some(row) = run_single_row(&requests, "basic_archiver_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            wal_info.archive_success_rate = Self::safe_get_f64(&row, "success_rate")?;
            wal_info.is_archiving_failing = Self::safe_get_bool(&row, "is_failing")?;
        }

        // Get WAL file summary
        if let Some(row) = run_single_row(&requests, "wal_file_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            wal_info.wal_file_count = Self::safe_i64_to_u64(&row, "wal_file_count")?;
            wal_info.total_wal_size_bytes = Self::safe_i64_to_u64(&row, "total_wal_size_bytes")?;
        }

        // Get timeline ID
        if let Some(row) = run_single_row(&requests, "timeline_id", context.clone(), Self::QUERY_TIMEOUT).await? {
            wal_info.timeline_id = Self::safe_get_i32(&row, "timeline_id")?;
        }

        // Conditionally collect detailed metrics only when problems are detected
        wal_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&wal_info, context).await?;

        Ok(wal_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresWalInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedWalMetrics>> {
        let needs_detailed_wal_info = core_info.is_archiving_failing
            || core_info.archive_success_rate < (100.0 - Self::HIGH_FAILURE_RATE_THRESHOLD)
            || core_info.wal_file_count > Self::HIGH_WAL_FILE_THRESHOLD
            || core_info.timeline_id > 1
            || Self::has_concerning_wal_performance(core_info);

        if !needs_detailed_wal_info {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresDetailedWalMetrics {
            archiving_stats: PostgresWalArchivingStats::default(),
            wal_files: PostgresWalFileInfo::default(),
            wal_settings: PostgresWalSettings::default(),
            timeline_info: PostgresTimelineInfo::default(),
            checkpoint_wal_stats: PostgresCheckpointWalStats::default(),
        };

        // Collect detailed archiving statistics
        if core_info.is_archiving_failing {
            let detailed_archiving_input = QueryInput::new(
                "SELECT
                    archived_count, last_archived_wal, last_archived_time,
                    failed_count, last_failed_wal, last_failed_time, stats_reset
                FROM pg_stat_archiver"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&detailed_archiving_input, context.clone(), Self::QUERY_TIMEOUT, "detailed_archiving").await
            {
                detailed_metrics.archiving_stats = Self::parse_archiving_stats(rows)?;
            }
        }

        // Collect detailed WAL file information
        if core_info.wal_file_count > Self::HIGH_WAL_FILE_THRESHOLD {
            let detailed_wal_files_input = QueryInput::new(
                format!(
                    "SELECT
                        name, size, modification as mtime
                    FROM pg_ls_waldir()
                    WHERE name ~ '^[0-9A-F]{{24}}$'
                    ORDER BY modification DESC
                    LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&detailed_wal_files_input, context.clone(), Self::QUERY_TIMEOUT, "detailed_wal_files").await
            {
                detailed_metrics.wal_files = Self::parse_wal_file_info(rows, core_info)?;
            }
        }

        // Collect WAL settings
        let wal_settings_input = QueryInput::new(
            "SELECT name, setting, unit, category, short_desc
            FROM pg_settings
            WHERE name IN (
                'wal_level', 'wal_buffers', 'wal_writer_delay', 'wal_writer_flush_after',
                'wal_compression', 'wal_log_hints', 'max_wal_size', 'min_wal_size',
                'archive_mode', 'archive_timeout', 'max_wal_senders'
            )
            ORDER BY name"
                .to_string(),
            Vec::new(),
        );

        if let Ok(rows) = run_query_with_timeout(&wal_settings_input, context.clone(), Self::QUERY_TIMEOUT, "wal_settings").await {
            detailed_metrics.wal_settings = Self::parse_wal_settings(rows)?;
        }

        // Collect timeline information if timeline switched
        if core_info.timeline_id > 1 {
            let timeline_info_input = QueryInput::new(
                "SELECT
                    timeline_id,
                    CASE WHEN pg_is_in_recovery() THEN
                        pg_last_wal_replay_lsn()::text
                    ELSE
                        pg_current_wal_lsn()::text
                    END as current_lsn_on_timeline
                FROM pg_control_checkpoint()"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&timeline_info_input, context.clone(), Self::QUERY_TIMEOUT, "timeline_info").await {
                detailed_metrics.timeline_info = Self::parse_timeline_info(rows)?;
            }
        }

        // Collect checkpoint WAL statistics
        let checkpoint_wal_input = QueryInput::new(
            "SELECT
                checkpoint_lsn::text as checkpoint_lsn,
                redo_lsn::text as redo_lsn,
                redo_wal_file,
                timeline_id as checkpoint_timeline_id,
                prev_timeline_id,
                full_page_writes,
                next_xid::text as next_xid,
                next_oid,
                oldest_xid,
                oldest_xid_db,
                oldest_active_xid,
                time as checkpoint_time
            FROM pg_control_checkpoint()"
                .to_string(),
            Vec::new(),
        );

        if let Ok(rows) = run_query_with_timeout(&checkpoint_wal_input, context.clone(), Self::QUERY_TIMEOUT, "checkpoint_wal").await {
            detailed_metrics.checkpoint_wal_stats = Self::parse_checkpoint_wal_stats(rows)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn has_concerning_wal_performance(core_info: &PostgresWalInfo) -> bool {
        // Check for concerning WAL performance indicators
        let avg_write_time = if core_info.wal_write > 0 {
            core_info.wal_write_time / core_info.wal_write as f64
        } else {
            0.0
        };

        let avg_sync_time = if core_info.wal_sync > 0 {
            core_info.wal_sync_time / core_info.wal_sync as f64
        } else {
            0.0
        };

        let buffer_full_ratio = if core_info.wal_write > 0 {
            (core_info.wal_buffers_full as f64 / core_info.wal_write as f64) * 100.0
        } else {
            0.0
        };

        avg_write_time > 10.0 || avg_sync_time > 50.0 || buffer_full_ratio > 90.0
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

    fn safe_get_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<DateTimeWrapper> {
        let text = row
            .get(column)
            .ok_or_else(|| EpError::metadata(format!("Failed to get datetime column {column}: column not found or NULL")))?;
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
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
                Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
            }
            None => Ok(None),
        }
    }

    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_bool(row: &PgSimpleRow, column: &str) -> ResultEP<bool> {
        row.get(column)
            .map(|s| s == "t" || s == "true" || s == "1")
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn parse_archiving_stats(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresWalArchivingStats> {
        if let Some(row) = rows.first() {
            let archived_count = Self::safe_i64_to_u64(row, "archived_count")?;
            let failed_count = Self::safe_i64_to_u64(row, "failed_count")?;

            let archive_success_rate = if archived_count + failed_count > 0 {
                (archived_count as f64 / (archived_count + failed_count) as f64) * 100.0
            } else {
                100.0
            };

            Ok(PostgresWalArchivingStats {
                archived_count,
                last_archived_wal: Self::safe_get_optional_string(row, "last_archived_wal")?,
                last_archived_time: Self::safe_get_optional_datetime(row, "last_archived_time")?,
                failed_count,
                last_failed_wal: Self::safe_get_optional_string(row, "last_failed_wal")?,
                last_failed_time: Self::safe_get_optional_datetime(row, "last_failed_time")?,
                stats_reset: Self::safe_get_optional_datetime(row, "stats_reset")?,
                archive_success_rate,
                is_archiving_failing: failed_count > 0,
            })
        } else {
            Ok(PostgresWalArchivingStats::default())
        }
    }

    fn parse_wal_file_info(rows: Vec<PgSimpleRow>, core_info: &PostgresWalInfo) -> ResultEP<PostgresWalFileInfo> {
        let mut oldest_time: Option<DateTimeWrapper> = None;
        let mut newest_time: Option<DateTimeWrapper> = None;
        let mut _total_size = 0u64;

        for row in &rows {
            let size = Self::safe_i64_to_u64(row, "size")?;
            _total_size += size;

            if let Ok(mtime) = Self::safe_get_datetime(row, "mtime") {
                if let Some(oldest) = &oldest_time {
                    if mtime.as_datetime() < oldest.as_datetime() {
                        oldest_time = Some(mtime.clone());
                    }
                } else {
                    oldest_time = Some(mtime.clone());
                }

                if let Some(newest) = &newest_time {
                    if mtime.as_datetime() > newest.as_datetime() {
                        newest_time = Some(mtime);
                    }
                } else {
                    newest_time = Some(mtime);
                }
            }
        }

        let avg_file_size = if core_info.wal_file_count > 0 {
            core_info.total_wal_size_bytes / core_info.wal_file_count
        } else {
            0
        };

        Ok(PostgresWalFileInfo {
            wal_file_count: core_info.wal_file_count,
            total_wal_size_bytes: core_info.total_wal_size_bytes,
            total_wal_size_pretty: format_bytes(core_info.total_wal_size_bytes),
            avg_wal_file_size: avg_file_size,
            oldest_wal_file_time: oldest_time,
            newest_wal_file_time: newest_time,
            ready_to_archive_count: 0, // Would need additional query
            archiving_count: 0,        // Would need additional query
        })
    }

    fn parse_wal_settings(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresWalSettings> {
        let mut settings = PostgresWalSettings::default();

        for row in rows {
            let name = Self::safe_get_string(&row, "name")?;
            let setting = Self::safe_get_string(&row, "setting")?;

            match name.as_str() {
                "wal_level" => settings.wal_level = setting,
                "wal_buffers" => settings.wal_buffers = setting,
                "wal_writer_delay" => {
                    settings.wal_writer_delay = setting.parse().unwrap_or(0);
                }
                "wal_writer_flush_after" => {
                    settings.wal_writer_flush_after = setting.parse().unwrap_or(0);
                }
                "wal_compression" => {
                    settings.wal_compression = setting == "on";
                }
                "wal_log_hints" => {
                    settings.wal_log_hints = setting == "on";
                }
                "max_wal_size" => settings.max_wal_size = setting,
                "min_wal_size" => settings.min_wal_size = setting,
                "archive_mode" => settings.archive_mode = setting,
                "archive_timeout" => {
                    settings.archive_timeout = setting.parse().unwrap_or(0);
                }
                "max_wal_senders" => {
                    settings.max_wal_senders = setting.parse().unwrap_or(0);
                }
                _ => {} // Ignore unknown settings
            }
        }

        Ok(settings)
    }

    fn parse_timeline_info(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresTimelineInfo> {
        if let Some(row) = rows.first() {
            Ok(PostgresTimelineInfo {
                timeline_id: Self::safe_get_i32(row, "timeline_id")?,
                current_lsn_on_timeline: Self::safe_get_optional_string(row, "current_lsn_on_timeline")?,
                previous_timeline_id: None,   // Would need additional query
                timeline_switch_lsn: None,    // Would need additional query
                timeline_history: Vec::new(), // Would need additional query
            })
        } else {
            Ok(PostgresTimelineInfo::default())
        }
    }

    fn parse_checkpoint_wal_stats(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresCheckpointWalStats> {
        if let Some(row) = rows.first() {
            Ok(PostgresCheckpointWalStats {
                checkpoint_lsn: Self::safe_get_optional_string(row, "checkpoint_lsn")?,
                redo_lsn: Self::safe_get_optional_string(row, "redo_lsn")?,
                redo_wal_file: Self::safe_get_optional_string(row, "redo_wal_file")?,
                checkpoint_timeline_id: Self::safe_get_i32(row, "checkpoint_timeline_id")?,
                prev_timeline_id: Self::safe_get_i32(row, "prev_timeline_id")?,
                full_page_writes: Self::safe_get_bool(row, "full_page_writes")?,
                next_xid: Self::safe_get_optional_string(row, "next_xid")?,
                next_oid: Self::safe_get_i32(row, "next_oid")?,
                next_multixact_id: 0, // Would need additional field
                next_multi_offset: 0, // Would need additional field
                oldest_xid: Self::safe_get_i32(row, "oldest_xid")?,
                oldest_xid_db: Self::safe_get_i32(row, "oldest_xid_db")?,
                oldest_active_xid: Self::safe_get_i32(row, "oldest_active_xid")?,
                oldest_multi_xid: 0, // Would need additional field
                oldest_multi_db: 0,  // Would need additional field
                checkpoint_time: Self::safe_get_optional_datetime(row, "checkpoint_time")?,
                oldest_commit_ts_xid: None, // Would need additional field
                newest_commit_ts_xid: None, // Would need additional field
            })
        } else {
            Ok(PostgresCheckpointWalStats::default())
        }
    }
}

// Helper function to format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

/// WAL archiving statistics and status
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresWalArchivingStats {
    /// Number of WAL files successfully archived
    pub archived_count: u64,
    /// Name of the last successfully archived WAL file
    pub last_archived_wal: Option<String>,
    /// Time of the last successful archive
    pub last_archived_time: Option<DateTimeWrapper>,
    /// Number of failed archive attempts
    pub failed_count: u64,
    /// Name of the last failed WAL file
    pub last_failed_wal: Option<String>,
    /// Time of the last failed archive attempt
    pub last_failed_time: Option<DateTimeWrapper>,
    /// Time when statistics were last reset
    pub stats_reset: Option<DateTimeWrapper>,
    /// Archive success rate (percentage)
    pub archive_success_rate: f64,
    /// Whether archiving is currently failing
    pub is_archiving_failing: bool,
}

/// WAL file information and disk usage
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresWalFileInfo {
    /// Number of WAL files currently on disk
    pub wal_file_count: u64,
    /// Total size of WAL files (bytes)
    pub total_wal_size_bytes: u64,
    /// Human-readable total WAL size
    pub total_wal_size_pretty: String,
    /// Average WAL file size (bytes)
    pub avg_wal_file_size: u64,
    /// Oldest WAL file timestamp
    pub oldest_wal_file_time: Option<DateTimeWrapper>,
    /// Newest WAL file timestamp
    pub newest_wal_file_time: Option<DateTimeWrapper>,
    /// Number of ready-to-archive WAL files
    pub ready_to_archive_count: u64,
    /// Number of currently archiving WAL files
    pub archiving_count: u64,
}

/// WAL configuration settings
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresWalSettings {
    /// WAL level setting (minimal, replica, logical)
    pub wal_level: String,
    /// Size of WAL buffers
    pub wal_buffers: String,
    /// WAL writer delay in milliseconds
    pub wal_writer_delay: i32,
    /// WAL writer flush after (pages)
    pub wal_writer_flush_after: i32,
    /// Whether WAL compression is enabled
    pub wal_compression: bool,
    /// Whether WAL log hints are enabled
    pub wal_log_hints: bool,
    /// Whether to zero new WAL files
    pub wal_init_zero: bool,
    /// Whether to recycle WAL files
    pub wal_recycle: bool,
    /// Maximum WAL size before checkpoint
    pub max_wal_size: String,
    /// Minimum WAL size to maintain
    pub min_wal_size: String,
    /// WAL size to keep for replication
    pub wal_keep_size: String,
    /// WAL sender timeout in milliseconds
    pub wal_sender_timeout: i32,
    /// Archive mode setting
    pub archive_mode: String,
    /// Archive command
    pub archive_command: String,
    /// Archive timeout in seconds
    pub archive_timeout: i32,
    /// Maximum number of WAL sender processes
    pub max_wal_senders: i32,
}

/// Timeline information for WAL
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTimelineInfo {
    /// Current timeline ID
    pub timeline_id: i32,
    /// Current LSN position on this timeline
    pub current_lsn_on_timeline: Option<String>,
    /// Previous timeline ID (if switched)
    pub previous_timeline_id: Option<i32>,
    /// LSN where timeline switch occurred
    pub timeline_switch_lsn: Option<String>,
    /// History of timeline switches
    pub timeline_history: Vec<PostgresTimelineSwitch>,
}

/// Information about a timeline switch
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTimelineSwitch {
    /// Timeline ID switched from
    pub from_timeline: i32,
    /// Timeline ID switched to
    pub to_timeline: i32,
    /// LSN where switch occurred
    pub switch_lsn: String,
    /// Time when switch occurred
    pub switch_time: DateTimeWrapper,
    /// Reason for timeline switch
    pub switch_reason: String,
}

/// Checkpoint-related WAL statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresCheckpointWalStats {
    /// LSN of the last checkpoint
    pub checkpoint_lsn: Option<String>,
    /// LSN where redo should start from
    pub redo_lsn: Option<String>,
    /// WAL file containing the redo point
    pub redo_wal_file: Option<String>,
    /// Timeline ID of the checkpoint
    pub checkpoint_timeline_id: i32,
    /// Previous timeline ID
    pub prev_timeline_id: i32,
    /// Whether full page writes were enabled
    pub full_page_writes: bool,
    /// Next transaction ID
    pub next_xid: Option<String>,
    /// Next object ID
    pub next_oid: i32,
    /// Next multixact ID
    pub next_multixact_id: i32,
    /// Next multixact offset
    pub next_multi_offset: i32,
    /// Oldest active transaction ID
    pub oldest_xid: i32,
    /// Database with oldest transaction
    pub oldest_xid_db: i32,
    /// Oldest active transaction ID
    pub oldest_active_xid: i32,
    /// Oldest multixact ID
    pub oldest_multi_xid: i32,
    /// Database with oldest multixact
    pub oldest_multi_db: i32,
    /// Time of the checkpoint
    pub checkpoint_time: Option<DateTimeWrapper>,
    /// Oldest commit timestamp transaction ID
    pub oldest_commit_ts_xid: Option<i32>,
    /// Newest commit timestamp transaction ID
    pub newest_commit_ts_xid: Option<i32>,
}

impl PostgresWalInfo {
    /// Calculates WAL generation rate in bytes per second
    ///
    /// # Arguments
    /// * `time_period_seconds` - Time period over which to calculate rate
    ///
    /// # Returns
    /// * WAL generation rate in bytes per second
    pub fn calculate_wal_generation_rate(&self, time_period_seconds: f64) -> f64 {
        if time_period_seconds <= 0.0 {
            0.0
        } else {
            self.wal_bytes as f64 / time_period_seconds
        }
    }

    /// Checks if WAL archiving is failing
    ///
    /// # Returns
    /// * True if archiving appears to be failing
    pub fn is_archiving_failing(&self) -> bool {
        self.is_archiving_failing
    }

    /// Gets archive success rate as percentage
    ///
    /// # Returns
    /// * Success rate from 0.0 to 100.0
    pub fn get_archive_success_rate(&self) -> f64 {
        self.archive_success_rate
    }

    /// Checks if WAL files are accumulating excessively
    ///
    /// # Arguments
    /// * `threshold_count` - Maximum acceptable WAL file count
    ///
    /// # Returns
    /// * True if WAL file count exceeds threshold
    pub fn has_excessive_wal_files(&self, threshold_count: u64) -> bool {
        self.wal_file_count > threshold_count
    }

    /// Checks if WAL disk usage is concerning
    ///
    /// # Arguments
    /// * `threshold_bytes` - Maximum acceptable WAL disk usage
    ///
    /// # Returns
    /// * True if WAL disk usage exceeds threshold
    pub fn has_excessive_wal_disk_usage(&self, threshold_bytes: u64) -> bool {
        self.total_wal_size_bytes > threshold_bytes
    }

    /// Gets time since last successful archive
    ///
    /// # Returns
    /// * Duration since last archive, or None if never archived
    pub fn time_since_last_archive(&self) -> Option<chrono::Duration> {
        if let Some(detailed) = &self.detailed_metrics {
            detailed.archiving_stats.last_archived_time.as_ref().map(|last_time| chrono::Utc::now() - last_time.as_datetime())
        } else {
            None
        }
    }

    /// Checks if archiving is stale
    ///
    /// # Arguments
    /// * `threshold_seconds` - Maximum acceptable time since last archive
    ///
    /// # Returns
    /// * True if too much time has passed since last successful archive
    pub fn is_archiving_stale(&self, threshold_seconds: f64) -> bool {
        self.time_since_last_archive().map(|duration| duration.num_seconds() as f64 > threshold_seconds).unwrap_or(true) // No archive time means stale
    }

    /// Calculates average WAL write performance
    ///
    /// # Returns
    /// * Average milliseconds per WAL write
    pub fn get_avg_wal_write_time(&self) -> f64 {
        if self.wal_write == 0 {
            0.0
        } else {
            self.wal_write_time / self.wal_write as f64
        }
    }

    /// Calculates average WAL sync performance
    ///
    /// # Returns
    /// * Average milliseconds per WAL sync
    pub fn get_avg_wal_sync_time(&self) -> f64 {
        if self.wal_sync == 0 {
            0.0
        } else {
            self.wal_sync_time / self.wal_sync as f64
        }
    }

    /// Checks if WAL I/O performance is concerning
    ///
    /// # Arguments
    /// * `write_threshold_ms` - Maximum acceptable average write time
    /// * `sync_threshold_ms` - Maximum acceptable average sync time
    ///
    /// # Returns
    /// * True if WAL I/O times exceed thresholds
    pub fn has_slow_wal_io(&self, write_threshold_ms: f64, sync_threshold_ms: f64) -> bool {
        self.get_avg_wal_write_time() > write_threshold_ms || self.get_avg_wal_sync_time() > sync_threshold_ms
    }

    /// Gets WAL buffer utilization
    ///
    /// # Returns
    /// * Percentage of time WAL buffers were full (0.0 to 100.0)
    pub fn get_wal_buffer_utilization(&self) -> f64 {
        if self.wal_write == 0 {
            0.0
        } else {
            (self.wal_buffers_full as f64 / self.wal_write as f64) * 100.0
        }
    }

    /// Checks if WAL buffers are frequently full
    ///
    /// # Arguments
    /// * `threshold_percentage` - Maximum acceptable buffer full percentage
    ///
    /// # Returns
    /// * True if buffers are full too often
    pub fn has_frequent_full_wal_buffers(&self, threshold_percentage: f64) -> bool {
        self.get_wal_buffer_utilization() > threshold_percentage
    }

    /// Checks if timeline has switched recently
    ///
    /// # Returns
    /// * True if timeline ID suggests recent timeline switch
    pub fn has_recent_timeline_switch(&self) -> bool {
        self.timeline_id > 1
    }

    /// Gets distance between checkpoint and current position
    ///
    /// # Returns
    /// * Estimated bytes between checkpoint and current LSN
    pub fn get_checkpoint_distance_estimate(&self) -> Option<u64> {
        // This would require LSN arithmetic which is complex
        // In a real implementation, you'd use pg_wal_lsn_diff()
        None
    }

    /// Checks if checkpoint distance is excessive
    ///
    /// # Arguments
    /// * `threshold_bytes` - Maximum acceptable distance from checkpoint
    ///
    /// # Returns
    /// * True if too far from last checkpoint
    pub fn is_checkpoint_distance_excessive(&self, threshold_bytes: u64) -> bool {
        self.get_checkpoint_distance_estimate().map(|distance| distance > threshold_bytes).unwrap_or(false)
    }

    /// Gets WAL efficiency metrics
    ///
    /// # Returns
    /// * Tuple of (fpi_ratio, records_per_byte, writes_per_sync)
    pub fn get_wal_efficiency_metrics(&self) -> (f64, f64, f64) {
        let fpi_ratio = if self.wal_records == 0 {
            0.0
        } else {
            self.wal_fpi as f64 / self.wal_records as f64
        };

        let records_per_byte = if self.wal_bytes == 0 {
            0.0
        } else {
            self.wal_records as f64 / self.wal_bytes as f64
        };

        let writes_per_sync = if self.wal_sync == 0 {
            0.0
        } else {
            self.wal_write as f64 / self.wal_sync as f64
        };

        (fpi_ratio, records_per_byte, writes_per_sync)
    }

    /// Checks if WAL settings are appropriate for workload
    ///
    /// # Returns
    /// * Vector of setting recommendations or warnings
    pub fn get_wal_setting_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if let Some(detailed) = &self.detailed_metrics {
            if detailed.wal_settings.wal_level == "minimal" && self.wal_file_count > 0 {
                recommendations.push("Consider increasing wal_level for better monitoring".to_string());
            }

            if self.has_frequent_full_wal_buffers(80.0) {
                recommendations.push("Consider increasing wal_buffers".to_string());
            }

            if detailed.wal_settings.archive_mode == "off" && self.wal_file_count > 100 {
                recommendations.push("Consider enabling archiving to manage WAL file growth".to_string());
            }
        }

        if self.is_archiving_failing() {
            recommendations.push("Check archive_command configuration".to_string());
        }

        if self.has_excessive_wal_files(1000) {
            recommendations.push("WAL file count is high - check archiving and replication".to_string());
        }

        if self.has_slow_wal_io(10.0, 50.0) {
            recommendations.push("WAL I/O performance is slow - check disk subsystem".to_string());
        }

        recommendations
    }

    /// Checks overall WAL health
    ///
    /// # Returns
    /// * True if WAL subsystem appears healthy
    pub fn is_wal_healthy(&self) -> bool {
        !self.is_archiving_failing()
            && !self.has_excessive_wal_files(1000)
            && !self.has_slow_wal_io(10.0, 50.0)
            && !self.has_frequent_full_wal_buffers(90.0)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets WAL health score
    ///
    /// # Returns
    /// * Health score from 0-100 (higher is better)
    pub fn get_wal_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for archiving failures
        if self.is_archiving_failing() {
            score -= 40.0;
        } else if self.archive_success_rate < 95.0 {
            score -= (100.0 - self.archive_success_rate) * 0.5;
        }

        // Deduct for excessive WAL files
        if self.has_excessive_wal_files(2000) {
            score -= 30.0;
        } else if self.has_excessive_wal_files(1000) {
            score -= 15.0;
        }

        // Deduct for slow WAL I/O
        if self.has_slow_wal_io(20.0, 100.0) {
            score -= 25.0;
        } else if self.has_slow_wal_io(10.0, 50.0) {
            score -= 10.0;
        }

        // Deduct for frequent full buffers
        if self.has_frequent_full_wal_buffers(95.0) {
            score -= 20.0;
        } else if self.has_frequent_full_wal_buffers(80.0) {
            score -= 10.0;
        }

        // Deduct for timeline switches
        if self.has_recent_timeline_switch() {
            score -= 5.0;
        }

        score.max(0.0)
    }

    /// Gets overall WAL system status
    ///
    /// # Returns
    /// * String describing overall WAL system health
    pub fn get_wal_system_status(&self) -> String {
        let health_score = self.get_wal_health_score();

        if health_score >= 90.0 {
            "Excellent - WAL system is operating optimally".to_string()
        } else if health_score >= 75.0 {
            "Good - WAL system is performing well with minor issues".to_string()
        } else if health_score >= 60.0 {
            "Fair - WAL system has some performance issues".to_string()
        } else if health_score >= 40.0 {
            "Poor - WAL system has significant problems".to_string()
        } else {
            "Critical - WAL system requires immediate attention".to_string()
        }
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_wal_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let wal_info = PostgresWalInfo::default();

        let result = wal_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.current_wal_lsn.is_some());
        assert!(info.archive_success_rate >= 0.0);
        assert!(info.archive_success_rate <= 100.0);
    }

    #[test]
    fn test_wal_generation_rate_calculation() {
        let wal_info = PostgresWalInfo { wal_bytes: 1000, ..Default::default() };

        assert_eq!(wal_info.calculate_wal_generation_rate(10.0), 100.0);
        assert_eq!(wal_info.calculate_wal_generation_rate(0.0), 0.0);
    }

    #[test]
    fn test_wal_buffer_utilization() {
        let mut wal_info = PostgresWalInfo { wal_write: 100, wal_buffers_full: 20, ..Default::default() };

        assert_eq!(wal_info.get_wal_buffer_utilization(), 20.0);

        wal_info.wal_write = 0;
        assert_eq!(wal_info.get_wal_buffer_utilization(), 0.0);
    }

    #[test]
    fn test_wal_io_performance() {
        let wal_info = PostgresWalInfo {
            wal_write: 10,
            wal_write_time: 100.0,
            wal_sync: 5,
            wal_sync_time: 250.0,
            ..Default::default()
        };

        assert_eq!(wal_info.get_avg_wal_write_time(), 10.0);
        assert_eq!(wal_info.get_avg_wal_sync_time(), 50.0);

        assert!(wal_info.has_slow_wal_io(5.0, 25.0));
        assert!(!wal_info.has_slow_wal_io(15.0, 75.0));
    }

    #[test]
    fn test_wal_health_scoring() {
        let mut wal_info = PostgresWalInfo {
            // Perfect health
            archive_success_rate: 100.0,
            is_archiving_failing: false,
            wal_file_count: 50,
            ..Default::default()
        };

        assert!(wal_info.get_wal_health_score() >= 90.0);

        // Some issues
        wal_info.is_archiving_failing = true;
        assert!(wal_info.get_wal_health_score() < 70.0);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1536), "1.50 KB");
    }

    #[test]
    fn test_wal_efficiency_metrics() {
        let wal_info = PostgresWalInfo {
            wal_records: 1000,
            wal_fpi: 100,
            wal_bytes: 50000,
            wal_write: 50,
            wal_sync: 10,
            ..Default::default()
        };

        let (fpi_ratio, records_per_byte, writes_per_sync) = wal_info.get_wal_efficiency_metrics();

        assert_eq!(fpi_ratio, 0.1);
        assert_eq!(records_per_byte, 0.02);
        assert_eq!(writes_per_sync, 5.0);
    }
}
