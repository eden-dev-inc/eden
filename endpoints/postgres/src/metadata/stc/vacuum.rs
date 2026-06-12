use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{get_first_string, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL vacuum and autovacuum information
///
/// This struct contains comprehensive metrics about vacuum operations,
/// autovacuum configuration, and table maintenance status. Critical for
/// monitoring database maintenance and preventing bloat and performance issues.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresVacuumInfo {
    /// Whether autovacuum is enabled globally
    pub autovacuum_enabled: bool,
    /// Maximum number of autovacuum worker processes
    pub autovacuum_max_workers: i32,
    /// Autovacuum naptime (seconds between runs)
    pub autovacuum_naptime: i32,
    /// Current number of active autovacuum processes
    pub active_autovacuum_workers: u64,
    /// Global vacuum statistics
    pub vacuum_stats: PostgresVacuumStats,
    /// Overall dead tuple percentage
    pub overall_dead_tuple_percentage: f64,
    /// Number of tables that need vacuum attention
    pub tables_needing_vacuum_count: u64,
    /// Number of tables with concerning bloat levels
    pub tables_with_high_bloat_count: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresDetailedVacuumMetrics>,
}

/// Detailed vacuum metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedVacuumMetrics {
    /// Tables currently being vacuumed
    pub tables_being_vacuumed: Vec<PostgresActiveVacuum>,
    /// Tables that need vacuum attention
    pub tables_needing_vacuum: Vec<PostgresTableVacuumStatus>,
    /// Recent vacuum activity
    pub recent_vacuum_activity: Vec<PostgresVacuumActivity>,
    /// Autovacuum configuration per table
    pub table_autovacuum_settings: Vec<PostgresTableAutovacuumSettings>,
    /// Bloat analysis results
    pub bloat_analysis: PostgresBloatAnalysis,
    /// Dead tuple statistics
    pub dead_tuple_stats: PostgresDeadTupleStats,
}

impl MetadataCollection for PostgresVacuumInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "core_vacuum_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    current_setting('autovacuum')::boolean as autovacuum_enabled,
                    current_setting('autovacuum_max_workers')::int as max_workers,
                    EXTRACT(EPOCH FROM current_setting('autovacuum_naptime')::interval)::int as naptime,
                    COUNT(*) FILTER (WHERE query ILIKE '%autovacuum%' AND state = 'active') as active_autovacuum_workers,
                    COUNT(*) FILTER (WHERE query ILIKE '%VACUUM%' AND state = 'active') as total_vacuum_workers
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "vacuum_summary_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as total_tables,
                    COUNT(*) FILTER (WHERE last_vacuum IS NULL AND last_autovacuum IS NULL) as never_vacuumed,
                    COUNT(*) FILTER (WHERE n_dead_tup > (n_live_tup * 0.2) AND n_live_tup + n_dead_tup > 1000) as high_dead_tuple_tables,
                    COALESCE(SUM(n_dead_tup), 0)::bigint as total_dead_tuples,
                    COALESCE(SUM(n_live_tup), 0)::bigint as total_live_tuples,
                    COALESCE(AVG(CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                        (n_dead_tup::float / (n_live_tup + n_dead_tup)::float) * 100
                    ELSE 0 END), 0)::float8 as avg_dead_tuple_percentage
                FROM pg_stat_user_tables"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL vacuum and autovacuum information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "vacuum"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresVacuumInfo {
    const HIGH_DEAD_TUPLE_THRESHOLD: f64 = 20.0; // 20% dead tuples
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

        let mut vacuum_info = PostgresVacuumInfo::default();
        let requests = self.request();

        // Execute core vacuum stats query
        if let Some(row) = run_single_row(&requests, "core_vacuum_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            vacuum_info.autovacuum_enabled = Self::safe_get_bool(&row, "autovacuum_enabled")?;
            vacuum_info.autovacuum_max_workers = Self::safe_get_i32(&row, "max_workers")?;
            vacuum_info.autovacuum_naptime = Self::safe_get_i32(&row, "naptime")?;
            vacuum_info.active_autovacuum_workers = Self::safe_i64_to_u64(&row, "active_autovacuum_workers")?;
        }

        // Get summary vacuum statistics
        if let Some(row) = run_single_row(&requests, "vacuum_summary_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            vacuum_info.vacuum_stats.total_tables = Self::safe_i64_to_u64(&row, "total_tables")?;
            vacuum_info.vacuum_stats.never_vacuumed = Self::safe_i64_to_u64(&row, "never_vacuumed")?;
            vacuum_info.tables_needing_vacuum_count = Self::safe_i64_to_u64(&row, "high_dead_tuple_tables")?;
            vacuum_info.vacuum_stats.total_dead_tuples = Self::safe_i64_to_u64(&row, "total_dead_tuples")?;
            vacuum_info.vacuum_stats.total_live_tuples = Self::safe_i64_to_u64(&row, "total_live_tuples")?;
            vacuum_info.overall_dead_tuple_percentage = Self::safe_get_f64(&row, "avg_dead_tuple_percentage")?;

            // Calculate derived metrics
            vacuum_info.vacuum_stats.overall_dead_tuple_percentage = vacuum_info.overall_dead_tuple_percentage;
            vacuum_info.tables_with_high_bloat_count = vacuum_info.tables_needing_vacuum_count;
        }

        // Conditionally collect detailed metrics only when problems are detected
        vacuum_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&vacuum_info, context).await?;

        Ok(vacuum_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresVacuumInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedVacuumMetrics>> {
        if core_info.vacuum_stats.total_tables == 0 {
            // No user tables to analyze; return early with no detailed metrics.
            return Ok(None);
        }

        let needs_detailed_vacuum_info = core_info.overall_dead_tuple_percentage > Self::HIGH_DEAD_TUPLE_THRESHOLD
            || core_info.vacuum_stats.never_vacuumed > 0
            || core_info.tables_needing_vacuum_count > 0
            || core_info.active_autovacuum_workers > 0;

        if !needs_detailed_vacuum_info {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresDetailedVacuumMetrics {
            tables_being_vacuumed: Vec::new(),
            tables_needing_vacuum: Vec::new(),
            recent_vacuum_activity: Vec::new(),
            table_autovacuum_settings: Vec::new(),
            bloat_analysis: PostgresBloatAnalysis::default(),
            dead_tuple_stats: PostgresDeadTupleStats::default(),
        };

        // Collect active vacuum operations
        if core_info.active_autovacuum_workers > 0 {
            let active_vacuum_input = QueryInput::new(
                format!(
                    "SELECT
                        pid, COALESCE(datname, 'unknown') as datname,
                        COALESCE(usename, 'unknown') as usename,
                        COALESCE(application_name, 'unknown') as application_name,
                        LEFT(query, 500) as query,
                        query_start,
                        EXTRACT(EPOCH FROM (now() - query_start)) as duration_seconds
                    FROM pg_stat_activity
                    WHERE (query ILIKE '%VACUUM%' OR query ILIKE '%autovacuum%')
                        AND state = 'active'
                        AND pid != pg_backend_pid()
                    ORDER BY query_start ASC
                    LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&active_vacuum_input, context.clone(), Self::QUERY_TIMEOUT, "active_vacuum").await {
                detailed_metrics.tables_being_vacuumed = Self::parse_active_vacuum_operations(rows)?;
            }
        }

        // Collect table vacuum status for problematic tables
        if core_info.tables_needing_vacuum_count > 0 {
            let table_vacuum_stats_input = QueryInput::new(
                format!(
                    "SELECT
                        schemaname, relname, last_vacuum, last_autovacuum,
                        last_analyze, last_autoanalyze, vacuum_count, autovacuum_count,
                        analyze_count, autoanalyze_count, n_tup_ins, n_tup_upd, n_tup_del,
                        n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze,
                        CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                            (n_dead_tup::float / (n_live_tup + n_dead_tup)::float) * 100
                        ELSE 0 END as dead_tuple_ratio
                    FROM pg_stat_user_tables
                    WHERE n_dead_tup > (n_live_tup * 0.1) AND n_live_tup + n_dead_tup > 100
                    ORDER BY dead_tuple_ratio DESC, n_dead_tup DESC
                    LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&table_vacuum_stats_input, context.clone(), Self::QUERY_TIMEOUT, "table_vacuum_stats").await
            {
                detailed_metrics.tables_needing_vacuum = Self::parse_table_vacuum_status(rows)?;
            }
        }

        // Collect autovacuum settings for tables with custom configuration
        let autovacuum_settings_input = QueryInput::new(
            format!(
                "SELECT
                    schemaname, tablename,
                    COALESCE((
                        SELECT option_value::boolean
                        FROM pg_options_to_table(reloptions)
                        WHERE option_name = 'autovacuum_enabled'
                    ), current_setting('autovacuum')::boolean) as autovacuum_enabled,
                    COALESCE((
                        SELECT option_value::int
                        FROM pg_options_to_table(reloptions)
                        WHERE option_name = 'autovacuum_vacuum_threshold'
                    ), current_setting('autovacuum_vacuum_threshold')::int) as vacuum_threshold,
                    COALESCE((
                        SELECT option_value::float
                        FROM pg_options_to_table(reloptions)
                        WHERE option_name = 'autovacuum_vacuum_scale_factor'
                    ), current_setting('autovacuum_vacuum_scale_factor')::float) as vacuum_scale_factor,
                    CASE WHEN reloptions IS NOT NULL THEN true ELSE false END as has_custom_settings
                FROM pg_tables t
                JOIN pg_class c ON c.relname = t.tablename
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
                WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
                    AND (reloptions IS NOT NULL OR schemaname IN (
                        SELECT DISTINCT schemaname FROM pg_stat_user_tables
                        WHERE n_dead_tup > (n_live_tup * 0.1) AND n_live_tup + n_dead_tup > 100
                    ))
                ORDER BY has_custom_settings DESC, t.schemaname, t.tablename
                LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );

        if let Ok(rows) =
            run_query_with_timeout(&autovacuum_settings_input, context.clone(), Self::QUERY_TIMEOUT, "autovacuum_settings").await
        {
            detailed_metrics.table_autovacuum_settings = Self::parse_autovacuum_settings(rows)?;
        }

        // Collect bloat analysis for high-bloat tables
        let bloat_analysis_input = QueryInput::new(
            format!(
                "SELECT
                    schemaname, relname,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size_pretty,
                    pg_total_relation_size(schemaname||'.'||relname) as total_size_bytes,
                    n_live_tup, n_dead_tup,
                    CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                        (n_dead_tup::float / (n_live_tup + n_dead_tup)::float) * 100
                    ELSE 0 END as estimated_bloat_ratio
                FROM pg_stat_user_tables
                WHERE n_live_tup + n_dead_tup > 1000
                    AND n_dead_tup > (n_live_tup * 0.1)
                ORDER BY estimated_bloat_ratio DESC, total_size_bytes DESC
                LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );

        if let Ok(rows) = run_query_with_timeout(&bloat_analysis_input, context.clone(), Self::QUERY_TIMEOUT, "bloat_analysis").await {
            detailed_metrics.bloat_analysis = Self::parse_bloat_analysis(rows)?;
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

    fn parse_active_vacuum_operations(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresActiveVacuum>> {
        let mut operations = Vec::with_capacity(rows.len());

        for row in rows {
            let query = Self::safe_get_string(&row, "query")?;
            operations.push(PostgresActiveVacuum {
                pid: Self::safe_get_i32(&row, "pid")?,
                database_name: Self::safe_get_string(&row, "datname")?,
                username: Self::safe_get_string(&row, "usename")?,
                application_name: Self::safe_get_optional_string(&row, "application_name")?,
                query: query.clone(),
                start_time: Self::safe_get_datetime(&row, "query_start")?,
                duration_seconds: Self::safe_get_f64(&row, "duration_seconds")?,
                vacuum_type: PostgresVacuumType::from_query(&query),
            });
        }

        Ok(operations)
    }

    fn parse_table_vacuum_status(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableVacuumStatus>> {
        let mut tables = Vec::with_capacity(rows.len());

        for row in rows {
            let dead_tuple_ratio = Self::safe_get_f64(&row, "dead_tuple_ratio")?;
            let dead_tuples = Self::safe_i64_to_u64(&row, "n_dead_tup")?;
            let live_tuples = Self::safe_i64_to_u64(&row, "n_live_tup")?;
            let modified_since_analyze = Self::safe_i64_to_u64(&row, "n_mod_since_analyze")?;

            // Determine maintenance priority
            let maintenance_priority = if dead_tuple_ratio > 50.0 {
                PostgresMaintenancePriority::Critical
            } else if dead_tuple_ratio > 30.0 {
                PostgresMaintenancePriority::High
            } else if dead_tuple_ratio > 15.0 {
                PostgresMaintenancePriority::Medium
            } else {
                PostgresMaintenancePriority::Low
            };

            tables.push(PostgresTableVacuumStatus {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                last_vacuum: Self::safe_get_optional_datetime(&row, "last_vacuum")?,
                last_autovacuum: Self::safe_get_optional_datetime(&row, "last_autovacuum")?,
                last_analyze: Self::safe_get_optional_datetime(&row, "last_analyze")?,
                last_autoanalyze: Self::safe_get_optional_datetime(&row, "last_autoanalyze")?,
                vacuum_count: Self::safe_i64_to_u64(&row, "vacuum_count")?,
                autovacuum_count: Self::safe_i64_to_u64(&row, "autovacuum_count")?,
                analyze_count: Self::safe_i64_to_u64(&row, "analyze_count")?,
                autoanalyze_count: Self::safe_i64_to_u64(&row, "autoanalyze_count")?,
                live_tuples,
                dead_tuples,
                modified_since_analyze,
                dead_tuple_ratio,
                needs_vacuum: dead_tuple_ratio > 10.0,
                needs_analyze: modified_since_analyze > (live_tuples / 10),
                maintenance_priority,
            });
        }

        Ok(tables)
    }

    fn parse_autovacuum_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableAutovacuumSettings>> {
        let mut settings = Vec::with_capacity(rows.len());

        for row in rows {
            settings.push(PostgresTableAutovacuumSettings {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                autovacuum_enabled: Self::safe_get_bool(&row, "autovacuum_enabled")?,
                vacuum_threshold: Self::safe_get_i32(&row, "vacuum_threshold")?,
                vacuum_scale_factor: Self::safe_get_f64(&row, "vacuum_scale_factor")?,
                analyze_threshold: 0, // Would need separate query for analyze settings
                analyze_scale_factor: 0.0,
                vacuum_cost_delay: None,
                vacuum_cost_limit: None,
                has_custom_settings: Self::safe_get_bool(&row, "has_custom_settings")?,
            });
        }

        Ok(settings)
    }

    fn parse_bloat_analysis(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresBloatAnalysis> {
        let mut high_bloat_tables = Vec::with_capacity(rows.len());
        let mut total_bloat_bytes = 0u64;
        let mut bloat_percentages = Vec::new();
        let mut high_bloat_count = 0u64;

        for row in rows {
            let bloat_percentage = Self::safe_get_f64(&row, "estimated_bloat_ratio")?;
            let total_size_bytes = Self::safe_i64_to_u64(&row, "total_size_bytes")?;
            let bloat_size_bytes = (total_size_bytes as f64 * (bloat_percentage / 100.0)) as u64;

            bloat_percentages.push(bloat_percentage);
            total_bloat_bytes += bloat_size_bytes;

            if bloat_percentage > 25.0 {
                high_bloat_count += 1;
            }

            let recommended_action = if bloat_percentage > 50.0 {
                "VACUUM FULL recommended".to_string()
            } else if bloat_percentage > 25.0 {
                "VACUUM recommended".to_string()
            } else {
                "Monitor".to_string()
            };

            high_bloat_tables.push(PostgresTableBloatInfo {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                total_size_bytes,
                total_size_pretty: Self::safe_get_string(&row, "total_size_pretty")?,
                bloat_percentage,
                bloat_size_bytes,
                live_tuples: Self::safe_i64_to_u64(&row, "n_live_tup")?,
                dead_tuples: Self::safe_i64_to_u64(&row, "n_dead_tup")?,
                recommended_action,
            });
        }

        let average_bloat_percentage = if !bloat_percentages.is_empty() {
            bloat_percentages.iter().sum::<f64>() / bloat_percentages.len() as f64
        } else {
            0.0
        };

        Ok(PostgresBloatAnalysis {
            high_bloat_tables,
            total_estimated_bloat_bytes: total_bloat_bytes,
            average_bloat_percentage,
            tables_with_high_bloat: high_bloat_count,
            is_bloat_concerning: average_bloat_percentage > 25.0 || high_bloat_count > 0,
        })
    }
}

/// Information about an active vacuum operation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresActiveVacuum {
    /// Process ID of the vacuum operation
    pub pid: i32,
    /// Database name
    pub database_name: String,
    /// Username running the vacuum
    pub username: String,
    /// Application name
    pub application_name: Option<String>,
    /// Vacuum query text (truncated)
    pub query: String,
    /// When the vacuum started
    pub start_time: DateTimeWrapper,
    /// Duration the vacuum has been running (seconds)
    pub duration_seconds: f64,
    /// Type of vacuum operation
    pub vacuum_type: PostgresVacuumType,
}

/// Type of vacuum operation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresVacuumType {
    /// Regular VACUUM
    Vacuum,
    /// VACUUM FULL
    VacuumFull,
    /// Autovacuum
    Autovacuum,
    /// VACUUM ANALYZE
    VacuumAnalyze,
    /// ANALYZE only
    Analyze,
    /// Unknown/other
    Unknown,
}

/// Vacuum status for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableVacuumStatus {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Last manual vacuum time
    pub last_vacuum: Option<DateTimeWrapper>,
    /// Last autovacuum time
    pub last_autovacuum: Option<DateTimeWrapper>,
    /// Last manual analyze time
    pub last_analyze: Option<DateTimeWrapper>,
    /// Last autoanalyze time
    pub last_autoanalyze: Option<DateTimeWrapper>,
    /// Number of manual vacuums
    pub vacuum_count: u64,
    /// Number of autovacuums
    pub autovacuum_count: u64,
    /// Number of manual analyzes
    pub analyze_count: u64,
    /// Number of autoanalyzes
    pub autoanalyze_count: u64,
    /// Live tuples count
    pub live_tuples: u64,
    /// Dead tuples count
    pub dead_tuples: u64,
    /// Tuples modified since last analyze
    pub modified_since_analyze: u64,
    /// Percentage of dead tuples
    pub dead_tuple_ratio: f64,
    /// Whether this table needs vacuum
    pub needs_vacuum: bool,
    /// Whether this table needs analyze
    pub needs_analyze: bool,
    /// Priority level for maintenance
    pub maintenance_priority: PostgresMaintenancePriority,
}

/// Maintenance priority levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresMaintenancePriority {
    /// Immediate attention required
    Critical,
    /// Should be addressed soon
    High,
    /// Normal maintenance window
    Medium,
    /// Low priority
    Low,
    /// No action needed
    None,
}

/// Recent vacuum activity information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresVacuumActivity {
    /// Table that was vacuumed
    pub table_name: String,
    /// Type of operation
    pub operation_type: PostgresVacuumType,
    /// When the operation completed
    pub completion_time: DateTimeWrapper,
    /// Duration of the operation (seconds)
    pub duration_seconds: f64,
    /// Whether the operation was successful
    pub was_successful: bool,
    /// Pages removed (if available)
    pub pages_removed: Option<u64>,
    /// Tuples removed (if available)
    pub tuples_removed: Option<u64>,
}

/// Autovacuum settings for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableAutovacuumSettings {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Whether autovacuum is enabled for this table
    pub autovacuum_enabled: bool,
    /// Vacuum threshold (minimum dead tuples)
    pub vacuum_threshold: i32,
    /// Vacuum scale factor
    pub vacuum_scale_factor: f64,
    /// Analyze threshold (minimum modified tuples)
    pub analyze_threshold: i32,
    /// Analyze scale factor
    pub analyze_scale_factor: f64,
    /// Custom vacuum cost delay
    pub vacuum_cost_delay: Option<i32>,
    /// Custom vacuum cost limit
    pub vacuum_cost_limit: Option<i32>,
    /// Whether settings are customized for this table
    pub has_custom_settings: bool,
}

/// Global vacuum statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresVacuumStats {
    /// Total number of tables
    pub total_tables: u64,
    /// Tables that have never been vacuumed
    pub never_vacuumed: u64,
    /// Tables overdue for vacuum
    pub overdue_vacuum: u64,
    /// Tables overdue for analyze
    pub overdue_analyze: u64,
    /// Average time since last vacuum (hours)
    pub avg_hours_since_vacuum: f64,
    /// Average time since last analyze (hours)
    pub avg_hours_since_analyze: f64,
    /// Total dead tuples across all tables
    pub total_dead_tuples: u64,
    /// Total live tuples across all tables
    pub total_live_tuples: u64,
    /// Overall dead tuple percentage
    pub overall_dead_tuple_percentage: f64,
}

/// Bloat analysis results
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresBloatAnalysis {
    /// Tables with highest estimated bloat
    pub high_bloat_tables: Vec<PostgresTableBloatInfo>,
    /// Total estimated bloat across all tables (bytes)
    pub total_estimated_bloat_bytes: u64,
    /// Average bloat percentage across all tables
    pub average_bloat_percentage: f64,
    /// Number of tables with concerning bloat levels
    pub tables_with_high_bloat: u64,
    /// Whether bloat is a system-wide concern
    pub is_bloat_concerning: bool,
}

/// Bloat information for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableBloatInfo {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Total table size in bytes
    pub total_size_bytes: u64,
    /// Human-readable total size
    pub total_size_pretty: String,
    /// Estimated bloat percentage
    pub bloat_percentage: f64,
    /// Estimated bloat size in bytes
    pub bloat_size_bytes: u64,
    /// Live tuples count
    pub live_tuples: u64,
    /// Dead tuples count
    pub dead_tuples: u64,
    /// Recommended action
    pub recommended_action: String,
}

/// Dead tuple statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDeadTupleStats {
    /// Distribution of dead tuple ratios across tables
    pub dead_tuple_distribution: HashMap<String, u64>, // Range -> Count
    /// Tables with highest dead tuple counts
    pub highest_dead_tuple_tables: Vec<String>,
    /// Tables with highest dead tuple ratios
    pub highest_dead_tuple_ratio_tables: Vec<String>,
    /// Average dead tuple ratio across all tables
    pub average_dead_tuple_ratio: f64,
    /// Whether dead tuple levels are concerning
    pub is_dead_tuple_level_concerning: bool,
}

impl PostgresVacuumInfo {
    /// Checks if autovacuum is properly configured
    ///
    /// # Returns
    /// * True if autovacuum appears to be working effectively
    pub fn is_autovacuum_healthy(&self) -> bool {
        self.autovacuum_enabled
            && self.autovacuum_max_workers > 0
            && self.vacuum_stats.overdue_vacuum < (self.vacuum_stats.total_tables / 10)
        // Less than 10% overdue
    }

    /// Gets tables that urgently need vacuum
    ///
    /// # Arguments
    /// * `dead_tuple_threshold` - Minimum dead tuple ratio to consider urgent
    ///
    /// # Returns
    /// * Vector of tables needing immediate vacuum attention
    pub fn get_urgent_vacuum_tables(&self, dead_tuple_threshold: f64) -> Vec<&PostgresTableVacuumStatus> {
        if let Some(detailed) = &self.detailed_metrics {
            detailed
                .tables_needing_vacuum
                .iter()
                .filter(|table| {
                    table.dead_tuple_ratio > dead_tuple_threshold || table.maintenance_priority == PostgresMaintenancePriority::Critical
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Gets tables that have never been vacuumed
    ///
    /// # Returns
    /// * Vector of tables that have never been vacuumed
    pub fn get_never_vacuumed_tables(&self) -> Vec<&PostgresTableVacuumStatus> {
        if let Some(detailed) = &self.detailed_metrics {
            detailed
                .tables_needing_vacuum
                .iter()
                .filter(|table| table.last_vacuum.is_none() && table.last_autovacuum.is_none())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Calculates vacuum workload intensity
    ///
    /// # Returns
    /// * Number indicating current vacuum workload (0.0 to 1.0+)
    pub fn get_vacuum_workload_intensity(&self) -> f64 {
        if self.autovacuum_max_workers == 0 {
            0.0
        } else {
            self.active_autovacuum_workers as f64 / self.autovacuum_max_workers as f64
        }
    }

    /// Checks if vacuum workers are overloaded
    ///
    /// # Arguments
    /// * `threshold_percentage` - Maximum acceptable worker utilization
    ///
    /// # Returns
    /// * True if vacuum workers are overutilized
    pub fn are_vacuum_workers_overloaded(&self, threshold_percentage: f64) -> bool {
        self.get_vacuum_workload_intensity() * 100.0 > threshold_percentage
    }

    /// Gets tables with custom autovacuum settings
    ///
    /// # Returns
    /// * Vector of tables with non-default autovacuum configuration
    pub fn get_tables_with_custom_settings(&self) -> Vec<&PostgresTableAutovacuumSettings> {
        if let Some(detailed) = &self.detailed_metrics {
            detailed.table_autovacuum_settings.iter().filter(|settings| settings.has_custom_settings).collect()
        } else {
            Vec::new()
        }
    }

    /// Calculates overall maintenance health score
    ///
    /// # Returns
    /// * Health score from 0-100 (higher is better)
    pub fn get_maintenance_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for disabled autovacuum
        if !self.autovacuum_enabled {
            score -= 50.0;
        }

        // Deduct for overdue tables
        if self.vacuum_stats.total_tables > 0 {
            let overdue_percentage = (self.vacuum_stats.overdue_vacuum as f64 / self.vacuum_stats.total_tables as f64) * 100.0;
            score -= overdue_percentage.min(30.0);
        }

        // Deduct for high dead tuple ratio
        if self.overall_dead_tuple_percentage > 20.0 {
            score -= (self.overall_dead_tuple_percentage - 20.0).min(25.0);
        }

        // Deduct for never vacuumed tables
        if self.vacuum_stats.total_tables > 0 {
            let never_vacuumed_pct = (self.vacuum_stats.never_vacuumed as f64 / self.vacuum_stats.total_tables as f64) * 100.0;
            score -= never_vacuumed_pct.min(20.0);
        }

        // Deduct for overloaded vacuum workers
        if self.are_vacuum_workers_overloaded(90.0) {
            score -= 15.0;
        }

        // Deduct for high bloat
        if let Some(detailed) = &self.detailed_metrics
            && detailed.bloat_analysis.is_bloat_concerning
        {
            score -= 20.0;
        }

        score.max(0.0)
    }

    /// Gets vacuum optimization recommendations
    ///
    /// # Returns
    /// * Vector of recommendations for improving vacuum performance
    pub fn get_optimization_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !self.autovacuum_enabled {
            recommendations.push("Enable autovacuum for automated maintenance".to_string());
        }

        if self.are_vacuum_workers_overloaded(80.0) {
            recommendations.push("Consider increasing autovacuum_max_workers".to_string());
        }

        if self.vacuum_stats.overdue_vacuum > (self.vacuum_stats.total_tables / 5) {
            recommendations.push("Many tables are overdue for vacuum - review autovacuum settings".to_string());
        }

        if self.overall_dead_tuple_percentage > 25.0 {
            recommendations.push("High dead tuple ratio - consider more aggressive autovacuum settings".to_string());
        }

        if self.vacuum_stats.never_vacuumed > 0 {
            recommendations.push(format!("{} tables have never been vacuumed - run manual VACUUM", self.vacuum_stats.never_vacuumed));
        }

        if let Some(detailed) = &self.detailed_metrics
            && detailed.bloat_analysis.is_bloat_concerning
        {
            recommendations.push("High table bloat detected - schedule VACUUM FULL for affected tables".to_string());
        }

        let urgent_tables = self.get_urgent_vacuum_tables(30.0);
        if !urgent_tables.is_empty() {
            recommendations.push(format!("{} tables need immediate vacuum attention", urgent_tables.len()));
        }

        if self.autovacuum_naptime > 300 {
            recommendations.push("Consider decreasing autovacuum_naptime for more frequent checks".to_string());
        }

        recommendations
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Checks if vacuum operations are keeping up with workload
    ///
    /// # Returns
    /// * True if vacuum operations appear to be keeping pace
    pub fn is_vacuum_keeping_up(&self) -> bool {
        self.overall_dead_tuple_percentage < 15.0
            && self.vacuum_stats.overdue_vacuum < (self.vacuum_stats.total_tables / 20)
            && !self.are_vacuum_workers_overloaded(80.0)
    }

    /// Gets vacuum efficiency metrics
    ///
    /// # Returns
    /// * Tuple of (vacuum_coverage, timeliness_score, resource_utilization)
    pub fn get_vacuum_efficiency_metrics(&self) -> (f64, f64, f64) {
        let vacuum_coverage = if self.vacuum_stats.total_tables == 0 {
            100.0
        } else {
            ((self.vacuum_stats.total_tables - self.vacuum_stats.never_vacuumed) as f64 / self.vacuum_stats.total_tables as f64) * 100.0
        };

        let timeliness_score = if self.vacuum_stats.total_tables == 0 {
            100.0
        } else {
            100.0 - ((self.vacuum_stats.overdue_vacuum as f64 / self.vacuum_stats.total_tables as f64) * 100.0)
        };

        let resource_utilization = self.get_vacuum_workload_intensity() * 100.0;

        (vacuum_coverage, timeliness_score, resource_utilization)
    }

    /// Gets overall vacuum system status
    ///
    /// # Returns
    /// * String describing overall vacuum system health
    pub fn get_vacuum_system_status(&self) -> String {
        let health_score = self.get_maintenance_health_score();

        if health_score >= 90.0 {
            "Excellent - Vacuum system is operating optimally".to_string()
        } else if health_score >= 75.0 {
            "Good - Vacuum system is performing well with minor issues".to_string()
        } else if health_score >= 60.0 {
            "Fair - Vacuum system has some performance issues".to_string()
        } else if health_score >= 40.0 {
            "Poor - Vacuum system has significant problems".to_string()
        } else {
            "Critical - Vacuum system requires immediate attention".to_string()
        }
    }
}

/// Maintenance task information
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresMaintenanceTask {
    /// Type of maintenance task
    pub task_type: PostgresMaintenanceTaskType,
    /// Full table name (schema.table)
    pub table_name: String,
    /// Priority level
    pub priority: PostgresMaintenancePriority,
    /// Estimated duration in minutes
    pub estimated_duration_minutes: u32,
    /// Task description
    pub description: String,
}

/// Types of maintenance tasks
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PostgresMaintenanceTaskType {
    /// Regular VACUUM
    Vacuum,
    /// VACUUM FULL (table rebuild)
    VacuumFull,
    /// ANALYZE (update statistics)
    Analyze,
    /// REINDEX (rebuild indexes)
    Reindex,
    /// CLUSTER (reorder table)
    Cluster,
}

impl PostgresVacuumType {
    /// Parses vacuum type from query text
    ///
    /// # Arguments
    /// * `query` - SQL query text
    ///
    /// # Returns
    /// * Corresponding vacuum type
    pub fn from_query(query: &str) -> Self {
        let query_upper = query.to_uppercase();

        if query_upper.contains("VACUUM FULL") {
            PostgresVacuumType::VacuumFull
        } else if query_upper.contains("VACUUM") && query_upper.contains("ANALYZE") {
            PostgresVacuumType::VacuumAnalyze
        } else if query_upper.contains("VACUUM") && query_upper.contains("AUTOVACUUM") {
            PostgresVacuumType::Autovacuum
        } else if query_upper.contains("VACUUM") {
            PostgresVacuumType::Vacuum
        } else if query_upper.contains("ANALYZE") {
            PostgresVacuumType::Analyze
        } else {
            PostgresVacuumType::Unknown
        }
    }

    /// Gets human-readable description
    ///
    /// # Returns
    /// * String description of the vacuum type
    pub fn description(&self) -> &'static str {
        match self {
            PostgresVacuumType::Vacuum => "Regular vacuum to reclaim space",
            PostgresVacuumType::VacuumFull => "Full vacuum to rebuild table",
            PostgresVacuumType::Autovacuum => "Automatic vacuum operation",
            PostgresVacuumType::VacuumAnalyze => "Vacuum with statistics update",
            PostgresVacuumType::Analyze => "Statistics update only",
            PostgresVacuumType::Unknown => "Unknown operation type",
        }
    }
}

impl PostgresMaintenancePriority {
    /// Gets the urgency level as a numeric value
    ///
    /// # Returns
    /// * Numeric urgency (0 = most urgent, 4 = least urgent)
    pub fn urgency_level(&self) -> u8 {
        match self {
            PostgresMaintenancePriority::Critical => 0,
            PostgresMaintenancePriority::High => 1,
            PostgresMaintenancePriority::Medium => 2,
            PostgresMaintenancePriority::Low => 3,
            PostgresMaintenancePriority::None => 4,
        }
    }

    /// Gets color code for UI display
    ///
    /// # Returns
    /// * Color code string
    pub fn color_code(&self) -> &'static str {
        match self {
            PostgresMaintenancePriority::Critical => "#FF0000", // Red
            PostgresMaintenancePriority::High => "#FF8C00",     // Dark Orange
            PostgresMaintenancePriority::Medium => "#FFD700",   // Gold
            PostgresMaintenancePriority::Low => "#90EE90",      // Light Green
            PostgresMaintenancePriority::None => "#D3D3D3",     // Light Gray
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
    async fn test_postgres_vacuum_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let vacuum_info = PostgresVacuumInfo::default();

        let result = vacuum_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok(), "sync_metadata failed: {:?}", result.as_ref().err());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.autovacuum_max_workers >= 0);
        assert!(info.autovacuum_naptime >= 0);
        assert!(info.overall_dead_tuple_percentage >= 0.0);
    }

    #[test]
    fn test_vacuum_type_parsing() {
        assert_eq!(PostgresVacuumType::from_query("VACUUM FULL table1"), PostgresVacuumType::VacuumFull);
        assert_eq!(PostgresVacuumType::from_query("VACUUM ANALYZE table1"), PostgresVacuumType::VacuumAnalyze);
        assert_eq!(PostgresVacuumType::from_query("autovacuum: VACUUM table1"), PostgresVacuumType::Autovacuum);
        assert_eq!(PostgresVacuumType::from_query("VACUUM table1"), PostgresVacuumType::Vacuum);
        assert_eq!(PostgresVacuumType::from_query("ANALYZE table1"), PostgresVacuumType::Analyze);
    }

    #[test]
    fn test_maintenance_priority_urgency() {
        assert_eq!(PostgresMaintenancePriority::Critical.urgency_level(), 0);
        assert_eq!(PostgresMaintenancePriority::High.urgency_level(), 1);
        assert_eq!(PostgresMaintenancePriority::Medium.urgency_level(), 2);
        assert_eq!(PostgresMaintenancePriority::Low.urgency_level(), 3);
        assert_eq!(PostgresMaintenancePriority::None.urgency_level(), 4);
    }

    #[test]
    fn test_vacuum_workload_intensity() {
        let mut vacuum_info = PostgresVacuumInfo {
            autovacuum_max_workers: 4,
            active_autovacuum_workers: 2,
            ..Default::default()
        };

        assert_eq!(vacuum_info.get_vacuum_workload_intensity(), 0.5);

        vacuum_info.autovacuum_max_workers = 0;
        assert_eq!(vacuum_info.get_vacuum_workload_intensity(), 0.0);
    }
}
