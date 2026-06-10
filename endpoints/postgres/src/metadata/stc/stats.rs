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

/// PostgreSQL performance statistics and metrics
///
/// This struct contains comprehensive performance metrics including query throughput,
/// cache hit ratios, I/O statistics, and slow query analysis. Critical for monitoring
/// database performance and identifying bottlenecks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresPerformanceStats {
    /// Buffer cache hit ratio (percentage)
    pub buffer_cache_hit_ratio: f64,
    /// Index hit ratio (percentage)
    pub index_hit_ratio: f64,
    /// Overall cache efficiency (percentage)
    pub overall_cache_efficiency: f64,
    /// Total database operations across all databases
    pub total_operations: u64,
    /// Total transactions across all databases
    pub total_transactions: u64,
    /// Total blocks read from disk
    pub total_blocks_read: u64,
    /// Total blocks hit in cache
    pub total_blocks_hit: u64,
    /// Total temporary files created
    pub total_temp_files: u64,
    /// Total bytes of temporary files
    pub total_temp_bytes: u64,
    /// Performance score (0-100)
    pub performance_score: f64,
    /// Whether system appears I/O bound
    pub is_io_bound: bool,
    /// Whether system appears CPU bound
    pub is_cpu_bound: bool,
    /// Workload pattern description
    pub workload_pattern: String,
    /// Detailed metrics collected only when performance issues detected
    pub detailed_metrics: Option<PostgresDetailedPerformanceMetrics>,
}

/// Detailed performance metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedPerformanceMetrics {
    /// Slow queries (only collected when performance issues detected)
    pub slow_queries: Vec<PostgresSlowQuery>,
    /// Most frequent queries
    pub frequent_queries: Vec<PostgresFrequentQuery>,
    /// Per-database performance statistics
    pub database_stats: Vec<PostgresPerformanceDatabaseStats>,
    /// Table I/O statistics for high-load tables
    pub table_io_stats: Vec<PostgresTableIOStats>,
    /// Tables with problematic sequential scan patterns
    pub problematic_seq_scan_tables: Vec<PostgresTableScanStats>,
    /// Performance recommendations
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresPerformanceStats {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "basic_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    SUM(tup_returned + tup_fetched + tup_inserted + tup_updated + tup_deleted)::bigint as total_operations,
                    SUM(xact_commit + xact_rollback)::bigint as total_transactions,
                    SUM(blks_read)::bigint as total_blocks_read,
                    SUM(blks_hit)::bigint as total_blocks_hit,
                    CASE WHEN SUM(blks_read + blks_hit) > 0 THEN
                        (SUM(blks_hit)::float / SUM(blks_read + blks_hit)::float) * 100
                    ELSE 100 END::double precision as buffer_hit_ratio,
                    SUM(temp_files)::bigint as total_temp_files,
                    SUM(temp_bytes)::bigint as total_temp_bytes
                FROM pg_stat_database
                WHERE datname IS NOT NULL"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "index_stats".to_string(),
                QueryInput::new(
                    "SELECT
                    SUM(idx_blks_read)::bigint as total_idx_blocks_read,
                    SUM(idx_blks_hit)::bigint as total_idx_blocks_hit,
                    CASE WHEN SUM(idx_blks_read + idx_blks_hit) > 0 THEN
                        (SUM(idx_blks_hit)::float / SUM(idx_blks_read + idx_blks_hit)::float) * 100
                    ELSE 100 END::double precision as index_hit_ratio
                FROM pg_statio_user_tables"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL performance statistics including throughput, cache ratios, and query performance"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "performance"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresPerformanceStats {
    const BUFFER_HIT_RATIO_THRESHOLD: f64 = 95.0;
    const INDEX_HIT_RATIO_THRESHOLD: f64 = 99.0;
    const TEMP_FILES_THRESHOLD: u64 = 100;
    const TEMP_BYTES_THRESHOLD: u64 = 100_000_000; // 100MB
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

        let mut performance_stats = PostgresPerformanceStats::default();
        let requests = self.request();

        // Execute basic stats query
        if let Some(row) = run_single_row(&requests, "basic_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            performance_stats.total_operations = Self::safe_i64_to_u64(&row, "total_operations")?;
            performance_stats.total_transactions = Self::safe_i64_to_u64(&row, "total_transactions")?;
            performance_stats.total_blocks_read = Self::safe_i64_to_u64(&row, "total_blocks_read")?;
            performance_stats.total_blocks_hit = Self::safe_i64_to_u64(&row, "total_blocks_hit")?;
            performance_stats.buffer_cache_hit_ratio = Self::safe_get_f64(&row, "buffer_hit_ratio")?;
            performance_stats.total_temp_files = Self::safe_i64_to_u64(&row, "total_temp_files")?;
            performance_stats.total_temp_bytes = Self::safe_i64_to_u64(&row, "total_temp_bytes")?;
        }

        // Execute index stats query
        if let Some(row) = run_single_row(&requests, "index_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            performance_stats.index_hit_ratio = Self::safe_get_f64(&row, "index_hit_ratio")?;
        }

        // Calculate derived metrics
        performance_stats.overall_cache_efficiency = (performance_stats.buffer_cache_hit_ratio + performance_stats.index_hit_ratio) / 2.0;

        performance_stats.is_io_bound = Self::calculate_io_bound(&performance_stats);
        performance_stats.is_cpu_bound = Self::calculate_cpu_bound(&performance_stats);
        performance_stats.workload_pattern = Self::analyze_workload_pattern(&performance_stats);
        performance_stats.performance_score = Self::calculate_performance_score(&performance_stats);

        // Conditionally collect detailed metrics only when problems are detected
        performance_stats.detailed_metrics = Self::collect_detailed_metrics_if_needed(&performance_stats, context).await?;

        Ok(performance_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        core_stats: &PostgresPerformanceStats,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedPerformanceMetrics>> {
        let needs_detailed_analysis = core_stats.buffer_cache_hit_ratio < Self::BUFFER_HIT_RATIO_THRESHOLD
            || core_stats.index_hit_ratio < Self::INDEX_HIT_RATIO_THRESHOLD
            || core_stats.total_temp_files > Self::TEMP_FILES_THRESHOLD
            || core_stats.total_temp_bytes > Self::TEMP_BYTES_THRESHOLD
            || core_stats.performance_score < 80.0;

        if !needs_detailed_analysis {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresDetailedPerformanceMetrics {
            slow_queries: Vec::new(),
            frequent_queries: Vec::new(),
            database_stats: Vec::new(),
            table_io_stats: Vec::new(),
            problematic_seq_scan_tables: Vec::new(),
            recommendations: Vec::new(),
        };

        // Collect slow queries if pg_stat_statements is available
        if let Ok(slow_query_rows) = Self::query_slow_queries(context.clone()).await {
            detailed_metrics.slow_queries = Self::parse_slow_queries(slow_query_rows)?;
        }

        // Collect frequent queries
        if let Ok(frequent_query_rows) = Self::query_frequent_queries(context.clone()).await {
            detailed_metrics.frequent_queries = Self::parse_frequent_queries(frequent_query_rows)?;
        }

        // Collect database performance stats
        if let Ok(db_stats_rows) = Self::query_database_stats(context.clone()).await {
            detailed_metrics.database_stats = Self::parse_database_stats(db_stats_rows)?;
        }

        // Collect table I/O stats
        if let Ok(table_io_rows) = Self::query_table_io_stats(context.clone()).await {
            detailed_metrics.table_io_stats = Self::parse_table_io_stats(table_io_rows)?;
        }

        // Collect sequential scan stats
        if let Ok(seq_scan_rows) = Self::query_sequential_scan_stats(context.clone()).await {
            detailed_metrics.problematic_seq_scan_tables = Self::parse_sequential_scan_stats(seq_scan_rows)?;
        }

        // Generate recommendations
        detailed_metrics.recommendations = Self::generate_recommendations(core_stats, &detailed_metrics);

        Ok(Some(detailed_metrics))
    }

    async fn query_slow_queries(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                LEFT(query, 500) as query, calls,
                total_exec_time, mean_exec_time, max_exec_time, stddev_exec_time,
                rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
                local_blks_hit, local_blks_read, temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time
            FROM pg_stat_statements
            WHERE mean_exec_time > 100.0  -- Only queries over 100ms
            ORDER BY mean_exec_time DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "slow_queries").await
    }

    async fn query_frequent_queries(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                LEFT(query, 300) as query, calls,
                total_exec_time, mean_exec_time, rows,
                shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written
            FROM pg_stat_statements
            WHERE calls > 100  -- Only frequently called queries
            ORDER BY calls DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "frequent_queries").await
    }

    async fn query_database_stats(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                datname, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
                xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes,
                deadlocks, checksum_failures, stats_reset,
                CASE WHEN (blks_read + blks_hit) > 0 THEN
                    (blks_hit::float / (blks_read + blks_hit)::float) * 100
                ELSE 100 END::double precision as cache_hit_ratio
            FROM pg_stat_database
            WHERE datname IS NOT NULL
                AND (tup_returned + tup_fetched + tup_inserted + tup_updated + tup_deleted) > 0
            ORDER BY (tup_returned + tup_fetched + tup_inserted + tup_updated + tup_deleted) DESC"
                .to_string(),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "database_stats").await
    }

    async fn query_table_io_stats(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                schemaname, relname, heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
                toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit,
                CASE WHEN (heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit) > 0 THEN
                    ((heap_blks_hit + idx_blks_hit)::float /
                     (heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit)::float) * 100
                ELSE 100 END::double precision as table_cache_hit_ratio
            FROM pg_statio_user_tables
            WHERE (heap_blks_read + idx_blks_read) > 100  -- Only tables with significant I/O
            ORDER BY (heap_blks_read + idx_blks_read) DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "table_io_stats").await
    }

    async fn query_sequential_scan_stats(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
                n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
                CASE WHEN (seq_scan + idx_scan) > 0 THEN
                    (seq_scan::float / (seq_scan + idx_scan)::float) * 100
                ELSE 0 END::double precision as seq_scan_ratio,
                CASE WHEN seq_scan > 0 THEN seq_tup_read::float / seq_scan::float
                ELSE 0 END::double precision as avg_tuples_per_seq_scan
            FROM pg_stat_user_tables
            WHERE seq_scan > 10  -- Only tables with significant sequential scans
                AND seq_tup_read > 1000  -- And significant tuple reads
            ORDER BY seq_tup_read DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "sequential_scan_stats").await
    }

    // Helper functions for safe type conversion (same as activity code)
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

    #[allow(dead_code)]
    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
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

    fn calculate_io_bound(stats: &PostgresPerformanceStats) -> bool {
        stats.buffer_cache_hit_ratio < 90.0 || stats.index_hit_ratio < 95.0 || stats.total_blocks_read > stats.total_blocks_hit
    }

    fn calculate_cpu_bound(stats: &PostgresPerformanceStats) -> bool {
        // CPU bound indicators: high temp file usage, suggesting complex operations
        stats.total_temp_files > Self::TEMP_FILES_THRESHOLD || stats.total_temp_bytes > Self::TEMP_BYTES_THRESHOLD
    }

    fn analyze_workload_pattern(stats: &PostgresPerformanceStats) -> String {
        if stats.total_operations == 0 {
            return "No significant activity detected".to_string();
        }

        if stats.total_temp_files > 0 {
            "Complex analytical workload with temporary file usage".to_string()
        } else if stats.buffer_cache_hit_ratio > 98.0 {
            "Cache-friendly OLTP workload".to_string()
        } else if stats.buffer_cache_hit_ratio < 90.0 {
            "I/O intensive workload".to_string()
        } else {
            "Mixed workload pattern".to_string()
        }
    }

    fn calculate_performance_score(stats: &PostgresPerformanceStats) -> f64 {
        let mut score = 100.0;

        // Deduct for poor cache hit ratios
        if stats.buffer_cache_hit_ratio < 95.0 {
            score -= (95.0 - stats.buffer_cache_hit_ratio) * 2.0;
        }

        if stats.index_hit_ratio < 99.0 {
            score -= (99.0 - stats.index_hit_ratio) * 1.5;
        }

        // Deduct for excessive temp file usage
        if stats.total_temp_files > Self::TEMP_FILES_THRESHOLD {
            score -= 15.0;
        }

        if stats.total_temp_bytes > Self::TEMP_BYTES_THRESHOLD {
            score -= 10.0;
        }

        score.clamp(0.0, 100.0)
    }

    fn parse_slow_queries(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresSlowQuery>> {
        let mut queries = Vec::with_capacity(rows.len());

        for row in rows {
            queries.push(PostgresSlowQuery {
                query: Self::safe_get_string(&row, "query")?,
                calls: Self::safe_i64_to_u64(&row, "calls")?,
                total_exec_time: Self::safe_get_f64(&row, "total_exec_time")?,
                mean_exec_time: Self::safe_get_f64(&row, "mean_exec_time")?,
                max_exec_time: Self::safe_get_f64(&row, "max_exec_time")?,
                stddev_exec_time: Self::safe_get_f64(&row, "stddev_exec_time")?,
                rows: Self::safe_i64_to_u64(&row, "rows")?,
                shared_blks_hit: Self::safe_i64_to_u64(&row, "shared_blks_hit")?,
                shared_blks_read: Self::safe_i64_to_u64(&row, "shared_blks_read")?,
                shared_blks_dirtied: Self::safe_i64_to_u64(&row, "shared_blks_dirtied")?,
                shared_blks_written: Self::safe_i64_to_u64(&row, "shared_blks_written")?,
                local_blks_hit: Self::safe_i64_to_u64(&row, "local_blks_hit")?,
                local_blks_read: Self::safe_i64_to_u64(&row, "local_blks_read")?,
                temp_blks_read: Self::safe_i64_to_u64(&row, "temp_blks_read")?,
                temp_blks_written: Self::safe_i64_to_u64(&row, "temp_blks_written")?,
                blk_read_time: Self::safe_get_f64(&row, "blk_read_time")?,
                blk_write_time: Self::safe_get_f64(&row, "blk_write_time")?,
            });
        }

        Ok(queries)
    }

    fn parse_frequent_queries(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresFrequentQuery>> {
        let mut queries = Vec::with_capacity(rows.len());

        for row in rows {
            queries.push(PostgresFrequentQuery {
                query: Self::safe_get_string(&row, "query")?,
                calls: Self::safe_i64_to_u64(&row, "calls")?,
                total_exec_time: Self::safe_get_f64(&row, "total_exec_time")?,
                mean_exec_time: Self::safe_get_f64(&row, "mean_exec_time")?,
                rows: Self::safe_i64_to_u64(&row, "rows")?,
                shared_blks_hit: Self::safe_i64_to_u64(&row, "shared_blks_hit")?,
                shared_blks_read: Self::safe_i64_to_u64(&row, "shared_blks_read")?,
                temp_blks_read: Self::safe_i64_to_u64(&row, "temp_blks_read")?,
                temp_blks_written: Self::safe_i64_to_u64(&row, "temp_blks_written")?,
            });
        }

        Ok(queries)
    }

    fn parse_database_stats(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresPerformanceDatabaseStats>> {
        let mut stats = Vec::with_capacity(rows.len());

        for row in rows {
            stats.push(PostgresPerformanceDatabaseStats {
                database_name: Self::safe_get_string(&row, "datname")?,
                tuples_returned: Self::safe_i64_to_u64(&row, "tup_returned")?,
                tuples_fetched: Self::safe_i64_to_u64(&row, "tup_fetched")?,
                tuples_inserted: Self::safe_i64_to_u64(&row, "tup_inserted")?,
                tuples_updated: Self::safe_i64_to_u64(&row, "tup_updated")?,
                tuples_deleted: Self::safe_i64_to_u64(&row, "tup_deleted")?,
                transactions_committed: Self::safe_i64_to_u64(&row, "xact_commit")?,
                transactions_rolled_back: Self::safe_i64_to_u64(&row, "xact_rollback")?,
                blocks_read: Self::safe_i64_to_u64(&row, "blks_read")?,
                blocks_hit: Self::safe_i64_to_u64(&row, "blks_hit")?,
                temp_files: Self::safe_i64_to_u64(&row, "temp_files")?,
                temp_bytes: Self::safe_i64_to_u64(&row, "temp_bytes")?,
                deadlocks: Self::safe_i64_to_u64(&row, "deadlocks")?,
                cache_hit_ratio: Self::safe_get_f64(&row, "cache_hit_ratio")?,
                stats_reset: Self::safe_get_datetime(&row, "stats_reset")?,
            });
        }

        Ok(stats)
    }

    fn parse_table_io_stats(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableIOStats>> {
        let mut stats = Vec::with_capacity(rows.len());

        for row in rows {
            stats.push(PostgresTableIOStats {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                heap_blocks_read: Self::safe_i64_to_u64(&row, "heap_blks_read")?,
                heap_blocks_hit: Self::safe_i64_to_u64(&row, "heap_blks_hit")?,
                index_blocks_read: Self::safe_i64_to_u64(&row, "idx_blks_read")?,
                index_blocks_hit: Self::safe_i64_to_u64(&row, "idx_blks_hit")?,
                table_cache_hit_ratio: Self::safe_get_f64(&row, "table_cache_hit_ratio")?,
            });
        }

        Ok(stats)
    }

    fn parse_sequential_scan_stats(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableScanStats>> {
        let mut stats = Vec::with_capacity(rows.len());

        for row in rows {
            stats.push(PostgresTableScanStats {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                sequential_scans: Self::safe_i64_to_u64(&row, "seq_scan")?,
                seq_tuples_read: Self::safe_i64_to_u64(&row, "seq_tup_read")?,
                index_scans: Self::safe_i64_to_u64(&row, "idx_scan")?,
                index_tuples_fetched: Self::safe_i64_to_u64(&row, "idx_tup_fetch")?,
                sequential_scan_ratio: Self::safe_get_f64(&row, "seq_scan_ratio")?,
                avg_tuples_per_seq_scan: Self::safe_get_f64(&row, "avg_tuples_per_seq_scan")?,
            });
        }

        Ok(stats)
    }

    fn generate_recommendations(
        core_stats: &PostgresPerformanceStats,
        detailed_metrics: &PostgresDetailedPerformanceMetrics,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if core_stats.buffer_cache_hit_ratio < Self::BUFFER_HIT_RATIO_THRESHOLD {
            recommendations.push(format!(
                "Buffer cache hit ratio is {:.1}% - consider increasing shared_buffers",
                core_stats.buffer_cache_hit_ratio
            ));
        }

        if core_stats.index_hit_ratio < Self::INDEX_HIT_RATIO_THRESHOLD {
            recommendations.push(format!(
                "Index hit ratio is {:.1}% - consider increasing effective_cache_size",
                core_stats.index_hit_ratio
            ));
        }

        if core_stats.total_temp_files > Self::TEMP_FILES_THRESHOLD {
            recommendations.push(format!(
                "High temporary file usage ({} files) - consider increasing work_mem",
                core_stats.total_temp_files
            ));
        }

        if !detailed_metrics.slow_queries.is_empty() {
            recommendations.push(format!(
                "Found {} slow queries - review query optimization and indexing",
                detailed_metrics.slow_queries.len()
            ));
        }

        if !detailed_metrics.problematic_seq_scan_tables.is_empty() {
            recommendations.push(format!(
                "Found {} tables with excessive sequential scans - review indexing strategy",
                detailed_metrics.problematic_seq_scan_tables.len()
            ));
        }

        if core_stats.is_io_bound {
            recommendations.push("System appears I/O bound - consider storage optimization".to_string());
        }

        if core_stats.is_cpu_bound {
            recommendations.push("System appears CPU bound - review query complexity".to_string());
        }

        recommendations
    }
}

/// Information about a slow query from pg_stat_statements
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSlowQuery {
    /// SQL query text (truncated for safety)
    pub query: String,
    /// Number of times this query was executed
    pub calls: u64,
    /// Total execution time across all calls (milliseconds)
    pub total_exec_time: f64,
    /// Average execution time per call (milliseconds)
    pub mean_exec_time: f64,
    /// Maximum execution time for any single call (milliseconds)
    pub max_exec_time: f64,
    /// Standard deviation of execution times
    pub stddev_exec_time: f64,
    /// Total number of rows returned/affected
    pub rows: u64,
    /// Shared buffer blocks hit
    pub shared_blks_hit: u64,
    /// Shared buffer blocks read from disk
    pub shared_blks_read: u64,
    /// Shared buffer blocks dirtied
    pub shared_blks_dirtied: u64,
    /// Shared buffer blocks written
    pub shared_blks_written: u64,
    /// Local buffer blocks hit
    pub local_blks_hit: u64,
    /// Local buffer blocks read
    pub local_blks_read: u64,
    /// Temporary blocks read
    pub temp_blks_read: u64,
    /// Temporary blocks written
    pub temp_blks_written: u64,
    /// Time spent reading blocks (milliseconds)
    pub blk_read_time: f64,
    /// Time spent writing blocks (milliseconds)
    pub blk_write_time: f64,
}

/// Information about a frequently executed query
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresFrequentQuery {
    /// SQL query text (truncated for safety)
    pub query: String,
    /// Number of times this query was executed
    pub calls: u64,
    /// Total execution time across all calls (milliseconds)
    pub total_exec_time: f64,
    /// Average execution time per call (milliseconds)
    pub mean_exec_time: f64,
    /// Total number of rows returned/affected
    pub rows: u64,
    /// Shared buffer blocks hit
    pub shared_blks_hit: u64,
    /// Shared buffer blocks read from disk
    pub shared_blks_read: u64,
    /// Temporary blocks read
    pub temp_blks_read: u64,
    /// Temporary blocks written
    pub temp_blks_written: u64,
}

/// Performance statistics for a specific database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresPerformanceDatabaseStats {
    /// Database name
    pub database_name: String,
    /// Number of tuples returned by queries
    pub tuples_returned: u64,
    /// Number of tuples fetched by queries
    pub tuples_fetched: u64,
    /// Number of tuples inserted
    pub tuples_inserted: u64,
    /// Number of tuples updated
    pub tuples_updated: u64,
    /// Number of tuples deleted
    pub tuples_deleted: u64,
    /// Number of transactions committed
    pub transactions_committed: u64,
    /// Number of transactions rolled back
    pub transactions_rolled_back: u64,
    /// Number of disk blocks read
    pub blocks_read: u64,
    /// Number of buffer hits
    pub blocks_hit: u64,
    /// Number of temporary files created
    pub temp_files: u64,
    /// Total bytes of temporary files
    pub temp_bytes: u64,
    /// Number of deadlocks
    pub deadlocks: u64,
    /// Cache hit ratio for this database
    pub cache_hit_ratio: f64,
    /// When statistics were last reset
    pub stats_reset: Option<DateTimeWrapper>,
}

/// I/O statistics for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableIOStats {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Heap blocks read from disk
    pub heap_blocks_read: u64,
    /// Heap blocks hit in cache
    pub heap_blocks_hit: u64,
    /// Index blocks read from disk
    pub index_blocks_read: u64,
    /// Index blocks hit in cache
    pub index_blocks_hit: u64,
    /// Cache hit ratio for this table
    pub table_cache_hit_ratio: f64,
}

/// Scan statistics for a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableScanStats {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Number of sequential scans
    pub sequential_scans: u64,
    /// Tuples read by sequential scans
    pub seq_tuples_read: u64,
    /// Number of index scans
    pub index_scans: u64,
    /// Tuples fetched by index scans
    pub index_tuples_fetched: u64,
    /// Percentage of scans that are sequential
    pub sequential_scan_ratio: f64,
    /// Average tuples per sequential scan
    pub avg_tuples_per_seq_scan: f64,
}

impl PostgresPerformanceStats {
    /// Checks if cache hit ratios are healthy
    pub fn has_healthy_cache_ratios(&self) -> bool {
        self.buffer_cache_hit_ratio >= Self::BUFFER_HIT_RATIO_THRESHOLD && self.index_hit_ratio >= Self::INDEX_HIT_RATIO_THRESHOLD
    }

    /// Gets the overall performance assessment
    pub fn get_performance_assessment(&self) -> String {
        match self.performance_score {
            score if score >= 90.0 => "Excellent".to_string(),
            score if score >= 80.0 => "Good".to_string(),
            score if score >= 70.0 => "Fair".to_string(),
            score if score >= 60.0 => "Poor".to_string(),
            _ => "Critical".to_string(),
        }
    }

    /// Checks if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets all performance recommendations
    pub fn get_all_recommendations(&self) -> Vec<&String> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets slow queries if available
    pub fn get_slow_queries(&self) -> Vec<&PostgresSlowQuery> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.slow_queries.iter().collect()).unwrap_or_default()
    }

    /// Gets frequent queries if available
    pub fn get_frequent_queries(&self) -> Vec<&PostgresFrequentQuery> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.frequent_queries.iter().collect()).unwrap_or_default()
    }

    /// Gets database performance stats if available
    pub fn get_database_stats(&self) -> Vec<&PostgresPerformanceDatabaseStats> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.database_stats.iter().collect()).unwrap_or_default()
    }

    /// Gets tables with high I/O if available
    pub fn get_high_io_tables(&self) -> Vec<&PostgresTableIOStats> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.table_io_stats.iter().collect()).unwrap_or_default()
    }

    /// Gets tables with problematic sequential scans if available
    pub fn get_problematic_seq_scan_tables(&self) -> Vec<&PostgresTableScanStats> {
        self.detailed_metrics.as_ref().map(|metrics| metrics.problematic_seq_scan_tables.iter().collect()).unwrap_or_default()
    }

    /// Calculates I/O efficiency
    pub fn calculate_io_efficiency(&self) -> f64 {
        let total_blocks = self.total_blocks_read + self.total_blocks_hit;
        if total_blocks == 0 {
            100.0
        } else {
            (self.total_blocks_hit as f64 / total_blocks as f64) * 100.0
        }
    }

    /// Checks if temporary file usage is excessive
    pub fn has_excessive_temp_usage(&self) -> bool {
        self.total_temp_files > Self::TEMP_FILES_THRESHOLD || self.total_temp_bytes > Self::TEMP_BYTES_THRESHOLD
    }

    /// Gets performance summary
    pub fn get_performance_summary(&self) -> String {
        format!(
            "Performance: {} ({:.1}/100). Cache Hit: {:.1}%. Index Hit: {:.1}%. Workload: {}",
            self.get_performance_assessment(),
            self.performance_score,
            self.buffer_cache_hit_ratio,
            self.index_hit_ratio,
            self.workload_pattern
        )
    }

    /// Gets bottleneck analysis
    pub fn get_bottleneck_analysis(&self) -> String {
        let mut bottlenecks = Vec::new();

        if self.is_io_bound {
            bottlenecks.push("I/O");
        }
        if self.is_cpu_bound {
            bottlenecks.push("CPU");
        }
        if self.has_excessive_temp_usage() {
            bottlenecks.push("Memory");
        }

        if bottlenecks.is_empty() {
            "No major bottlenecks detected".to_string()
        } else {
            format!("Potential bottlenecks: {}", bottlenecks.join(", "))
        }
    }

    /// Checks if system needs immediate attention
    pub fn needs_immediate_attention(&self) -> bool {
        self.performance_score < 60.0
            || self.buffer_cache_hit_ratio < 80.0
            || self.index_hit_ratio < 90.0
            || (self.is_io_bound && self.is_cpu_bound)
    }

    /// Gets top recommendations by priority
    pub fn get_priority_recommendations(&self) -> Vec<String> {
        let mut priority_recs = Vec::new();

        if self.buffer_cache_hit_ratio < 80.0 {
            priority_recs.push("CRITICAL: Buffer cache hit ratio is very low - immediate tuning required".to_string());
        }

        if self.index_hit_ratio < 90.0 {
            priority_recs.push("HIGH: Index hit ratio is low - review index strategy".to_string());
        }

        if self.performance_score < 60.0 {
            priority_recs.push("HIGH: Overall performance is poor - comprehensive review needed".to_string());
        }

        if let Some(detailed) = &self.detailed_metrics {
            if detailed.slow_queries.len() > 10 {
                priority_recs.push("MEDIUM: Multiple slow queries detected - optimize query performance".to_string());
            }

            if detailed.problematic_seq_scan_tables.len() > 5 {
                priority_recs.push("MEDIUM: Multiple tables with poor scan patterns - review indexing".to_string());
            }
        }

        priority_recs
    }

    /// Calculates estimated operations per second (if time period known)
    pub fn calculate_operations_per_second(&self, time_period_seconds: f64) -> f64 {
        if time_period_seconds <= 0.0 {
            0.0
        } else {
            self.total_operations as f64 / time_period_seconds
        }
    }

    /// Gets cache efficiency breakdown
    pub fn get_cache_efficiency_breakdown(&self) -> (f64, f64, f64) {
        (self.buffer_cache_hit_ratio, self.index_hit_ratio, self.overall_cache_efficiency)
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_metadata_performance() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;
        let telemetry_wrapper = &mut telemetry_wrapper;

        let performance_stats = PostgresPerformanceStats::default();

        let result = performance_stats
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let stats = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(stats.buffer_cache_hit_ratio >= 0.0);
        assert!(stats.buffer_cache_hit_ratio <= 100.0);
        assert!(stats.index_hit_ratio >= 0.0);
        assert!(stats.index_hit_ratio <= 100.0);
        assert!(stats.performance_score >= 0.0);
        assert!(stats.performance_score <= 100.0);
        assert!(!stats.workload_pattern.is_empty());
    }

    #[tokio::test]
    async fn test_postgres_performance_calculations() {
        let stats = PostgresPerformanceStats {
            buffer_cache_hit_ratio: 95.5,
            index_hit_ratio: 99.2,
            overall_cache_efficiency: 97.35,
            total_operations: 1000,
            total_blocks_read: 100,
            total_blocks_hit: 900,
            ..Default::default()
        };

        assert!(stats.has_healthy_cache_ratios());
        assert_eq!(stats.calculate_io_efficiency(), 90.0);
        assert_eq!(stats.calculate_operations_per_second(10.0), 100.0);

        let (buffer, index, overall) = stats.get_cache_efficiency_breakdown();
        assert_eq!(buffer, 95.5);
        assert_eq!(index, 99.2);
        assert_eq!(overall, 97.35);
    }

    #[tokio::test]
    async fn test_postgres_performance_assessment() {
        let mut stats = PostgresPerformanceStats { performance_score: 95.0, ..Default::default() };

        assert_eq!(stats.get_performance_assessment(), "Excellent");

        stats.performance_score = 85.0;
        assert_eq!(stats.get_performance_assessment(), "Good");

        stats.performance_score = 75.0;
        assert_eq!(stats.get_performance_assessment(), "Fair");

        stats.performance_score = 65.0;
        assert_eq!(stats.get_performance_assessment(), "Poor");

        stats.performance_score = 50.0;
        assert_eq!(stats.get_performance_assessment(), "Critical");
    }

    #[tokio::test]
    async fn test_postgres_performance_bottleneck_detection() {
        let mut stats = PostgresPerformanceStats { is_io_bound: true, is_cpu_bound: false, ..Default::default() };

        assert!(stats.get_bottleneck_analysis().contains("I/O"));

        stats.is_io_bound = false;
        stats.is_cpu_bound = true;
        assert!(stats.get_bottleneck_analysis().contains("CPU"));

        stats.is_io_bound = false;
        stats.is_cpu_bound = false;
        stats.total_temp_files = 200;
        assert!(stats.get_bottleneck_analysis().contains("Memory"));
    }

    #[tokio::test]
    async fn test_postgres_performance_thresholds() {
        let stats = PostgresPerformanceStats {
            buffer_cache_hit_ratio: 85.0,
            index_hit_ratio: 95.0,
            total_temp_files: 150,
            total_temp_bytes: 200_000_000,
            ..Default::default()
        };

        assert!(!stats.has_healthy_cache_ratios());
        assert!(stats.has_excessive_temp_usage());
    }
}
