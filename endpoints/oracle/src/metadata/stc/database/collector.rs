use super::*;
use function_name::named;
impl OracleDatabaseStats {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut database_stats = OracleDatabaseStats::default();
        let requests = self.request();

        database_stats.collection_timestamp = DateTimeWrapper::from(Utc::now());

        let instance_info_rows = run_named_query(&requests, "instance_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = instance_info_rows.first() {
            database_stats.instance_id = row.get_i32("instance_id")?;
            database_stats.database_name = row.get_string("database_name")?;
            database_stats.db_unique_name = row.get_string("db_unique_name")?;
            database_stats.database_role = row.get_string("database_role")?;
            database_stats.database_status = row.get_string("database_status")?;
            database_stats.instance_status = row.get_string("instance_status")?;
            database_stats.startup_time = row.get_datetime("startup_time")?;
            database_stats.uptime_seconds = row.get_f64("uptime_seconds")?;
            database_stats.version = row.get_string("version")?;
            database_stats.host_name = row.get_string("host_name")?;
        }

        let cache_ratios_rows = run_named_query(&requests, "cache_hit_ratios", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = cache_ratios_rows.first() {
            database_stats.buffer_cache_hit_ratio = row.get_f64("buffer_cache_hit_ratio")?;
            database_stats.library_cache_hit_ratio = row.get_f64("library_cache_hit_ratio")?;
            database_stats.dictionary_cache_hit_ratio = row.get_f64("dictionary_cache_hit_ratio")?;
        }

        let parse_ratios_rows = run_named_query(&requests, "parse_ratios", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = parse_ratios_rows.first() {
            database_stats.soft_parse_ratio = row.get_f64("soft_parse_ratio")?;
            database_stats.execute_to_parse_ratio = row.get_f64("execute_to_parse_ratio")?;
        }

        let io_stats_rows = run_named_query(&requests, "io_statistics", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = io_stats_rows.first() {
            database_stats.physical_reads_per_sec = row.get_f64("physical_reads_per_sec")?;
            database_stats.physical_writes_per_sec = row.get_f64("physical_writes_per_sec")?;
            database_stats.logical_reads_per_sec = row.get_f64("logical_reads_per_sec")?;
            database_stats.block_changes_per_sec = row.get_f64("block_changes_per_sec")?;
            database_stats.redo_size_per_sec = row.get_f64("redo_size_per_sec")?;
            database_stats.user_calls_per_sec = row.get_f64("user_calls_per_sec")?;
            database_stats.transactions_per_sec = row.get_f64("transactions_per_sec")?;
            database_stats.executions_per_sec = row.get_f64("executions_per_sec")?;
        }

        let transaction_stats_rows = run_named_query(&requests, "transaction_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = transaction_stats_rows.first() {
            database_stats.user_commits = row.get_u64("user_commits")?;
            database_stats.user_rollbacks = row.get_u64("user_rollbacks")?;
            database_stats.user_transaction_rate = row.get_f64("user_transaction_rate")?;
            database_stats.user_commit_percentage = row.get_f64("user_commit_percentage")?;
        }

        let session_process_rows = run_named_query(&requests, "session_process_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = session_process_rows.first() {
            database_stats.current_sessions = row.get_u64("current_sessions")?;
            database_stats.current_processes = row.get_u64("current_processes")?;
            database_stats.peak_sessions = row.get_u64("peak_sessions")?;
            database_stats.peak_processes = row.get_u64("peak_processes")?;
        }

        let memory_stats_rows = run_named_query(&requests, "memory_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = memory_stats_rows.first() {
            database_stats.sga_size = row.get_u64("sga_size")?;
            database_stats.pga_aggregate_target = row.get_u64("pga_aggregate_target")?;
            database_stats.pga_used = row.get_u64("pga_used")?;
            database_stats.shared_pool_size = row.get_u64("shared_pool_size")?;
            database_stats.buffer_cache_size = row.get_u64("buffer_cache_size")?;
            database_stats.log_buffer_size = row.get_u64("log_buffer_size")?;
        }

        // Database size and tablespace counts require DBA views
        if capabilities.has(&crate::metadata::capabilities::ORACLE_HAS_DBA_VIEWS) {
            let database_size_rows = run_named_query(&requests, "database_size", context.clone(), Self::QUERY_TIMEOUT).await?;
            if let Some(row) = database_size_rows.first() {
                database_stats.database_size = row.get_u64("database_size")?;
                database_stats.used_space = row.get_u64("used_space")?;
                database_stats.free_space = row.get_u64("free_space")?;
            }

            let tablespace_counts_rows = run_named_query(&requests, "tablespace_counts", context.clone(), Self::QUERY_TIMEOUT).await?;
            if let Some(row) = tablespace_counts_rows.first() {
                database_stats.tablespace_count = row.get_u64("tablespace_count")?;
                database_stats.datafile_count = row.get_u64("datafile_count")?;
                database_stats.controlfile_count = row.get_u64("controlfile_count")?;
                database_stats.redo_log_groups = row.get_u64("redo_log_groups")?;
            }
        }

        let cpu_stats_rows = run_named_query(&requests, "cpu_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = cpu_stats_rows.first() {
            database_stats.cpu_usage_percentage = row.get_f64("cpu_usage_percentage")?;
            database_stats.db_cpu_time = row.get_u64("db_cpu_time")?;
            database_stats.background_cpu_time = row.get_u64("background_cpu_time")?;
            database_stats.parse_cpu_time = row.get_u64("parse_cpu_time")?;
        }

        let archive_stats_rows = run_named_query(&requests, "archive_log_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = archive_stats_rows.first() {
            database_stats.archive_log_rate_mb_per_hour = row.get_f64("archive_log_rate_mb_per_hour")?;
            database_stats.archive_logs_today = row.get_u64("archive_logs_today")?;
            database_stats.avg_archive_log_size = row.get_u64("avg_archive_log_size")?;
        }

        let response_time_rows = run_named_query(&requests, "response_time_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = response_time_rows.first() {
            database_stats.response_time_per_txn = row.get_f64("response_time_per_txn")?;
            database_stats.sql_service_response_time = row.get_f64("sql_service_response_time")?;
            database_stats.database_time_per_sec = row.get_f64("database_time_per_sec")?;
            database_stats.background_time_per_sec = row.get_f64("background_time_per_sec")?;
        }

        let wait_events_rows = run_named_query(&requests, "top_wait_events", context.clone(), Self::QUERY_TIMEOUT).await?;
        database_stats.top_wait_events = Self::parse_wait_events(wait_events_rows)?;

        database_stats.calculate_derived_metrics();

        Ok(database_stats)
    }

    fn parse_wait_events(rows: Vec<Row>) -> ResultEP<Vec<OracleWaitEventStats>> {
        let mut events = Vec::with_capacity(rows.len());

        for row in rows {
            events.push(OracleWaitEventStats {
                event: row.get_string("event")?,
                wait_class: row.get_string("wait_class")?,
                total_waits: row.get_u64("total_waits")?,
                total_timeouts: row.get_u64("total_timeouts")?,
                time_waited: row.get_f64("time_waited")?,
                average_wait: row.get_f64("average_wait")?,
                pct_of_total_time: row.get_f64("pct_of_total_time")?,
            });
        }

        Ok(events)
    }

    fn calculate_derived_metrics(&mut self) {
        if self.database_size > 0 {
            self.data_dict_cache_hit_ratio = (self.used_space as f64 / self.database_size as f64) * 100.0;
        }

        // Growth rate approximated from uptime; no historical data available
        if self.uptime_seconds > 0.0 {
            let days_running = self.uptime_seconds / 86400.0;
            if days_running > 1.0 {
                self.growth_rate_per_day = self.used_space as f64 / days_running;
            }
        }
    }
}
