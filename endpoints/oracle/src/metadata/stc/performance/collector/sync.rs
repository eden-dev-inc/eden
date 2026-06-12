use super::*;
use function_name::named;
impl OraclePerformanceStatsCollection {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(45);

    pub fn new() -> Self {
        Self::default()
    }

    #[named]
    pub async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let start_time = Instant::now();
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut collection = OraclePerformanceStatsCollection::default();
        let requests = self.request();
        let mut metadata = CollectionMetadata::default();

        collection.collection_timestamp = DateTimeWrapper::from(Utc::now());

        let system_stats_rows = run_named_query(&requests, "system_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;
        collection.stats.system_stats = Self::process_system_stats(&system_stats_rows)?;

        let wait_events_rows = run_named_query(&requests, "wait_events", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;
        collection.stats.wait_events = Self::process_wait_events(&wait_events_rows)?;

        let sql_stats_rows = run_named_query(&requests, "sql_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;
        collection.stats.sql_performance = Self::process_sql_performance(&sql_stats_rows)?;

        let memory_stats_rows = run_named_query(&requests, "memory_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        let buffer_pool_rows = run_named_query(&requests, "buffer_pool_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        let library_cache_rows = run_named_query(&requests, "library_cache_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        collection.stats.memory_utilization = Self::process_memory_utilization(&memory_stats_rows, &buffer_pool_rows, &library_cache_rows)?;

        // File I/O and tablespace I/O require DBA views
        let has_dba = capabilities.has(&crate::metadata::capabilities::ORACLE_HAS_DBA_VIEWS);
        let (file_io_rows, tablespace_io_rows) = if has_dba {
            let file_io = run_named_query(&requests, "file_io_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
            metadata.queries_executed += 1;
            let tablespace_io = run_named_query(&requests, "tablespace_io", context.clone(), Self::QUERY_TIMEOUT).await?;
            metadata.queries_executed += 1;
            (file_io, tablespace_io)
        } else {
            (Vec::new(), Vec::new())
        };

        collection.stats.io_statistics = Self::process_io_statistics(&file_io_rows, &tablespace_io_rows, &collection.stats.wait_events)?;

        let session_stats_rows = run_named_query(&requests, "session_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        let blocking_sessions_rows = run_named_query(&requests, "blocking_sessions", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        collection.stats.session_statistics = Self::process_session_statistics(&session_stats_rows, &blocking_sessions_rows)?;

        let advisors_rows = run_named_query(&requests, "memory_advisors", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        collection.stats.memory_utilization.advisors = Self::process_memory_advisors(&advisors_rows)?;

        let workarea_rows = run_named_query(&requests, "workarea_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        metadata.queries_executed += 1;

        collection.stats.memory_utilization.pga_stats.workarea_memory = Self::process_workarea_stats(&workarea_rows)?;

        collection.stats.performance_analysis = Self::generate_performance_analysis(
            &collection.stats.system_stats,
            &collection.stats.wait_events,
            &collection.stats.sql_performance,
            &collection.stats.memory_utilization,
            &collection.stats.io_statistics,
            &collection.stats.session_statistics,
        )?;

        collection.recommendations = Self::generate_recommendations(&collection.stats)?;
        collection.stats.alerts = Self::generate_alerts(&collection.stats)?;
        collection.health_score = Self::calculate_health_score(&collection.stats)?;

        metadata.collection_duration_ms = start_time.elapsed().as_millis() as u64;
        metadata.data_quality_score = Self::calculate_data_quality(&collection.stats);
        collection.collection_metadata = metadata;

        collection.stats.collection_timestamp = DateTimeWrapper::from(Utc::now());

        Ok(collection)
    }
}
