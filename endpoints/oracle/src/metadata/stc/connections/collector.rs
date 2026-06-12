use super::*;
use function_name::named;
impl OracleConnectionInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut connection_info = OracleConnectionInfo::default();
        let requests = self.request();

        let session_stats_rows = run_named_query(&requests, "session_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = session_stats_rows.first() {
            connection_info.current_user_sessions = row.get_u64("current_user_sessions")?;
            connection_info.current_background_sessions = row.get_u64("current_background_sessions")?;
            connection_info.current_recursive_sessions = row.get_u64("current_recursive_sessions")?;
            connection_info.total_active_sessions = row.get_u64("total_active_sessions")?;
            connection_info.max_sessions = row.get_u64("max_sessions")?;
            connection_info.max_processes = row.get_u64("max_processes")?;
            connection_info.current_processes = row.get_u64("current_processes")?;
            connection_info.sessions_waiting = row.get_u64("sessions_waiting")?;
            connection_info.sessions_blocking = row.get_u64("sessions_blocking")?;

            connection_info.session_utilization_pct = ratio_percentage(
                connection_info.current_user_sessions
                    + connection_info.current_background_sessions
                    + connection_info.current_recursive_sessions,
                connection_info.max_sessions,
            );
            connection_info.process_utilization_pct = ratio_percentage(connection_info.current_processes, connection_info.max_processes);
        }

        let memory_stats_rows = run_named_query(&requests, "memory_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = memory_stats_rows.first() {
            connection_info.avg_session_pga = row.get_u64("avg_session_pga")?;
            connection_info.total_pga_allocated = row.get_u64("total_pga_allocated")?;
            connection_info.pga_aggregate_limit = row.get_u64("pga_aggregate_limit")?;
            connection_info.pga_over_allocation_count = row.get_u64("pga_over_allocation_count")?;
        }

        let sga_stats_rows = run_named_query(&requests, "sga_stats", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = sga_stats_rows.first() {
            connection_info.shared_pool_size = row.get_u64("shared_pool_size")?;
            connection_info.shared_pool_free = row.get_u64("shared_pool_free")?;
            connection_info.buffer_cache_size = row.get_u64("buffer_cache_size")?;
        }

        let connections_by_service_rows =
            run_named_query(&requests, "connections_by_service", context.clone(), Self::QUERY_TIMEOUT).await?;
        connection_info.connections_by_service = Self::parse_connections_by_service(connections_by_service_rows)?;

        let connections_by_machine_rows =
            run_named_query(&requests, "connections_by_machine", context.clone(), Self::QUERY_TIMEOUT).await?;
        connection_info.connections_by_machine = Self::parse_connections_by_machine(connections_by_machine_rows)?;

        let session_breakdown_rows = run_named_query(&requests, "session_breakdown", context.clone(), Self::QUERY_TIMEOUT).await?;
        connection_info.session_breakdown = Self::parse_session_breakdown(session_breakdown_rows)?;

        connection_info.connection_pool_stats = Self::collect_connection_pool_stats(context).await.ok();

        Ok(connection_info)
    }

    /// Collect DRCP/UCP pool stats if available. Not all Oracle environments expose these.
    async fn collect_connection_pool_stats(context: OracleAsync) -> ResultEP<OracleConnectionPoolStats> {
        let pool_query = crate::metadata::stc::utils::query(
            "SELECT
                pool_name,
                active_conn_count,
                idle_conn_count,
                busy_conn_count,
                max_conn_count,
                min_conn_count,
                initial_conn_count,
                incr_conn_count,
                decr_conn_count,
                num_requests,
                num_hits,
                num_misses
            FROM v$cpool_stats
            WHERE pool_name IS NOT NULL"
                .to_string(),
        );

        let rows = run_query_with_timeout(&pool_query, context, Self::QUERY_TIMEOUT, "connection_pool_stats").await?;

        if let Some(row) = rows.first() {
            Ok(OracleConnectionPoolStats {
                pool_name: row.get_string("pool_name")?,
                active_connections: row.get_u64("active_conn_count")?,
                idle_connections: row.get_u64("idle_conn_count")?,
                busy_connections: row.get_u64("busy_conn_count")?,
                max_connections: row.get_u64("max_conn_count")?,
                min_connections: row.get_u64("min_conn_count")?,
                initial_connections: row.get_u64("initial_conn_count")?,
                increment_connections: row.get_u64("incr_conn_count")?,
                decrement_connections: row.get_u64("decr_conn_count")?,
                total_requests: row.get_u64("num_requests")?,
                cache_hits: row.get_u64("num_hits")?,
                cache_misses: row.get_u64("num_misses")?,
                hit_ratio: ratio_percentage(row.get_u64("num_hits")?, row.get_u64("num_requests")?),
            })
        } else {
            Err(EpError::metadata("No connection pool statistics available"))
        }
    }

    fn parse_connections_by_service(rows: Vec<Row>) -> ResultEP<Vec<OracleConnectionsByService>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(OracleConnectionsByService {
                service_name: row.get_string("service_name")?,
                total_connections: row.get_u64("total_connections")?,
                active_connections: row.get_u64("active_connections")?,
                inactive_connections: row.get_u64("inactive_connections")?,
                killed_connections: row.get_u64("killed_connections")?,
                avg_pga_per_connection: row.get_u64("avg_pga_per_connection")?,
                longest_idle_time: row.get_i32("longest_idle_time")?,
            });
        }

        Ok(connections)
    }

    fn parse_connections_by_machine(rows: Vec<Row>) -> ResultEP<Vec<OracleConnectionsByMachine>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(OracleConnectionsByMachine {
                machine_name: row.get_string("machine_name")?,
                total_connections: row.get_u64("total_connections")?,
                active_connections: row.get_u64("active_connections")?,
                inactive_connections: row.get_u64("inactive_connections")?,
                unique_users: row.get_u64("unique_users")?,
                avg_pga_per_connection: row.get_u64("avg_pga_per_connection")?,
                earliest_logon: row.get_datetime("earliest_logon")?,
                latest_logon: row.get_datetime("latest_logon")?,
            });
        }

        Ok(connections)
    }

    fn parse_session_breakdown(rows: Vec<Row>) -> ResultEP<OracleSessionBreakdown> {
        let mut breakdown = OracleSessionBreakdown {
            active_sessions: OracleSessionStats::default(),
            inactive_sessions: OracleSessionStats::default(),
            killed_sessions: OracleSessionStats::default(),
            cached_sessions: OracleSessionStats::default(),
        };

        for row in rows {
            let status = row.get_string("status")?;
            let session_count = row.get_u64("session_count")?;
            let avg_pga_memory = row.get_u64("avg_pga_memory")?;
            let avg_idle_time = row.get_f64("avg_idle_time")?;
            let max_idle_time = row.get_f64("max_idle_time")?;
            let blocked_sessions = row.get_u64("blocked_sessions")?;
            let blocking_sessions = row.get_u64("blocking_sessions")?;

            let stats = OracleSessionStats {
                session_count,
                avg_pga_memory,
                avg_idle_time,
                max_idle_time,
                blocked_sessions,
                blocking_sessions,
            };

            match status.to_uppercase().as_str() {
                "ACTIVE" => breakdown.active_sessions = stats,
                "INACTIVE" => breakdown.inactive_sessions = stats,
                "KILLED" => breakdown.killed_sessions = stats,
                "CACHED" => breakdown.cached_sessions = stats,
                _ => {} // Unknown status, skip
            }
        }

        Ok(breakdown)
    }
}
