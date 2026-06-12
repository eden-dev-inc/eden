use super::*;
use function_name::named;
impl OracleSessionInfo {
    pub(crate) const LONG_SESSION_THRESHOLD: f64 = 28800.0; // 8 hours in seconds
    pub(crate) const HIGH_PGA_THRESHOLD: u64 = 104857600; // 100MB
    pub(crate) const HIGH_TEMP_THRESHOLD: u64 = 104857600; // 100MB
    pub(crate) const HIGH_SESSION_COUNT_THRESHOLD: u64 = 100; // Many active sessions
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(8);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut session_info = OracleSessionInfo::default();
        let requests = self.request();

        let session_summary_rows = run_named_query(&requests, "session_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        if let Some(row) = session_summary_rows.first() {
            session_info.total_user_sessions = row.get_u64("total_user_sessions")?;
            session_info.active_user_sessions = row.get_u64("active_user_sessions")?;
            session_info.inactive_user_sessions = row.get_u64("inactive_user_sessions")?;
            session_info.killed_sessions = row.get_u64("killed_sessions")?;
            session_info.cached_sessions = row.get_u64("cached_sessions")?;
            session_info.background_processes = row.get_u64("background_processes")?;
            session_info.max_sessions = row.get_u64("max_sessions")?;
            session_info.unique_users = row.get_u64("unique_users")?;
            session_info.unique_programs = row.get_u64("unique_programs")?;
            session_info.unique_machines = row.get_u64("unique_machines")?;
            session_info.avg_session_duration = row.get_f64("avg_session_duration")?;
            session_info.longest_session_duration = row.get_f64("longest_session_duration")?;
            session_info.sessions_waiting_for_locks = row.get_u64("sessions_waiting_for_locks")?;

            session_info.session_utilization_pct =
                ratio_percentage(session_info.total_user_sessions + session_info.background_processes, session_info.max_sessions);
        }

        let connection_rows = run_named_query(&requests, "connection_activity", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = connection_rows.first() {
            session_info.new_sessions_last_hour = row.get_u64("new_sessions_last_hour")?;
            session_info.total_logons_since_startup = row.get_u64("total_logons_since_startup")?;
            session_info.dedicated_connections = row.get_u64("dedicated_connections")?;
            session_info.shared_connections = row.get_u64("shared_connections")?;
        }

        let resource_rows = run_named_query(&requests, "resource_usage", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = resource_rows.first() {
            session_info.sessions_using_temp = row.get_u64("sessions_using_temp")?;
            session_info.total_temp_space_used = row.get_u64("total_temp_space_used")?;
            session_info.high_pga_sessions = row.get_u64("high_pga_sessions")?;
            session_info.total_pga_used = row.get_u64("total_pga_used")?;
        }

        // Security metrics and session history require DBA views (dba_audit_trail)
        if capabilities.has(&crate::metadata::capabilities::ORACLE_HAS_DBA_VIEWS) {
            let security_rows = run_named_query(&requests, "security_metrics", context.clone(), Self::QUERY_TIMEOUT).await?;
            if let Some(row) = security_rows.first() {
                session_info.failed_logins_last_hour = row.get_u64("failed_logins_last_hour")?;
            }

            let history_rows = run_named_query(&requests, "session_counts_history", context.clone(), Self::QUERY_TIMEOUT).await?;
            if let Some(row) = history_rows.first() {
                session_info.disconnected_sessions_last_hour = row.get_u64("disconnected_sessions_last_hour")?;
            }
        }

        // Conditionally collect detailed metrics only when problems are detected
        session_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&session_info, context).await?;

        Ok(session_info)
    }
}
