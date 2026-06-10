use super::*;
use function_name::named;

impl OracleActivityInfo {
    const LONG_SQL_THRESHOLD: f64 = 30.0;
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut activity_info = OracleActivityInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "core_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            activity_info.active_sessions = row.get_u64("active_sessions")?;
            activity_info.inactive_sessions = row.get_u64("inactive_sessions")?;
            activity_info.killed_sessions = row.get_u64("killed_sessions")?;
            activity_info.total_sessions = row.get_u64("total_sessions")?;
            activity_info.max_sessions = row.get_u64("max_sessions")?;
            activity_info.longest_sql_duration = row.get_f64("longest_sql_duration")?;
            activity_info.longest_transaction_duration = row.get_f64("longest_transaction_duration")?;
            activity_info.avg_active_sql_duration = row.get_f64("avg_active_sql_duration")?;
            activity_info.waiting_sessions_count = row.get_u64("waiting_sessions_count")?;
            activity_info.session_utilization_pct = ratio_percentage(activity_info.total_sessions, activity_info.max_sessions);
        }

        if let Some(row) = run_single_row(&requests, "blocking_count", context.clone(), Self::QUERY_TIMEOUT).await? {
            activity_info.blocking_sessions_count = row.get_u64("blocking_count")?;
        }

        if let Some(row) = run_single_row(&requests, "system_resources", context.clone(), Self::QUERY_TIMEOUT).await? {
            activity_info.parallel_servers_active = row.get_u64("parallel_servers_active")?;
            activity_info.parallel_servers_max = row.get_u64("parallel_servers_max")?;
            activity_info.current_pga_used = row.get_u64("current_pga_used")?;
            activity_info.pga_aggregate_limit = row.get_u64("pga_aggregate_limit")?;
            activity_info.sga_size = row.get_u64("sga_size")?;
            activity_info.process_count = row.get_u64("process_count")?;
            activity_info.process_limit = row.get_u64("process_limit")?;
        }

        activity_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&activity_info, context).await?;

        Ok(activity_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &OracleActivityInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleDetailedMetrics>> {
        let needs_long_sql_details = core_info.longest_sql_duration > Self::LONG_SQL_THRESHOLD;
        let needs_blocking_details = core_info.blocking_sessions_count > 0;
        let needs_performance_details = core_info.session_utilization_pct > 80.0 || core_info.waiting_sessions_count > 10;

        if !crate::metadata::stc::utils::should_collect(&[needs_long_sql_details, needs_blocking_details, needs_performance_details]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleDetailedMetrics {
            long_running_sql: Vec::new(),
            blocked_sessions: Vec::new(),
            sessions_by_schema: None,
            top_wait_events: None,
        };

        crate::metadata::stc::utils::assign_optional_vec_if(
            needs_long_sql_details,
            &mut detailed_metrics.long_running_sql,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    format!(
                        "SELECT
                    s.sid, s.serial#, s.username, s.schemaname,
                    SUBSTR(sq.sql_text, 1, 500) as sql_text,
                    (SYSDATE - s.sql_exec_start) * 86400 as duration,
                    s.status, s.program, s.machine, s.osuser,
                    s.sql_exec_start, s.last_call_et,
                    s.blocking_session, s.event, s.wait_class,
                    s.sql_id, s.sql_child_number
                FROM v$session s
                LEFT JOIN v$sql sq ON s.sql_id = sq.sql_id AND s.sql_child_number = sq.child_number
                WHERE s.type = 'USER'
                    AND s.status = 'ACTIVE'
                    AND s.sql_exec_start IS NOT NULL
                    AND (SYSDATE - s.sql_exec_start) * 86400 > {}
                ORDER BY s.sql_exec_start ASC",
                        Self::LONG_SQL_THRESHOLD,
                    ),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "long_running_sql",
            Self::parse_long_running_sql,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_vec_if(
            needs_blocking_details,
            &mut detailed_metrics.blocked_sessions,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    blocked.sid as blocked_sid,
                    blocked.serial# as blocked_serial,
                    blocked.username as blocked_username,
                    blocker.sid as blocking_sid,
                    blocker.serial# as blocking_serial,
                    blocker.username as blocking_username,
                    SUBSTR(blocked_sql.sql_text, 1, 300) as blocked_sql_text,
                    SUBSTR(blocker_sql.sql_text, 1, 300) as blocking_sql_text,
                    w.lock_type, w.mode_held, w.mode_requested,
                    (SYSDATE - blocked.sql_exec_start) * 86400 as blocked_duration,
                    blocked.schemaname as schema_name,
                    o.object_name, o.object_type,
                    blocked.event as wait_event,
                    blocked.seconds_in_wait
                FROM v$session blocked
                JOIN v$session blocker ON blocked.blocking_session = blocker.sid
                LEFT JOIN v$sql blocked_sql ON blocked.sql_id = blocked_sql.sql_id
                LEFT JOIN v$sql blocker_sql ON blocker.sql_id = blocker_sql.sql_id
                LEFT JOIN dba_waiters w ON blocked.sid = w.waiting_session
                LEFT JOIN dba_objects o ON w.object_id = o.object_id
                WHERE blocked.blocking_session IS NOT NULL
                    AND blocked.blocking_session_status = 'VALID'
                ORDER BY blocked_duration DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "blocked_sessions",
            Self::parse_blocked_sessions,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_performance_details,
            &mut detailed_metrics.top_wait_events,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    event, wait_class, total_waits, total_timeouts,
                    time_waited, average_wait, time_waited_fg,
                    ROUND((time_waited / SUM(time_waited) OVER()) * 100, 2) as pct_of_total_time
                FROM v$system_event
                WHERE wait_class != 'Idle'
                    AND time_waited > 0
                ORDER BY time_waited DESC"
                        .to_string(),
                    20,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "wait_events",
            Self::parse_wait_events,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }

    fn parse_long_running_sql(rows: Vec<Row>) -> ResultEP<Vec<OracleActiveSql>> {
        map_rows(rows, |row| {
            Ok(OracleActiveSql {
                sid: row.get_i32("sid")?,
                serial_number: row.get_i32("serial#")?,
                username: row.get_string("username")?,
                schema_name: row.get_string("schemaname")?,
                sql_text: row.get_string("sql_text")?,
                duration: row.get_f64("duration")?,
                status: row.get_string("status")?,
                program: row.get_opt_string("program")?,
                machine: row.get_opt_string("machine")?,
                os_user: row.get_opt_string("osuser")?,
                sql_exec_start: row.get_datetime("sql_exec_start")?,
                last_call_et: row.get_i32("last_call_et")?,
                blocking_session: row.get_opt_i32("blocking_session")?,
                event: row.get_opt_string("event")?,
                wait_class: row.get_opt_string("wait_class")?,
                sql_id: row.get_opt_string("sql_id")?,
                sql_child_number: row.get_opt_i32("sql_child_number")?,
            })
        })
    }

    fn parse_blocked_sessions(rows: Vec<Row>) -> ResultEP<Vec<OracleBlockedSession>> {
        map_rows(rows, |row| {
            Ok(OracleBlockedSession {
                blocked_sid: row.get_i32("blocked_sid")?,
                blocked_serial: row.get_i32("blocked_serial")?,
                blocked_username: row.get_string("blocked_username")?,
                blocking_sid: row.get_i32("blocking_sid")?,
                blocking_serial: row.get_i32("blocking_serial")?,
                blocking_username: row.get_string("blocking_username")?,
                blocked_sql_text: row.get_string("blocked_sql_text")?,
                blocking_sql_text: row.get_string("blocking_sql_text")?,
                lock_type: row.get_opt_string("lock_type")?,
                mode_held: row.get_opt_string("mode_held")?,
                mode_requested: row.get_opt_string("mode_requested")?,
                blocked_duration: row.get_f64("blocked_duration")?,
                schema_name: row.get_string("schema_name")?,
                object_name: row.get_opt_string("object_name")?,
                object_type: row.get_opt_string("object_type")?,
                wait_event: row.get_opt_string("wait_event")?,
                seconds_in_wait: row.get_opt_i32("seconds_in_wait")?,
            })
        })
    }

    fn parse_wait_events(rows: Vec<Row>) -> ResultEP<Vec<OracleWaitEvent>> {
        map_rows(rows, |row| {
            Ok(OracleWaitEvent {
                event: row.get_string("event")?,
                wait_class: row.get_string("wait_class")?,
                total_waits: row.get_u64("total_waits")?,
                total_timeouts: row.get_u64("total_timeouts")?,
                time_waited: row.get_f64("time_waited")?,
                average_wait: row.get_f64("average_wait")?,
                time_waited_fg: row.get_f64("time_waited_fg")?,
                pct_of_total_time: row.get_f64("pct_of_total_time")?,
            })
        })
    }
}
