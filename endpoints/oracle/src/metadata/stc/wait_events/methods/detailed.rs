use super::*;

impl OracleWaitEventInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleWaitEventInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleWaitEventDetailedMetrics>> {
        let needs_details = core_info.sessions_waiting > Self::HIGH_SESSION_WAIT_THRESHOLD
            || core_info.wait_time_percent > Self::HIGH_WAIT_TIME_THRESHOLD
            || core_info.io_wait_percentage() > 40.0
            || core_info.concurrency_wait_percentage() > 15.0;

        if !crate::metadata::stc::utils::should_collect(&[needs_details]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleWaitEventDetailedMetrics {
            top_wait_events: Vec::new(),
            wait_class_analysis: None,
            session_wait_analysis: None,
            wait_trends: None,
            io_wait_breakdown: None,
        };

        let top_events_input = crate::metadata::stc::utils::query_with_limit(
            "SELECT
                    event,
                    wait_class,
                    total_waits,
                    time_waited_micro as time_waited_us,
                    average_wait as avg_wait_us,
                    time_waited_micro as max_wait_us,
                    ROUND((time_waited_micro / NULLIF(SUM(time_waited_micro) OVER(), 0)) * 100, 2) as time_waited_percent,
                    ROUND(total_waits / GREATEST(1, EXTRACT(DAY FROM (SYSDATE - startup_time)) * 24 * 3600), 2) as waits_per_sec,
                    ROUND(average_wait / 1000, 2) as avg_wait_ms,
                    ROW_NUMBER() OVER (ORDER BY time_waited_micro DESC) as rank_by_time,
                    ROW_NUMBER() OVER (ORDER BY total_waits DESC) as rank_by_waits,
                    CASE
                        WHEN time_waited_micro > 0.4 * SUM(time_waited_micro) OVER() THEN 'CRITICAL'
                        WHEN time_waited_micro > 0.2 * SUM(time_waited_micro) OVER() THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as issue_severity
                FROM v$system_event, v$instance
                WHERE wait_class != 'Idle'
                   AND time_waited_micro > 0
                ORDER BY time_waited_micro DESC"
                .to_string(),
            Self::MAX_DETAILED_RESULTS,
        );

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.top_wait_events,
            &top_events_input,
            context.clone(),
            Self::QUERY_TIMEOUT,
            "top_wait_events",
            Self::parse_wait_event_details,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            core_info.sessions_waiting > 5,
            &mut detailed_metrics.session_wait_analysis,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        s.sid,
                        s.serial#,
                        s.username,
                        s.program,
                        s.machine,
                        sw.event as wait_event,
                        sw.wait_class,
                        sw.wait_time,
                        sw.seconds_in_wait,
                        sw.state,
                        sw.p1,
                        sw.p2,
                        sw.p3,
                        s.sql_id,
                        s.blocking_session
                    FROM v$session s
                    JOIN v$session_wait sw ON s.sid = sw.sid
                    WHERE s.type = 'USER'
                       AND sw.wait_class != 'Idle'
                       AND (sw.state = 'WAITING' OR sw.seconds_in_wait > 1)
                    ORDER BY sw.seconds_in_wait DESC, sw.wait_time DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "session_wait_analysis",
            Self::parse_session_wait_details,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
