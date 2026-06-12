use super::*;

impl OracleSessionInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleSessionInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleSessionDetailedMetrics>> {
        let needs_resource_details = core_info.high_pga_sessions > 5 || core_info.sessions_using_temp > 10;
        let needs_long_session_details = core_info.longest_session_duration > Self::LONG_SESSION_THRESHOLD;
        let needs_blocking_details = core_info.sessions_waiting_for_locks > 0;
        let needs_security_details = core_info.failed_logins_last_hour > 5;
        let needs_analysis_details = core_info.active_user_sessions > Self::HIGH_SESSION_COUNT_THRESHOLD;

        if !crate::metadata::stc::utils::should_collect(&[
            needs_resource_details,
            needs_long_session_details,
            needs_blocking_details,
            needs_security_details,
            needs_analysis_details,
        ]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleSessionDetailedMetrics {
            resource_intensive_sessions: Vec::new(),
            long_running_sessions: None,
            blocked_sessions: None,
            failed_login_attempts: None,
            user_session_stats: None,
            program_session_stats: None,
        };

        let resource_sessions_query = crate::metadata::stc::utils::query_with_limit(
            format!(
                "SELECT
                s.sid,
                s.serial#,
                s.username,
                s.program,
                s.machine,
                s.osuser,
                s.status,
                ROUND((SYSDATE - s.logon_time) * 86400, 0) as session_duration,
                p.pga_used_mem,
                p.pga_alloc_mem,
                p.pga_freeable_mem,
                ss.value as temp_space_used,
                s.sql_id,
                s.event,
                s.wait_class,
                s.seconds_in_wait,
                s.blocking_session
            FROM v$session s
            LEFT JOIN v$process p ON s.paddr = p.addr
            LEFT JOIN v$sesstat ss ON s.sid = ss.sid AND ss.statistic# = (
                SELECT statistic# FROM v$statname WHERE name = 'session uga memory max'
            )
            WHERE s.type = 'USER'
               AND (p.pga_used_mem > {} OR ss.value > {} OR s.status = 'ACTIVE')
            ORDER BY NVL(p.pga_used_mem, 0) + NVL(ss.value, 0) DESC",
                Self::HIGH_PGA_THRESHOLD,
                Self::HIGH_TEMP_THRESHOLD,
            ),
            Self::MAX_DETAILED_RESULTS,
        );

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.resource_intensive_sessions,
            &resource_sessions_query,
            context.clone(),
            Self::QUERY_TIMEOUT,
            "resource_sessions",
            Self::parse_resource_sessions,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_long_session_details,
            &mut detailed_metrics.long_running_sessions,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    format!(
                        "SELECT
                    s.sid,
                    s.serial#,
                    s.username,
                    s.program,
                    s.machine,
                    s.osuser,
                    s.status,
                    s.logon_time,
                    ROUND((SYSDATE - s.logon_time) * 86400, 0) as session_duration,
                    s.last_call_et,
                    s.sql_id,
                    SUBSTR(sq.sql_text, 1, 200) as sql_text,
                    s.event,
                    s.wait_class
                FROM v$session s
                LEFT JOIN v$sql sq ON s.sql_id = sq.sql_id
                WHERE s.type = 'USER'
                   AND s.logon_time IS NOT NULL
                   AND (SYSDATE - s.logon_time) * 86400 > {}
                ORDER BY s.logon_time ASC",
                        Self::LONG_SESSION_THRESHOLD,
                    ),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "long_sessions",
            Self::parse_long_sessions,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_blocking_details,
            &mut detailed_metrics.blocked_sessions,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    blocked.sid as blocked_sid,
                    blocked.serial# as blocked_serial,
                    blocked.username as blocked_username,
                    blocked.program as blocked_program,
                    blocker.sid as blocking_sid,
                    blocker.serial# as blocking_serial,
                    blocker.username as blocking_username,
                    blocker.program as blocking_program,
                    blocked.event as wait_event,
                    blocked.seconds_in_wait,
                    blocked.sql_id as blocked_sql_id,
                    blocker.sql_id as blocking_sql_id,
                    SUBSTR(blocked_sql.sql_text, 1, 200) as blocked_sql_text,
                    SUBSTR(blocker_sql.sql_text, 1, 200) as blocking_sql_text
                FROM v$session blocked
                JOIN v$session blocker ON blocked.blocking_session = blocker.sid
                LEFT JOIN v$sql blocked_sql ON blocked.sql_id = blocked_sql.sql_id
                LEFT JOIN v$sql blocker_sql ON blocker.sql_id = blocker_sql.sql_id
                WHERE blocked.blocking_session IS NOT NULL
                ORDER BY blocked.seconds_in_wait DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "blocked_sessions",
            Self::parse_blocked_session_details,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_security_details,
            &mut detailed_metrics.failed_login_attempts,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    username,
                    terminal,
                    timestamp,
                    returncode,
                    client_id,
                    COUNT(*) as attempt_count
                FROM dba_audit_trail
                WHERE action_name = 'LOGON'
                  AND returncode != 0
                  AND timestamp >= SYSDATE - 1/24
                GROUP BY username, terminal, timestamp, returncode, client_id
                ORDER BY timestamp DESC, attempt_count DESC"
                        .to_string(),
                    50,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "failed_logins",
            Self::parse_failed_logins,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_analysis_details,
            &mut detailed_metrics.user_session_stats,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    username,
                    COUNT(*) as session_count,
                    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_count,
                    COUNT(CASE WHEN status = 'INACTIVE' THEN 1 END) as inactive_count,
                    AVG(CASE WHEN logon_time IS NOT NULL
                        THEN (SYSDATE - logon_time) * 86400 ELSE NULL END) as avg_duration,
                    MAX(CASE WHEN logon_time IS NOT NULL
                        THEN (SYSDATE - logon_time) * 86400 ELSE 0 END) as max_duration
                FROM v$session
                WHERE type = 'USER' AND username IS NOT NULL
                GROUP BY username
                ORDER BY session_count DESC"
                        .to_string(),
                    50,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "user_stats",
            Self::parse_user_session_stats,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_analysis_details,
            &mut detailed_metrics.program_session_stats,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    program,
                    COUNT(*) as session_count,
                    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_count,
                    COUNT(DISTINCT username) as unique_users,
                    COUNT(DISTINCT machine) as unique_machines
                FROM v$session
                WHERE type = 'USER' AND program IS NOT NULL
                GROUP BY program
                ORDER BY session_count DESC"
                        .to_string(),
                    30,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "program_stats",
            Self::parse_program_session_stats,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
