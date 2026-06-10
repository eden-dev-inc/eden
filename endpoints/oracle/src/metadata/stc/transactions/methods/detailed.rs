use super::*;

impl OracleTransactionInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleTransactionInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleTransactionDetailedMetrics>> {
        let needs_details = core_info.long_running_transactions > 0
            || core_info.blocking_sessions > 0
            || core_info.deadlocks_detected > 0
            || core_info.rollback_percentage() > 10.0;

        if !crate::metadata::stc::utils::should_collect(&[needs_details]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleTransactionDetailedMetrics {
            problem_transactions: Vec::new(),
            lock_analysis: None,
            session_analysis: None,
            undo_analysis: None,
            deadlock_analysis: None,
        };

        crate::metadata::stc::utils::assign_optional_vec_if(
            core_info.long_running_transactions > 0,
            &mut detailed_metrics.problem_transactions,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        s.sid,
                        s.serial#,
                        s.username,
                        s.program,
                        s.machine,
                        TO_CHAR(t.start_date, 'YYYY-MM-DD HH24:MI:SS') as start_time,
                        ROUND((SYSDATE - t.start_date) * 24 * 60 * 60) as duration_seconds,
                        s.status,
                        s.sql_id,
                        SUBSTR(sq.sql_text, 1, 100) as sql_text,
                        t.used_ublk as undo_blocks,
                        t.used_urec as undo_records,
                        'LOCAL' as transaction_type,
                        CASE WHEN s.blocking_session IS NOT NULL THEN 'YES' ELSE 'NO' END as lock_wait,
                        s.blocking_session,
                        CASE
                            WHEN (SYSDATE - t.start_date) * 24 * 60 > 120 THEN 'CRITICAL'
                            WHEN (SYSDATE - t.start_date) * 24 * 60 > 30 THEN 'WARNING'
                            ELSE 'NORMAL'
                        END as issue_severity
                    FROM v$transaction t
                    JOIN v$session s ON t.ses_addr = s.saddr
                    LEFT JOIN v$sql sq ON s.sql_id = sq.sql_id
                    WHERE s.type = 'USER'
                       AND (SYSDATE - t.start_date) * 24 * 60 > 30
                    ORDER BY duration_seconds DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "problem_transactions",
            Self::parse_transaction_details,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            core_info.blocking_sessions > 0,
            &mut detailed_metrics.lock_analysis,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        l1.sid as holding_sid,
                        l2.sid as waiting_sid,
                        l1.type as lock_type,
                        l1.lmode as mode_held,
                        l2.request as mode_requested,
                        o.object_name,
                        o.object_type,
                        NVL(w.seconds_in_wait, 0) as wait_time_seconds,
                        s1.sql_id as blocking_sql_id,
                        s2.sql_id as waiting_sql_id,
                        TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') as request_time
                    FROM v$lock l1
                    JOIN v$lock l2 ON l1.id1 = l2.id1 AND l1.id2 = l2.id2
                    JOIN v$session s1 ON l1.sid = s1.sid
                    JOIN v$session s2 ON l2.sid = s2.sid
                    LEFT JOIN dba_objects o ON l1.id1 = o.object_id
                    LEFT JOIN v$session_wait w ON l2.sid = w.sid
                    WHERE l1.lmode > 0 AND l2.request > 0
                       AND l1.sid != l2.sid
                    ORDER BY wait_time_seconds DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "lock_analysis",
            Self::parse_lock_details,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
