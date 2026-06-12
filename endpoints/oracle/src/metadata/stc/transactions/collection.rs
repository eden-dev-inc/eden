use super::*;
impl MetadataCollection for OracleTransactionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "transaction_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as active_transactions,
                    COUNT(CASE WHEN (SYSDATE - t.start_date) * 24 * 60 > 30 THEN 1 END) as long_running_transactions,
                    COUNT(CASE WHEN (SYSDATE - t.start_date) * 24 * 60 > 120 THEN 1 END) as very_long_transactions,
                    NVL(AVG((SYSDATE - t.start_date) * 24 * 60 * 60), 0) as avg_transaction_duration,
                    NVL(MAX((SYSDATE - t.start_date) * 24 * 60 * 60), 0) as max_transaction_duration
                FROM v$transaction t
                JOIN v$session s ON t.ses_addr = s.saddr
                WHERE s.type = 'USER'"
                        .to_string(),
                ),
            ),
            (
                "commit_rollback_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(CASE WHEN name = 'user commits' THEN value ELSE 0 END) as user_commits,
                    SUM(CASE WHEN name = 'user rollbacks' THEN value ELSE 0 END) as user_rollbacks
                FROM v$sysstat
                WHERE name IN ('user commits', 'user rollbacks')"
                        .to_string(),
                ),
            ),
            (
                "session_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as active_sessions,
                    COUNT(CASE WHEN blocking_session IS NOT NULL THEN 1 END) as sessions_waiting_locks,
                    COUNT(DISTINCT blocking_session) as blocking_sessions
                FROM v$session
                WHERE type = 'USER' AND status = 'ACTIVE'"
                        .to_string(),
                ),
            ),
            (
                "lock_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(CASE WHEN name = 'enqueue deadlocks' THEN value ELSE 0 END) as deadlocks_detected,
                    SUM(CASE WHEN name = 'enqueue timeouts' THEN value ELSE 0 END) as lock_timeouts
                FROM v$sysstat
                WHERE name IN ('enqueue deadlocks', 'enqueue timeouts')"
                        .to_string(),
                ),
            ),
            (
                "undo_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as undo_segments,
                    COUNT(CASE WHEN r.status = 'ONLINE' THEN 1 END) as active_undo_segments,
                    SUM(NVL(r.curblk, 0)) as undo_blocks_used
                FROM v$rollstat r
                JOIN v$rollname n ON r.usn = n.usn"
                        .to_string(),
                ),
            ),
            (
                "undo_retention".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    NVL(MAX(maxquerylen), 0) as max_undo_retention,
                    NVL(MIN(tuned_undoretention), 0) as current_undo_retention
                FROM v$undostat
                WHERE begin_time > SYSDATE - 1"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle transaction and session metrics", "transactions", SyncFrequency::High);
}
