use super::*;
impl MetadataCollection for OracleSessionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "session_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN s.type = 'USER' THEN 1 END) as total_user_sessions,
                    COUNT(CASE WHEN s.type = 'USER' AND s.status = 'ACTIVE' THEN 1 END) as active_user_sessions,
                    COUNT(CASE WHEN s.type = 'USER' AND s.status = 'INACTIVE' THEN 1 END) as inactive_user_sessions,
                    COUNT(CASE WHEN s.status = 'KILLED' THEN 1 END) as killed_sessions,
                    COUNT(CASE WHEN s.type = 'USER' AND s.status = 'CACHED' THEN 1 END) as cached_sessions,
                    COUNT(CASE WHEN s.type = 'BACKGROUND' THEN 1 END) as background_processes,
                    p.max_sessions,
                    COUNT(DISTINCT CASE WHEN s.type = 'USER' THEN s.username END) as unique_users,
                    COUNT(DISTINCT CASE WHEN s.type = 'USER' THEN s.program END) as unique_programs,
                    COUNT(DISTINCT CASE WHEN s.type = 'USER' THEN s.machine END) as unique_machines,
                    COALESCE(AVG(CASE WHEN s.type = 'USER' AND s.logon_time IS NOT NULL
                        THEN (SYSDATE - s.logon_time) * 86400 ELSE NULL END), 0) as avg_session_duration,
                    COALESCE(MAX(CASE WHEN s.type = 'USER' AND s.logon_time IS NOT NULL
                        THEN (SYSDATE - s.logon_time) * 86400 ELSE 0 END), 0) as longest_session_duration,
                    COUNT(CASE WHEN s.blocking_session IS NOT NULL THEN 1 END) as sessions_waiting_for_locks
                FROM v$session s
                CROSS JOIN (SELECT TO_NUMBER(value) as max_sessions FROM v$parameter WHERE name = 'sessions') p
                GROUP BY p.max_sessions"
                        .to_string(),
                ),
            ),
            (
                "connection_activity".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN s.logon_time >= SYSDATE - 1/24 THEN 1 END) as new_sessions_last_hour,
                    NVL(MAX(st.value), 0) as total_logons_since_startup,
                    COUNT(CASE WHEN s.server = 'DEDICATED' THEN 1 END) as dedicated_connections,
                    COUNT(CASE WHEN s.server = 'SHARED' THEN 1 END) as shared_connections
                FROM v$session s
                CROSS JOIN (SELECT value FROM v$sysstat WHERE name = 'logons cumulative') st
                WHERE s.type = 'USER'"
                        .to_string(),
                ),
            ),
            (
                "resource_usage".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN ss.value > 104857600 THEN 1 END) as sessions_using_temp,
                    SUM(CASE WHEN ss.value > 0 THEN ss.value ELSE 0 END) as total_temp_space_used,
                    COUNT(CASE WHEN p.pga_used_mem > 104857600 THEN 1 END) as high_pga_sessions,
                    SUM(CASE WHEN p.pga_used_mem > 0 THEN p.pga_used_mem ELSE 0 END) as total_pga_used
                FROM v$session s
                LEFT JOIN v$sesstat ss ON s.sid = ss.sid AND ss.statistic# = (
                    SELECT statistic# FROM v$statname WHERE name = 'session uga memory max'
                )
                LEFT JOIN v$process p ON s.paddr = p.addr
                WHERE s.type = 'USER'"
                        .to_string(),
                ),
            ),
            (
                "security_metrics".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as failed_logins_last_hour
                FROM dba_audit_trail
                WHERE action_name = 'LOGON'
                  AND returncode != 0
                  AND timestamp >= SYSDATE - 1/24"
                        .to_string(),
                ),
            ),
            (
                "session_counts_history".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN timestamp >= SYSDATE - 1/24 THEN 1 END) as disconnected_sessions_last_hour
                FROM dba_audit_trail
                WHERE action_name = 'LOGOFF'
                  AND timestamp >= SYSDATE - 1/24"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle session activity and connection metrics", "session", SyncFrequency::High);
}
