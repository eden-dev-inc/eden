use super::*;
impl MetadataCollection for OracleActivityInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            ("core_stats".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) as active_sessions,
                    COUNT(CASE WHEN s.status = 'INACTIVE' THEN 1 END) as inactive_sessions,
                    COUNT(CASE WHEN s.status = 'KILLED' THEN 1 END) as killed_sessions,
                    COUNT(*) as total_sessions,
                    p.max_sessions,
                    COALESCE(MAX(CASE WHEN s.sql_exec_start IS NOT NULL
                        THEN (SYSDATE - s.sql_exec_start) * 86400 ELSE 0 END), 0) as longest_sql_duration,
                    COALESCE(MAX(CASE WHEN s.logon_time IS NOT NULL
                        THEN (SYSDATE - s.logon_time) * 86400 ELSE 0 END), 0) as longest_transaction_duration,
                    COALESCE(AVG(CASE WHEN s.sql_exec_start IS NOT NULL AND s.status = 'ACTIVE'
                        THEN (SYSDATE - s.sql_exec_start) * 86400 ELSE NULL END), 0) as avg_active_sql_duration,
                    COUNT(CASE WHEN s.blocking_session IS NOT NULL THEN 1 END) as waiting_sessions_count
                FROM v$session s
                CROSS JOIN (SELECT TO_NUMBER(value) as max_sessions FROM v$parameter WHERE name = 'sessions') p
                WHERE s.type = 'USER'
                GROUP BY p.max_sessions".to_string())
            ),
            ("blocking_count".to_string(),
             crate::metadata::stc::utils::query("SELECT COUNT(DISTINCT blocking_session) as blocking_count
                 FROM v$session
                 WHERE blocking_session IS NOT NULL
                   AND blocking_session_status = 'VALID'".to_string())
            ),
            ("system_resources".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    (SELECT COUNT(*) FROM v$px_session WHERE qcsid IS NOT NULL) as parallel_servers_active,
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'parallel_max_servers') as parallel_servers_max,
                    (SELECT SUM(pga_used_mem) FROM v$process WHERE pga_used_mem > 0) as current_pga_used,
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'pga_aggregate_limit') as pga_aggregate_limit,
                    (SELECT SUM(bytes) FROM v$sgainfo WHERE name IN ('Fixed Size', 'Variable Size', 'Database Buffers', 'Redo Buffers')) as sga_size,
                    (SELECT COUNT(*) FROM v$process WHERE addr IS NOT NULL) as process_count,
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'processes') as process_limit
                FROM dual".to_string())
            )
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle database activity metrics", "activity", SyncFrequency::High);
}
