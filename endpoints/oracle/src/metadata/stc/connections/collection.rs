use super::*;
impl MetadataCollection for OracleConnectionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "session_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN s.type = 'USER' THEN 1 END) as current_user_sessions,
                    COUNT(CASE WHEN s.type = 'BACKGROUND' THEN 1 END) as current_background_sessions,
                    COUNT(CASE WHEN s.type = 'RECURSIVE' THEN 1 END) as current_recursive_sessions,
                    COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) as total_active_sessions,
                    params.max_sessions,
                    params.max_processes,
                    params.current_processes,
                    COUNT(CASE WHEN s.wait_class != 'Idle' AND s.event IS NOT NULL THEN 1 END) as sessions_waiting,
                    COUNT(DISTINCT CASE WHEN s.blocking_session IS NOT NULL THEN s.blocking_session END) as sessions_blocking
                FROM v$session s
                CROSS JOIN (
                    SELECT
                        (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'sessions') as max_sessions,
                        (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'processes') as max_processes,
                        (SELECT COUNT(*) FROM v$process WHERE addr IS NOT NULL) as current_processes
                    FROM dual
                ) params
                GROUP BY params.max_sessions, params.max_processes, params.current_processes"
                        .to_string(),
                ),
            ),
            (
                "memory_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COALESCE(AVG(p.pga_used_mem), 0) as avg_session_pga,
                    COALESCE(SUM(p.pga_used_mem), 0) as total_pga_allocated,
                    params.pga_aggregate_limit,
                    COUNT(CASE WHEN p.pga_used_mem > p.pga_max_mem THEN 1 END) as pga_over_allocation_count
                FROM v$process p
                JOIN v$session s ON p.addr = s.paddr
                CROSS JOIN (SELECT TO_NUMBER(value) as pga_aggregate_limit FROM v$parameter WHERE name = 'pga_aggregate_limit') params
                WHERE s.type = 'USER'
                GROUP BY params.pga_aggregate_limit"
                        .to_string(),
                ),
            ),
            (
                "sga_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    (SELECT bytes FROM v$sgainfo WHERE name = 'Shared Pool Size') as shared_pool_size,
                    (SELECT bytes FROM v$sgastat WHERE pool = 'shared pool' AND name = 'free memory') as shared_pool_free,
                    (SELECT bytes FROM v$sgainfo WHERE name = 'Buffer Cache Size') as buffer_cache_size
                FROM dual"
                        .to_string(),
                ),
            ),
            (
                "connections_by_service".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COALESCE(s.service_name, 'Unknown') as service_name,
                    COUNT(*) as total_connections,
                    COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) as active_connections,
                    COUNT(CASE WHEN s.status = 'INACTIVE' THEN 1 END) as inactive_connections,
                    COUNT(CASE WHEN s.status = 'KILLED' THEN 1 END) as killed_connections,
                    COALESCE(AVG(p.pga_used_mem), 0) as avg_pga_per_connection,
                    MAX(s.last_call_et) as longest_idle_time
                FROM v$session s
                LEFT JOIN v$process p ON s.paddr = p.addr
                WHERE s.type = 'USER'
                GROUP BY s.service_name
                ORDER BY COUNT(*) DESC"
                        .to_string(),
                ),
            ),
            (
                "connections_by_machine".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COALESCE(s.machine, 'Unknown') as machine_name,
                    COUNT(*) as total_connections,
                    COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) as active_connections,
                    COUNT(CASE WHEN s.status = 'INACTIVE' THEN 1 END) as inactive_connections,
                    COUNT(DISTINCT s.username) as unique_users,
                    COALESCE(AVG(p.pga_used_mem), 0) as avg_pga_per_connection,
                    MIN(s.logon_time) as earliest_logon,
                    MAX(s.logon_time) as latest_logon
                FROM v$session s
                LEFT JOIN v$process p ON s.paddr = p.addr
                WHERE s.type = 'USER'
                GROUP BY s.machine
                ORDER BY COUNT(*) DESC
                FETCH FIRST 20 ROWS ONLY"
                        .to_string(),
                ),
            ),
            (
                "session_breakdown".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    s.status,
                    COUNT(*) as session_count,
                    COALESCE(AVG(p.pga_used_mem), 0) as avg_pga_memory,
                    COALESCE(AVG(s.last_call_et), 0) as avg_idle_time,
                    MAX(s.last_call_et) as max_idle_time,
                    COUNT(CASE WHEN s.blocking_session IS NOT NULL THEN 1 END) as blocked_sessions,
                    COUNT(CASE WHEN s.sid IN (
                        SELECT s2.blocking_session FROM v$session s2
                        WHERE s2.blocking_session IS NOT NULL
                    ) THEN 1 END) as blocking_sessions
                FROM v$session s
                LEFT JOIN v$process p ON s.paddr = p.addr
                WHERE s.type = 'USER'
                GROUP BY s.status
                ORDER BY COUNT(*) DESC"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Oracle connection and session information", "connections", SyncFrequency::High);
}
