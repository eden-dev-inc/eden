use super::*;
impl MetadataCollection for OracleLockInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "lock_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_active_locks,
                    COUNT(CASE WHEN blocking_session IS NOT NULL THEN 1 END) as blocking_locks,
                    COUNT(DISTINCT CASE WHEN blocking_session IS NOT NULL THEN blocking_session END) as unique_blockers,
                    COUNT(CASE WHEN wait_time > 0 THEN 1 END) as waiting_sessions,
                    NVL(AVG(wait_time), 0) as avg_lock_wait_time,
                    NVL(MAX(wait_time), 0) as max_lock_wait_time,
                    NVL(SUM(wait_time), 0) as total_lock_wait_time
                FROM v$session
                WHERE type = 'USER'"
                        .to_string(),
                ),
            ),
            (
                "lock_types".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN type = 'TX' THEN 1 END) as row_level_locks,
                    COUNT(CASE WHEN type = 'TM' THEN 1 END) as table_level_locks,
                    COUNT(CASE WHEN type IN ('DDL', 'DML') THEN 1 END) as ddl_locks,
                    COUNT(CASE WHEN type IN ('ST', 'SV', 'SQ') THEN 1 END) as system_locks,
                    COUNT(CASE WHEN type = 'LB' THEN 1 END) as library_cache_locks,
                    COUNT(CASE WHEN type = 'DC' THEN 1 END) as dictionary_cache_locks,
                    COUNT(CASE WHEN type NOT IN ('TX', 'TM', 'DDL', 'DML', 'ST', 'SV', 'SQ', 'LB', 'DC') THEN 1 END) as other_locks
                FROM v$lock
                WHERE request > 0 OR lmode > 0"
                        .to_string(),
                ),
            ),
            (
                "lock_modes".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN lmode = 1 THEN 1 END) as null_locks,
                    COUNT(CASE WHEN lmode = 2 THEN 1 END) as row_share_locks,
                    COUNT(CASE WHEN lmode = 3 THEN 1 END) as row_exclusive_locks,
                    COUNT(CASE WHEN lmode = 4 THEN 1 END) as share_locks,
                    COUNT(CASE WHEN lmode = 5 THEN 1 END) as share_row_exclusive_locks,
                    COUNT(CASE WHEN lmode = 6 THEN 1 END) as exclusive_locks
                FROM v$lock
                WHERE lmode > 0"
                        .to_string(),
                ),
            ),
            (
                "blocking_chains".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT DISTINCT
                    s1.sid as blocked_sid,
                    s1.serial# as blocked_serial,
                    s1.username as blocked_username,
                    s1.schemaname as blocked_schema,
                    s1.osuser as blocked_osuser,
                    s1.machine as blocked_machine,
                    s1.program as blocked_program,
                    SUBSTR(sq1.sql_text, 1, 200) as blocked_sql,
                    s1.blocking_session as blocking_sid,
                    s2.serial# as blocking_serial,
                    s2.username as blocking_username,
                    s2.schemaname as blocking_schema,
                    s2.osuser as blocking_osuser,
                    s2.machine as blocking_machine,
                    s2.program as blocking_program,
                    SUBSTR(sq2.sql_text, 1, 200) as blocking_sql,
                    s1.wait_time as wait_time_cs,
                    s1.seconds_in_wait,
                    s1.event as wait_event,
                    o.object_name,
                    o.object_type,
                    l.type as lock_type,
                    l.lmode as lock_mode_held,
                    l.request as lock_mode_requested
                FROM v$session s1
                JOIN v$session s2 ON s1.blocking_session = s2.sid
                LEFT JOIN v$lock l ON s1.sid = l.sid
                LEFT JOIN dba_objects o ON l.id1 = o.object_id
                LEFT JOIN v$sql sq1 ON s1.sql_id = sq1.sql_id
                LEFT JOIN v$sql sq2 ON s2.sql_id = sq2.sql_id
                WHERE s1.blocking_session IS NOT NULL
                    AND s1.type = 'USER'
                ORDER BY s1.seconds_in_wait DESC"
                        .to_string(),
                ),
            ),
            (
                "lock_conflicts".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    l1.sid as waiting_sid,
                    l2.sid as holding_sid,
                    l1.type as lock_type,
                    l1.id1,
                    l1.id2,
                    l1.lmode as mode_held,
                    l1.request as mode_requested,
                    l2.lmode as blocking_mode,
                    o.owner as object_owner,
                    o.object_name,
                    o.object_type,
                    s1.seconds_in_wait,
                    s1.event as wait_event
                FROM v$lock l1
                JOIN v$lock l2 ON l1.id1 = l2.id1 AND l1.id2 = l2.id2 AND l1.type = l2.type
                JOIN v$session s1 ON l1.sid = s1.sid
                LEFT JOIN dba_objects o ON l1.id1 = o.object_id
                WHERE l1.request > 0
                    AND l2.lmode > 0
                    AND l1.sid != l2.sid
                    AND s1.type = 'USER'
                ORDER BY s1.seconds_in_wait DESC
                FETCH FIRST 50 ROWS ONLY"
                        .to_string(),
                ),
            ),
            (
                "deadlock_info".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    value as total_deadlocks
                FROM v$sysstat
                WHERE name = 'enqueue deadlocks'"
                        .to_string(),
                ),
            ),
            (
                "contended_objects".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    o.owner,
                    o.object_name,
                    o.object_type,
                    COUNT(*) as lock_count,
                    COUNT(CASE WHEN l.request > 0 THEN 1 END) as waiting_count,
                    AVG(s.seconds_in_wait) as avg_wait_seconds,
                    MAX(s.seconds_in_wait) as max_wait_seconds,
                    COUNT(DISTINCT l.sid) as unique_sessions
                FROM v$lock l
                JOIN dba_objects o ON l.id1 = o.object_id
                LEFT JOIN v$session s ON l.sid = s.sid AND s.type = 'USER'
                WHERE l.type IN ('TX', 'TM')
                    AND o.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX')
                GROUP BY o.owner, o.object_name, o.object_type
                HAVING COUNT(*) > 1
                ORDER BY COUNT(CASE WHEN l.request > 0 THEN 1 END) DESC, AVG(s.seconds_in_wait) DESC
                FETCH FIRST 20 ROWS ONLY"
                        .to_string(),
                ),
            ),
            (
                "high_wait_sessions".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    s.sid,
                    s.serial#,
                    s.username,
                    s.schemaname,
                    s.osuser,
                    s.machine,
                    s.program,
                    s.seconds_in_wait,
                    s.event as wait_event,
                    s.p1text,
                    s.p1,
                    s.p2text,
                    s.p2,
                    SUBSTR(sq.sql_text, 1, 200) as current_sql,
                    s.blocking_session,
                    s.row_wait_obj#,
                    s.row_wait_file#,
                    s.row_wait_block#,
                    s.row_wait_row#
                FROM v$session s
                LEFT JOIN v$sql sq ON s.sql_id = sq.sql_id
                WHERE s.type = 'USER'
                    AND s.seconds_in_wait > 5
                    AND s.event LIKE '%enq%' OR s.event LIKE '%lock%'
                ORDER BY s.seconds_in_wait DESC
                FETCH FIRST 30 ROWS ONLY"
                        .to_string(),
                ),
            ),
            (
                "session_counts".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_user_sessions,
                    COUNT(CASE WHEN blocking_session IS NOT NULL THEN 1 END) as blocked_sessions
                FROM v$session
                WHERE type = 'USER'"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Oracle lock information and blocking analysis", "locks", SyncFrequency::High);
}
