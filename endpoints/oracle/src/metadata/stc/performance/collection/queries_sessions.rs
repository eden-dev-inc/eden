use super::*;

pub(super) fn session_queries() -> Vec<(String, QueryInput)> {
    vec![
        (
            "session_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    s.sid,
                    s.serial#,
                    s.username,
                    s.program,
                    s.status,
                    s.logon_time,
                    s.sql_id,
                    s.wait_class,
                    s.event,
                    s.seconds_in_wait,
                    ss.value as cpu_time,
                    CASE
                        WHEN s.status = 'ACTIVE' AND s.sql_id IS NOT NULL THEN 1
                        ELSE 0
                    END as is_active,
                    CASE
                        WHEN s.blocking_session IS NOT NULL THEN 1
                        ELSE 0
                    END as is_blocked
                FROM v$session s
                LEFT JOIN v$sesstat ss ON s.sid = ss.sid AND ss.statistic# = (
                    SELECT statistic# FROM v$statname WHERE name = 'CPU used by this session'
                )
                WHERE s.type = 'USER'
                    AND s.username IS NOT NULL
                ORDER BY s.logon_time DESC"
                    .to_string(),
            ),
        ),
        (
            "blocking_sessions".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    bs.sid as blocking_sid,
                    s.sid as blocked_sid,
                    bs.username as blocking_username,
                    s.username as blocked_username,
                    l.type as lock_type,
                    DECODE(l.lmode,
                        0, 'None',
                        1, 'Null',
                        2, 'Row-S',
                        3, 'Row-X',
                        4, 'Share',
                        5, 'S/Row-X',
                        6, 'Exclusive'
                    ) as lock_mode,
                    o.object_name,
                    s.seconds_in_wait as block_time_seconds,
                    bs.sql_id as blocking_sql_id,
                    s.sql_id as blocked_sql_id
                FROM v$session s
                JOIN v$session bs ON s.blocking_session = bs.sid
                LEFT JOIN v$lock l ON s.sid = l.sid
                LEFT JOIN dba_objects o ON l.id1 = o.object_id
                WHERE s.blocking_session IS NOT NULL"
                    .to_string(),
            ),
        ),
    ]
}
