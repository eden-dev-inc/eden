use super::*;

pub(super) fn core_queries() -> Vec<(String, QueryInput)> {
    vec![
        (
            "system_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    s.name as stat_name,
                    s.value as stat_value,
                    s.class
                FROM v$sysstat s
                WHERE s.name IN (
                    'CPU used by this session',
                    'DB time',
                    'user calls',
                    'parse count (total)',
                    'parse count (hard)',
                    'execute count',
                    'session logical reads',
                    'physical reads',
                    'physical writes',
                    'redo size',
                    'user commits',
                    'user rollbacks'
                )
                ORDER BY s.name"
                    .to_string(),
            ),
        ),
        (
            "wait_events".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    se.event,
                    se.wait_class,
                    se.total_waits,
                    se.total_timeouts,
                    se.time_waited,
                    se.average_wait,
                    ROUND((se.time_waited / SUM(se.time_waited) OVER ()) * 100, 2) as pct_db_time
                FROM v$system_event se
                WHERE se.wait_class != 'Idle'
                    AND se.total_waits > 0
                ORDER BY se.time_waited DESC
                FETCH FIRST 20 ROWS ONLY"
                    .to_string(),
            ),
        ),
        (
            "sql_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    s.sql_id,
                    SUBSTR(s.sql_text, 1, 100) as sql_text,
                    s.executions,
                    s.elapsed_time,
                    s.cpu_time,
                    s.buffer_gets,
                    s.disk_reads,
                    s.rows_processed,
                    s.parse_calls,
                    s.optimizer_cost,
                    TO_DATE(s.first_load_time, 'YYYY-MM-DD/HH24:MI:SS') as first_load_time,
                    s.last_active_time,
                    CASE
                        WHEN s.executions > 0 THEN s.elapsed_time / s.executions
                        ELSE 0
                    END as avg_elapsed_time,
                    CASE
                        WHEN s.executions > 0 THEN s.cpu_time / s.executions
                        ELSE 0
                    END as avg_cpu_time
                FROM v$sql s
                WHERE s.executions > 0
                    AND s.elapsed_time > 0
                ORDER BY s.elapsed_time DESC
                FETCH FIRST 50 ROWS ONLY"
                    .to_string(),
            ),
        ),
    ]
}
