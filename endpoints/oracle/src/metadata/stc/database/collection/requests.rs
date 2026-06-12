use super::*;

pub(super) fn build_requests() -> HashMap<String, QueryInput> {
    HashMap::from([
        (
            "instance_info".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    i.instance_number as instance_id,
                    d.name as database_name,
                    d.db_unique_name,
                    d.database_role,
                    d.open_mode as database_status,
                    i.status as instance_status,
                    i.startup_time,
                    (SYSDATE - i.startup_time) * 86400 as uptime_seconds,
                    i.version,
                    i.host_name
                FROM v$instance i, v$database d"
                    .to_string(),
            ),
        ),
        (
            "cache_hit_ratios".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ROUND((1 - (phyread.value / (dbget.value + conget.value))) * 100, 2) as buffer_cache_hit_ratio,
                    ROUND((libhit.value / (libhit.value + libmiss.value)) * 100, 2) as library_cache_hit_ratio,
                    ROUND((dicthit.value / (dicthit.value + dictmiss.value)) * 100, 2) as dictionary_cache_hit_ratio
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'physical reads') phyread,
                    (SELECT value FROM v$sysstat WHERE name = 'db block gets') dbget,
                    (SELECT value FROM v$sysstat WHERE name = 'consistent gets') conget,
                    (SELECT value FROM v$sysstat WHERE name = 'library cache hits') libhit,
                    (SELECT value FROM v$sysstat WHERE name = 'library cache misses') libmiss,
                    (SELECT value FROM v$sysstat WHERE name = 'data dictionary hits') dicthit,
                    (SELECT value FROM v$sysstat WHERE name = 'data dictionary misses') dictmiss"
                    .to_string(),
            ),
        ),
        (
            "parse_ratios".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ROUND((softparse.value / (softparse.value + hardparse.value)) * 100, 2) as soft_parse_ratio,
                    ROUND(executions.value / (softparse.value + hardparse.value), 2) as execute_to_parse_ratio
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'parse count (soft)') softparse,
                    (SELECT value FROM v$sysstat WHERE name = 'parse count (hard)') hardparse,
                    (SELECT value FROM v$sysstat WHERE name = 'execute count') executions"
                    .to_string(),
            ),
        ),
        (
            "io_statistics".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ROUND(phyread.value / uptime.uptime_seconds, 2) as physical_reads_per_sec,
                    ROUND(phywrite.value / uptime.uptime_seconds, 2) as physical_writes_per_sec,
                    ROUND((dbget.value + conget.value) / uptime.uptime_seconds, 2) as logical_reads_per_sec,
                    ROUND(blockchange.value / uptime.uptime_seconds, 2) as block_changes_per_sec,
                    ROUND(redosize.value / uptime.uptime_seconds, 2) as redo_size_per_sec,
                    ROUND(usercalls.value / uptime.uptime_seconds, 2) as user_calls_per_sec,
                    ROUND(usertxn.value / uptime.uptime_seconds, 2) as transactions_per_sec,
                    ROUND(executions.value / uptime.uptime_seconds, 2) as executions_per_sec
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'physical reads') phyread,
                    (SELECT value FROM v$sysstat WHERE name = 'physical writes') phywrite,
                    (SELECT value FROM v$sysstat WHERE name = 'db block gets') dbget,
                    (SELECT value FROM v$sysstat WHERE name = 'consistent gets') conget,
                    (SELECT value FROM v$sysstat WHERE name = 'db block changes') blockchange,
                    (SELECT value FROM v$sysstat WHERE name = 'redo size') redosize,
                    (SELECT value FROM v$sysstat WHERE name = 'user calls') usercalls,
                    (SELECT (SELECT value FROM v$sysstat WHERE name = 'user commits') + (SELECT value FROM v$sysstat WHERE name = 'user rollbacks') as value FROM dual) usertxn,
                    (SELECT value FROM v$sysstat WHERE name = 'execute count') executions,
                    (SELECT (SYSDATE - startup_time) * 86400 as uptime_seconds FROM v$instance) uptime"
                    .to_string(),
            ),
        ),
        (
            "transaction_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    commits.value as user_commits,
                    rollbacks.value as user_rollbacks,
                    ROUND((commits.value + rollbacks.value) / uptime.uptime_seconds, 2) as user_transaction_rate,
                    ROUND((commits.value / GREATEST(commits.value + rollbacks.value, 1)) * 100, 2) as user_commit_percentage
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'user commits') commits,
                    (SELECT value FROM v$sysstat WHERE name = 'user rollbacks') rollbacks,
                    (SELECT (SYSDATE - startup_time) * 86400 as uptime_seconds FROM v$instance) uptime"
                    .to_string(),
            ),
        ),
        (
            "session_process_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    (SELECT COUNT(*) FROM v$session WHERE type = 'USER') as current_sessions,
                    (SELECT COUNT(*) FROM v$process WHERE addr IS NOT NULL) as current_processes,
                    (SELECT value FROM v$sysstat WHERE name = 'logons cumulative') as peak_sessions,
                    (SELECT value FROM v$sysstat WHERE name = 'opened cursors cumulative') as peak_processes
                FROM dual"
                    .to_string(),
            ),
        ),
        (
            "memory_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    (SELECT SUM(bytes) FROM v$sgainfo) as sga_size,
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'pga_aggregate_target') as pga_aggregate_target,
                    (SELECT SUM(pga_used_mem) FROM v$process WHERE pga_used_mem > 0) as pga_used,
                    (SELECT bytes FROM v$sgainfo WHERE name = 'Shared Pool Size') as shared_pool_size,
                    (SELECT bytes FROM v$sgainfo WHERE name = 'Buffer Cache Size') as buffer_cache_size,
                    (SELECT bytes FROM v$sgainfo WHERE name = 'Redo Buffers') as log_buffer_size
                FROM dual"
                    .to_string(),
            ),
        ),
        (
            "database_size".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    SUM(bytes) as database_size,
                    SUM(bytes) - SUM(NVL(free_bytes, 0)) as used_space,
                    SUM(NVL(free_bytes, 0)) as free_space
                FROM (
                    SELECT
                        df.bytes,
                        fs.free_bytes
                    FROM (
                        SELECT tablespace_name, SUM(bytes) as bytes
                        FROM dba_data_files
                        GROUP BY tablespace_name
                    ) df
                    LEFT JOIN (
                        SELECT tablespace_name, SUM(bytes) as free_bytes
                        FROM dba_free_space
                        GROUP BY tablespace_name
                    ) fs ON df.tablespace_name = fs.tablespace_name
                )"
                .to_string(),
            ),
        ),
        (
            "tablespace_counts".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    (SELECT COUNT(*) FROM dba_tablespaces) as tablespace_count,
                    (SELECT COUNT(*) FROM dba_data_files) as datafile_count,
                    (SELECT COUNT(*) FROM v$controlfile) as controlfile_count,
                    (SELECT COUNT(DISTINCT group#) FROM v$log) as redo_log_groups
                FROM dual"
                    .to_string(),
            ),
        ),
        (
            "cpu_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ROUND(cpu_used.value / cpu_total.value * 100, 2) as cpu_usage_percentage,
                    dbcpu.value as db_cpu_time,
                    bgcpu.value as background_cpu_time,
                    parsecpu.value as parse_cpu_time
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'CPU used by this session') cpu_used,
                    (SELECT (SELECT value FROM v$osstat WHERE stat_name = 'IDLE_TIME') + (SELECT value FROM v$osstat WHERE stat_name = 'BUSY_TIME') as value FROM dual) cpu_total,
                    (SELECT value FROM v$sysstat WHERE name = 'CPU used when call started') dbcpu,
                    (SELECT value FROM v$sysstat WHERE name = 'background cpu time') bgcpu,
                    (SELECT value FROM v$sysstat WHERE name = 'parse time cpu') parsecpu"
                    .to_string(),
            ),
        ),
        (
            "archive_log_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    NVL(ROUND((SELECT SUM(blocks * block_size) / 1024 / 1024
                           FROM v$archived_log
                           WHERE first_time >= SYSDATE - 1) / 24, 2), 0) as archive_log_rate_mb_per_hour,
                    (SELECT COUNT(*)
                     FROM v$archived_log
                     WHERE first_time >= TRUNC(SYSDATE)) as archive_logs_today,
                    NVL((SELECT ROUND(AVG(blocks * block_size), 0)
                     FROM v$archived_log
                     WHERE first_time >= SYSDATE - 7), 0) as avg_archive_log_size
                FROM dual"
                    .to_string(),
            ),
        ),
        (
            "response_time_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ROUND(dbtime.value / usertxn.value / 1000000, 4) as response_time_per_txn,
                    ROUND(dbtime.value / usercalls.value / 1000000, 4) as sql_service_response_time,
                    ROUND(dbtime.value / uptime.uptime_seconds / 1000000, 2) as database_time_per_sec,
                    ROUND(bgtime.value / uptime.uptime_seconds / 1000000, 2) as background_time_per_sec
                FROM
                    (SELECT value FROM v$sysstat WHERE name = 'DB time') dbtime,
                    (SELECT value FROM v$sysstat WHERE name = 'background elapsed time') bgtime,
                    (SELECT (SELECT value FROM v$sysstat WHERE name = 'user commits') + (SELECT value FROM v$sysstat WHERE name = 'user rollbacks') as value FROM dual) usertxn,
                    (SELECT value FROM v$sysstat WHERE name = 'user calls') usercalls,
                    (SELECT (SYSDATE - startup_time) * 86400 as uptime_seconds FROM v$instance) uptime"
                    .to_string(),
            ),
        ),
        (
            "top_wait_events".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    event,
                    wait_class,
                    total_waits,
                    total_timeouts,
                    time_waited,
                    average_wait,
                    ROUND((time_waited / SUM(time_waited) OVER()) * 100, 2) as pct_of_total_time
                FROM v$system_event
                WHERE wait_class != 'Idle'
                    AND time_waited > 0
                ORDER BY time_waited DESC
                FETCH FIRST 10 ROWS ONLY"
                    .to_string(),
            ),
        ),
    ])
}
