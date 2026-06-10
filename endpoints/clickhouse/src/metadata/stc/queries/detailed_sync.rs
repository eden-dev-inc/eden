use super::{ClickhouseQueryDetailedMetrics, ClickhouseQueryInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseQueryInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseQueryDetailedMetrics>> {
    let has_slow_queries = core_info.slow_queries > 0;
    let has_high_memory = core_info.high_memory_queries > 0;
    let has_long_running = core_info.long_running_queries > 0;
    let has_failures = core_info.failed_queries_last_hour > 0;
    let has_blocked_queries = core_info.queries_waiting_for_locks > 0;

    collect_if_needed::<ClickhouseQueryDetailedMetrics, _, _>(
        ClickhouseQueryInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseQueryInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_slow_queries,
                    &mut detailed_metrics.slow_running_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_SLOW_QUERIES,
                    || {
                        format!(
                            "SELECT
                    query_id, user, current_database as database, query,
                    elapsed, memory_usage, read_bytes, read_rows,
                    total_rows_approx, http_user_agent,
                    client_name, client_hostname, client_revision,
                    thread_ids, ProfileEvents, Settings
                FROM system.processes
                WHERE elapsed > {}
                ORDER BY elapsed DESC
                LIMIT {}",
                            ClickhouseQueryInfo::SLOW_QUERY_THRESHOLD,
                            ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_slow_queries,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_high_memory,
                    &mut detailed_metrics.high_memory_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_HIGH_MEMORY_QUERIES,
                    || {
                        format!(
                            "SELECT
                    query_id, user, current_database as database, query,
                    elapsed, memory_usage, peak_memory_usage,
                    read_bytes, read_rows, written_bytes, written_rows,
                    client_name, http_user_agent
                FROM system.processes
                WHERE memory_usage > {}
                ORDER BY memory_usage DESC
                LIMIT {}",
                            ClickhouseQueryInfo::HIGH_MEMORY_THRESHOLD,
                            ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_high_memory_queries,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_long_running,
                    &mut detailed_metrics.long_running_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_LONG_RUNNING_QUERIES,
                    || {
                        format!(
                            "SELECT
                    query_id, user, current_database as database, query,
                    elapsed, memory_usage, read_bytes, read_rows,
                    client_name, client_hostname, http_user_agent,
                    ProfileEvents, Settings, thread_ids
                FROM system.processes
                WHERE elapsed > {}
                ORDER BY elapsed DESC
                LIMIT {}",
                            ClickhouseQueryInfo::LONG_RUNNING_THRESHOLD,
                            ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_long_running_queries,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_failures,
                    &mut detailed_metrics.recent_failed_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_FAILED_QUERIES,
                    || {
                        format!(
                            "SELECT
                    query_id, user, database, query, exception,
                    event_time, query_duration_ms, memory_usage,
                    read_bytes, read_rows, written_bytes, written_rows,
                    result_bytes, result_rows, client_name, http_user_agent
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 HOUR
                    AND exception != ''
                    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
                ORDER BY event_time DESC
                LIMIT {}",
                            ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_failed_queries,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_blocked_queries,
                    &mut detailed_metrics.blocked_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_BLOCKED_QUERIES,
                    || {
                        format!(
                            "SELECT
                    query_id, user, current_database as database, query,
                    elapsed, memory_usage, read_bytes, read_rows,
                    client_name, http_user_agent,
                    thread_ids
                FROM system.processes
                WHERE query LIKE '%LOCK%' OR query LIKE '%ALTER%'
                ORDER BY elapsed DESC
                LIMIT {}",
                            ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_blocked_queries,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.expensive_queries,
                    ClickhouseQueryInfo::DETAIL_QUERY_EXPENSIVE_QUERIES,
                    format!(
                        "SELECT
                query_id, user, current_database as database, query,
                elapsed, memory_usage, peak_memory_usage,
                read_bytes, read_rows, written_bytes, written_rows,
                ProfileEvents['RealTimeMicroseconds'] as cpu_time_microseconds,
                ProfileEvents['OSIOWaitMicroseconds'] as io_wait_microseconds,
                client_name, http_user_agent
                FROM system.processes
                WHERE memory_usage > {} OR read_bytes > {}
                ORDER BY memory_usage DESC, read_bytes DESC
                LIMIT {}",
                        ClickhouseQueryInfo::EXPENSIVE_QUERY_THRESHOLD,
                        ClickhouseQueryInfo::EXPENSIVE_QUERY_THRESHOLD,
                        ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_expensive_queries,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.database_query_stats,
                    ClickhouseQueryInfo::DETAIL_QUERY_DATABASE_STATS,
                    format!(
                        "SELECT
                database,
                count() as query_count,
                avg(query_duration_ms / 1000) as avg_duration_seconds,
                max(query_duration_ms / 1000) as max_duration_seconds,
                sum(memory_usage) as total_memory_usage,
                avg(memory_usage) as avg_memory_usage,
                sum(read_bytes) as total_bytes_read,
                sum(read_rows) as total_rows_read,
                countIf(exception != '') as failed_queries
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 HOUR
                    AND type = 'QueryFinish'
                GROUP BY database
                ORDER BY query_count DESC
                LIMIT {}",
                        ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_database_stats,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.user_query_stats,
                    ClickhouseQueryInfo::DETAIL_QUERY_USER_STATS,
                    format!(
                        "SELECT
                user,
                count() as query_count,
                avg(query_duration_ms / 1000) as avg_duration_seconds,
                max(query_duration_ms / 1000) as max_duration_seconds,
                sum(memory_usage) as total_memory_usage,
                avg(memory_usage) as avg_memory_usage,
                sum(read_bytes) as total_bytes_read,
                sum(read_rows) as total_rows_read,
                countIf(exception != '') as failed_queries
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 HOUR
                    AND type = 'QueryFinish'
                GROUP BY user
                ORDER BY query_count DESC
                LIMIT {}",
                        ClickhouseQueryInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_user_stats,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
