use super::{ClickhouseConnectionDetailedMetrics, ClickhouseConnectionInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseConnectionInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseConnectionDetailedMetrics>> {
    let has_long_connections = core_info.longest_connection_duration > ClickhouseConnectionInfo::LONG_CONNECTION_THRESHOLD;
    let has_failures = core_info.connection_failures_last_minute > 0;
    let has_high_memory = core_info.avg_memory_per_connection > ClickhouseConnectionInfo::HIGH_MEMORY_THRESHOLD;

    collect_if_needed::<ClickhouseConnectionDetailedMetrics, _, _>(
        ClickhouseConnectionInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseConnectionInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_long_connections,
                    &mut detailed_metrics.long_running_connections,
                    ClickhouseConnectionInfo::DETAIL_QUERY_LONG_RUNNING_CONNECTIONS,
                    || {
                        format!(
                            "SELECT
                    user, current_database as database,
                    multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', interface = 3, 'MySQL', interface = 4, 'PostgreSQL', interface = 5, 'gRPC', interface = 6, 'InterServer', 'Unknown') as protocol,
                    query_id,
                    substr(query, 1, 200) as query_text,
                    elapsed as duration,
                    memory_usage, read_rows, read_bytes,
                    client_name, client_hostname,
                    '' as client_version,
                    query_start_time
                FROM system.processes
                WHERE elapsed > {}
                ORDER BY elapsed DESC
                LIMIT {}",
                            ClickhouseConnectionInfo::LONG_CONNECTION_THRESHOLD,
                            ClickhouseConnectionInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_long_connections,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_high_memory,
                    &mut detailed_metrics.high_memory_connections,
                    ClickhouseConnectionInfo::DETAIL_QUERY_HIGH_MEMORY_CONNECTIONS,
                    || {
                        format!(
                            "SELECT
                    user, current_database as database,
                    multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', interface = 3, 'MySQL', interface = 4, 'PostgreSQL', interface = 5, 'gRPC', interface = 6, 'InterServer', 'Unknown') as protocol,
                    query_id,
                    substr(query, 1, 200) as query_text,
                    memory_usage, elapsed as duration,
                    read_rows, read_bytes, client_name
                FROM system.processes
                WHERE memory_usage > {}
                ORDER BY memory_usage DESC
                LIMIT {}",
                            ClickhouseConnectionInfo::HIGH_MEMORY_THRESHOLD,
                            ClickhouseConnectionInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_high_memory_connections,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_failures,
                    &mut detailed_metrics.recent_connection_failures,
                    ClickhouseConnectionInfo::DETAIL_QUERY_CONNECTION_FAILURES,
                    || {
                        format!(
                            "SELECT
                    user, database, client_name, client_hostname,
                    exception, event_time, query_duration_ms / 1000.0 as duration,
                    substr(query, 1, 200) as query_text
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 MINUTE
                    AND exception != ''
                    AND type IN ('QueryStart', 'ExceptionBeforeStart')
                ORDER BY event_time DESC
                LIMIT {}",
                            ClickhouseConnectionInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_connection_failures,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.client_distribution,
                    ClickhouseConnectionInfo::DETAIL_QUERY_CLIENT_DISTRIBUTION,
                    "SELECT
                client_name, client_hostname,
                count() as connection_count,
                sum(memory_usage) as total_memory,
                avg(elapsed) as avg_duration
            FROM system.processes
            WHERE client_name != ''
            GROUP BY client_name, client_hostname
            ORDER BY connection_count DESC
            LIMIT 20",
                    super::parsers::parse_client_stats,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.idle_connections,
                    ClickhouseConnectionInfo::DETAIL_QUERY_IDLE_CONNECTIONS,
                    format!(
                        "SELECT
                user, current_database as database,
                multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', interface = 3, 'MySQL', interface = 4, 'PostgreSQL', interface = 5, 'gRPC', interface = 6, 'InterServer', 'Unknown') as protocol,
                query_id,
                elapsed as idle_duration,
                memory_usage, client_name, client_hostname
            FROM system.processes
            WHERE query = '' AND elapsed > {}
            ORDER BY elapsed DESC
            LIMIT {}",
                        ClickhouseConnectionInfo::IDLE_THRESHOLD,
                        ClickhouseConnectionInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_idle_connections,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
