use super::{ClickhouseActivityInfo, ClickhouseDetailedMetrics};
use crate::metadata::stc::utils::{collect_if_needed, query};
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseActivityInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseDetailedMetrics>> {
    let needs_long_query_details = core_info.longest_query_duration > ClickhouseActivityInfo::LONG_QUERY_THRESHOLD;
    let needs_failed_query_details = core_info.failed_queries_last_minute > 0;
    let needs_memory_details = core_info.query_memory_usage > ClickhouseActivityInfo::HIGH_MEMORY_THRESHOLD;

    collect_if_needed::<ClickhouseDetailedMetrics, _, _>(
        needs_long_query_details || needs_failed_query_details || needs_memory_details,
        context,
        ClickhouseActivityInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            if needs_long_query_details {
                let long_query_input = query(format!(
                    "SELECT
                    query_id, user, current_database as database,
                    substr(query, 1, 500) as query,
                    elapsed as duration,
                    memory_usage, read_rows, read_bytes,
                    query_start_time, query_kind,
                    client_name, client_hostname,
                    thread_ids[1] as main_thread_id
                FROM system.processes
                WHERE elapsed > {}
                    AND query NOT LIKE '%system.processes%'
                ORDER BY elapsed DESC
                LIMIT {}",
                    ClickhouseActivityInfo::LONG_QUERY_THRESHOLD,
                    ClickhouseActivityInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.long_running_queries,
                        &long_query_input,
                        "long_running_queries",
                        super::parsers::parse_long_running_queries,
                    )
                    .await?;
            }

            if needs_failed_query_details {
                let failed_query_input = query(format!(
                    "SELECT
                    query_id, user, database,
                    substr(query, 1, 300) as query,
                    exception, query_duration_ms / 1000.0 as duration,
                    event_time, query_kind,
                    client_name, client_hostname,
                    memory_usage, read_rows, read_bytes
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 MINUTE
                    AND exception != ''
                    AND query NOT LIKE '%system.query_log%'
                ORDER BY event_time DESC
                LIMIT {}",
                    ClickhouseActivityInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.recent_failed_queries,
                        &failed_query_input,
                        "failed_queries",
                        super::parsers::parse_failed_queries,
                    )
                    .await?;
            }

            if needs_memory_details {
                let memory_query_input = query(format!(
                    "SELECT
                    query_id, user, current_database as database,
                    substr(query, 1, 300) as query,
                    memory_usage, elapsed as duration,
                    read_rows, read_bytes, query_start_time
                FROM system.processes
                WHERE memory_usage > {}
                    AND query NOT LIKE '%system.processes%'
                ORDER BY memory_usage DESC
                LIMIT {}",
                    ClickhouseActivityInfo::HIGH_MEMORY_THRESHOLD,
                    ClickhouseActivityInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.memory_intensive_queries,
                        &memory_query_input,
                        "memory_intensive_queries",
                        super::parsers::parse_memory_queries,
                    )
                    .await?;
            }

            Ok(detailed_metrics)
        },
    )
    .await
}
