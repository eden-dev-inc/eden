use super::{ClickhouseZooKeeperDetailedMetrics, ClickhouseZooKeeperInfo};
use crate::metadata::stc::utils::{collect_if_needed, query};
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseZooKeeperInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseZooKeeperDetailedMetrics>> {
    let needs_replication_details = core_info.tables_with_replication_lag > 0;
    let needs_failure_details = core_info.failed_operations_last_minute > 0 || core_info.coordination_errors_last_hour > 0;
    let needs_detached_details = core_info.detached_replicas > 0;
    let needs_session_details = core_info.avg_operation_latency_ms > ClickhouseZooKeeperInfo::HIGH_LATENCY_THRESHOLD;

    collect_if_needed::<ClickhouseZooKeeperDetailedMetrics, _, _>(
        needs_replication_details || needs_failure_details || needs_detached_details || needs_session_details,
        context,
        ClickhouseZooKeeperInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            if needs_replication_details {
                let lagging_replicas_query_input = query(format!(
                    "SELECT
                    database, table,
                    log_max_index - log_pointer as replication_lag_entries,
                    queue_size,
                    is_readonly,
                    is_session_expired,
                    last_queue_update,
                    absolute_delay,
                    total_replicas,
                    active_replicas
                FROM system.replicas
                WHERE log_max_index - log_pointer > {}
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                ORDER BY replication_lag_entries DESC
                LIMIT {}",
                    ClickhouseZooKeeperInfo::HIGH_REPLICATION_LAG_THRESHOLD,
                    ClickhouseZooKeeperInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.lagging_replicas,
                        &lagging_replicas_query_input,
                        "lagging_replicas",
                        super::parsers::parse_lagging_replicas,
                    )
                    .await?;
            }

            // system.zookeeper_log does not exist in CH 24.3+;
            // failed ZooKeeper operation detail is unavailable.
            if needs_failure_details {
                detailed_metrics.failed_operations = Vec::new();
            }

            if needs_detached_details {
                let detached_replicas_query_input = query(format!(
                    "SELECT
                    database, table,
                    is_session_expired,
                    is_readonly,
                    queue_size,
                    log_max_index - log_pointer as replication_lag,
                    last_queue_update,
                    zookeeper_path,
                    replica_name
                FROM system.replicas
                WHERE (is_session_expired = 1 OR is_readonly = 1)
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                ORDER BY last_queue_update ASC
                LIMIT {}",
                    ClickhouseZooKeeperInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.detached_replica_details,
                        &detached_replicas_query_input,
                        "detached_replicas",
                        super::parsers::parse_detached_replicas,
                    )
                    .await?;
            }

            // system.zookeeper_connection does not exist in CH 24.3+;
            // session-level detail is unavailable from system.metrics.
            if needs_session_details {
                detailed_metrics.session_details = Some(Vec::new());
            }

            if needs_replication_details || needs_detached_details {
                let queue_analysis_query_input = query(
                    "SELECT
                    database, table,
                    queue_size,
                    inserts_in_queue,
                    merges_in_queue,
                    part_mutations_in_queue,
                    total_replicas,
                    active_replicas
                FROM system.replicas
                WHERE queue_size > 0
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                ORDER BY queue_size DESC"
                        .to_string(),
                );

                detail_queries
                    .assign(
                        &mut detailed_metrics.replication_queue_analysis,
                        &queue_analysis_query_input,
                        "queue_analysis",
                        |rows| Ok(Some(super::parsers::parse_replication_queue_analysis(rows)?)),
                    )
                    .await?;
            }

            Ok(detailed_metrics)
        },
    )
    .await
}
