use super::{ClickhouseReplicationDetailedMetrics, ClickhouseReplicationInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseReplicationInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseReplicationDetailedMetrics>> {
    let has_high_lag = core_info.max_replication_lag > ClickhouseReplicationInfo::HIGH_LAG_THRESHOLD;
    let has_errors = core_info.tables_with_errors > 0;
    let has_readonly_tables = core_info.readonly_tables > 0;
    let has_large_queue = core_info.total_queue_size > ClickhouseReplicationInfo::LARGE_QUEUE_THRESHOLD;
    let has_failed_operations = core_info.failed_operations_last_hour > 0;
    let has_recovery = core_info.tables_in_recovery > 0;

    collect_if_needed::<ClickhouseReplicationDetailedMetrics, _, _>(
        ClickhouseReplicationInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseReplicationInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_high_lag,
                    &mut detailed_metrics.high_lag_tables,
                    ClickhouseReplicationInfo::DETAIL_QUERY_HIGH_LAG_TABLES,
                    || {
                        format!(
                            "SELECT
                    database, table, replica_name,
                    absolute_delay, log_max_index, log_pointer,
                    queue_size, inserts_in_queue, merges_in_queue,
                    last_queue_update,
                    is_session_expired, zookeeper_path,
                    active_replicas, total_replicas
                FROM system.replicas
                WHERE absolute_delay > {}
                ORDER BY absolute_delay DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::HIGH_LAG_THRESHOLD,
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_high_lag_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_errors,
                    &mut detailed_metrics.error_tables,
                    ClickhouseReplicationInfo::DETAIL_QUERY_ERROR_TABLES,
                    || {
                        format!(
                            "SELECT
                    database, table, replica_name,
                    last_queue_update_exception as last_exception,
                    last_queue_update as last_exception_time,
                    queue_size, absolute_delay,
                    is_readonly, is_session_expired,
                    zookeeper_path, replica_path,
                    log_max_index, log_pointer
                FROM system.replicas
                WHERE last_queue_update_exception != ''
                ORDER BY last_queue_update DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_error_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_readonly_tables,
                    &mut detailed_metrics.readonly_table_details,
                    ClickhouseReplicationInfo::DETAIL_QUERY_READONLY_TABLES,
                    || {
                        format!(
                            "SELECT
                    database, table, replica_name,
                    last_queue_update_exception as last_exception,
                    last_queue_update as last_exception_time,
                    absolute_delay, queue_size,
                    is_session_expired, zookeeper_path,
                    log_max_index, log_pointer,
                    active_replicas, total_replicas
                FROM system.replicas
                WHERE is_readonly = 1
                ORDER BY last_queue_update DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_readonly_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_large_queue,
                    &mut detailed_metrics.large_queue_entries,
                    ClickhouseReplicationInfo::DETAIL_QUERY_LARGE_QUEUE,
                    || {
                        format!(
                            "SELECT
                    database, table,
                    'REPLICATION' as type,
                    last_queue_update as create_time,
                    0 as required_quorum,
                    '' as source_replica,
                    '' as new_part_name,
                    '' as parts_to_merge,
                    (inserts_in_queue > 0 OR merges_in_queue > 0) as is_currently_executing,
                    0 as num_tries,
                    last_queue_update as last_attempt_time,
                    last_queue_update_exception as last_exception,
                    '' as postpone_reason
                FROM system.replicas
                WHERE queue_size > 0
                ORDER BY queue_size DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_large_queue_entries,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_failed_operations,
                    &mut detailed_metrics.failed_operations,
                    ClickhouseReplicationInfo::DETAIL_QUERY_FAILED_OPERATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, replica_name,
                    last_queue_update_exception as last_exception,
                    last_queue_update as last_exception_time,
                    queue_size, absolute_delay,
                    last_queue_update,
                    zookeeper_path
                FROM system.replicas
                WHERE last_queue_update >= now() - INTERVAL 1 HOUR
                    AND last_queue_update_exception != ''
                ORDER BY last_queue_update DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_failed_operations,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.replica_sync_status,
                    ClickhouseReplicationInfo::DETAIL_QUERY_REPLICA_STATUS,
                    format!(
                        "SELECT
                database, table, replica_name,
                is_leader, is_readonly, is_session_expired,
                absolute_delay, queue_size,
                active_replicas, total_replicas,
                zookeeper_path, replica_path,
                log_max_index, log_pointer,
                last_queue_update
                FROM system.replicas
                ORDER BY absolute_delay DESC, queue_size DESC
                LIMIT {}",
                        ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_replica_status,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.zookeeper_status,
                    ClickhouseReplicationInfo::DETAIL_QUERY_ZOOKEEPER_STATUS,
                    format!(
                        "SELECT
                zookeeper_path,
                count() as replica_count,
                countIf(is_session_expired = 0) as active_replicas,
                countIf(is_readonly = 1) as readonly_replicas,
                max(absolute_delay) as max_lag,
                sum(queue_size) as total_queue_size,
                countIf(last_queue_update_exception != '') as error_count
                FROM system.replicas
                GROUP BY zookeeper_path
                ORDER BY max_lag DESC, total_queue_size DESC
                LIMIT {}",
                        ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_zookeeper_status,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_recovery,
                    &mut detailed_metrics.recovery_operations,
                    ClickhouseReplicationInfo::DETAIL_QUERY_RECOVERY_OPERATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, replica_name,
                    last_queue_update_exception as last_exception,
                    last_queue_update as last_exception_time,
                    absolute_delay, queue_size,
                    is_readonly, is_session_expired,
                    zookeeper_path,
                    now() - last_queue_update as recovery_duration
                FROM system.replicas
                WHERE is_readonly = 1
                    AND last_queue_update_exception LIKE '%recovery%'
                ORDER BY recovery_duration DESC
                LIMIT {}",
                            ClickhouseReplicationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_recovery_operations,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
