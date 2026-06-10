use super::ClickhouseZooKeeperInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseZooKeeperInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseZooKeeperInfo> {
    let mut zookeeper_info = ClickhouseZooKeeperInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseZooKeeperInfo::QUERY_TIMEOUT);

    let (zk_connections_row, zk_operations_row, replication_status_row, zk_sessions_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseZooKeeperInfo::QUERY_ZK_CONNECTIONS),
        metadata_queries.row(ClickhouseZooKeeperInfo::QUERY_ZK_OPERATIONS),
        metadata_queries.row(ClickhouseZooKeeperInfo::QUERY_REPLICATION_STATUS),
        metadata_queries.row(ClickhouseZooKeeperInfo::QUERY_ZK_SESSIONS),
    )?;

    if let Some(row) = zk_connections_row {
        zookeeper_info.active_connections = row.u64_or_zero("active_connections")?;
        zookeeper_info.avg_operation_latency_ms = row.f64_or_zero("avg_operation_latency_ms")?;
    }

    if let Some(row) = zk_operations_row {
        zookeeper_info.operations_last_minute = row.u64_or_zero("operations_last_minute")?;
        zookeeper_info.failed_operations_last_minute = row.u64_or_zero("failed_operations_last_minute")?;
        zookeeper_info.coordination_errors_last_hour = row.u64_or_zero("coordination_errors_last_hour")?;
    }

    if let Some(row) = replication_status_row {
        zookeeper_info.replication_queue_size = row.u64_or_zero("replication_queue_size")?;
        zookeeper_info.detached_replicas = row.u64_or_zero("detached_replicas")?;
        zookeeper_info.readonly_replicas = row.u64_or_zero("readonly_replicas")?;
        zookeeper_info.tables_with_replication_lag = row.u64_or_zero("tables_with_replication_lag")?;

        let max_lag_entries = row.u64_or_zero("max_replication_lag_entries")?;
        zookeeper_info.max_replication_lag_seconds = max_lag_entries as f64 * 0.1;
    }

    if let Some(row) = zk_sessions_row {
        zookeeper_info.active_sessions = row.u64_or_zero("active_sessions")?;
        zookeeper_info.pending_operations = row.u64_or_zero("pending_operations")?;
    }

    zookeeper_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&zookeeper_info, context).await?;

    Ok(zookeeper_info)
}
