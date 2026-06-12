use super::ClickhouseReplicationInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseReplicationInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseReplicationInfo> {
    let mut replication_info = ClickhouseReplicationInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseReplicationInfo::QUERY_TIMEOUT);

    let (replication_overview_row, replica_status_row, zookeeper_sessions_row, queue_performance_row, recent_failures_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseReplicationInfo::QUERY_REPLICATION_OVERVIEW),
        metadata_queries.row(ClickhouseReplicationInfo::QUERY_REPLICA_STATUS),
        metadata_queries.row(ClickhouseReplicationInfo::QUERY_ZOOKEEPER_SESSIONS),
        metadata_queries.row(ClickhouseReplicationInfo::QUERY_QUEUE_PERFORMANCE),
        metadata_queries.row(ClickhouseReplicationInfo::QUERY_RECENT_FAILURES),
    )?;

    if let Some(row) = replication_overview_row {
        replication_info.total_replicated_tables = row.u64_or_zero("total_replicated_tables")?;
        replication_info.synchronized_tables = row.u64_or_zero("synchronized_tables")?;
        replication_info.lagging_tables = row.u64_or_zero("lagging_tables")?;
        replication_info.tables_with_errors = row.u64_or_zero("tables_with_errors")?;
        replication_info.readonly_tables = row.u64_or_zero("readonly_tables")?;
        replication_info.total_queue_size = row.u64_or_zero("total_queue_size")?;
        replication_info.active_queue_entries = row.u64_or_zero("active_queue_entries")?;
        replication_info.max_replication_lag = row.f64_or_zero("max_replication_lag")?;
        replication_info.avg_replication_lag = row.f64_or_zero("avg_replication_lag")?;
    }

    if let Some(row) = replica_status_row {
        replication_info.out_of_sync_replicas = row.u64_or_zero("out_of_sync_replicas")?;
        replication_info.tables_in_recovery = row.u64_or_zero("tables_in_recovery")?;
    }

    if let Some(row) = zookeeper_sessions_row {
        replication_info.total_zookeeper_sessions = row.u64_or_zero("total_zookeeper_sessions")?;
        replication_info.active_zookeeper_sessions = row.u64_or_zero("active_zookeeper_sessions")?;
    }

    if let Some(row) = queue_performance_row {
        replication_info.avg_queue_processing_time = row.f64_or_zero("avg_queue_processing_time")?;
    }

    if let Some(row) = recent_failures_row {
        replication_info.failed_operations_last_hour = row.u64_or_zero("failed_operations_last_hour")?;
    }

    replication_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&replication_info, context).await?;

    Ok(replication_info)
}
