use super::ClickhouseQueryInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseQueryInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseQueryInfo> {
    let mut query_info = ClickhouseQueryInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseQueryInfo::QUERY_TIMEOUT);

    let (query_overview_row, query_locks_row, query_disk_usage_row, recent_query_stats_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseQueryInfo::QUERY_OVERVIEW),
        metadata_queries.row(ClickhouseQueryInfo::QUERY_LOCKS),
        metadata_queries.row(ClickhouseQueryInfo::QUERY_DISK_USAGE),
        metadata_queries.row(ClickhouseQueryInfo::QUERY_RECENT_STATS),
    )?;

    if let Some(row) = query_overview_row {
        query_info.running_queries = row.u64_or_zero("running_queries")?;
        query_info.slow_queries = row.u64_or_zero("slow_queries")?;
        query_info.high_memory_queries = row.u64_or_zero("high_memory_queries")?;
        query_info.long_running_queries = row.u64_or_zero("long_running_queries")?;
        query_info.total_query_memory_usage = row.u64_or_zero("total_query_memory_usage")?;
        query_info.avg_query_memory_usage = row.u64_or_zero("avg_query_memory_usage")?;
        query_info.max_query_memory_usage = row.u64_or_zero("max_query_memory_usage")?;
        query_info.max_running_query_time = row.f64_or_zero("max_running_query_time")?;
        query_info.total_bytes_read = row.u64_or_zero("total_bytes_read")?;
        query_info.total_rows_processed = row.u64_or_zero("total_rows_processed")?;
    }

    if let Some(row) = query_locks_row {
        query_info.queries_waiting_for_locks = row.u64_or_zero("queries_waiting_for_locks")?;
    }

    if let Some(row) = query_disk_usage_row {
        query_info.queries_reading_from_disk = row.u64_or_zero("queries_reading_from_disk")?;
    }

    if let Some(row) = recent_query_stats_row {
        query_info.queries_last_hour = row.u64_or_zero("queries_last_hour")?;
        query_info.failed_queries_last_hour = row.u64_or_zero("failed_queries_last_hour")?;
        query_info.avg_query_execution_time = row.f64_or_zero("avg_query_execution_time")?;
        query_info.cancelled_queries_last_hour = row.u64_or_zero("cancelled_queries_last_hour")?;
    }

    query_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&query_info, context).await?;

    Ok(query_info)
}
