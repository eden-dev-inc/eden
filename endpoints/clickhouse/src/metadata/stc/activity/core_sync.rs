use super::ClickhouseActivityInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseActivityInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseActivityInfo> {
    let mut activity_info = ClickhouseActivityInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseActivityInfo::QUERY_TIMEOUT);

    let (query_stats_row, performance_stats_row, background_ops_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseActivityInfo::QUERY_STATS),
        metadata_queries.row(ClickhouseActivityInfo::QUERY_PERFORMANCE_STATS),
        metadata_queries.row(ClickhouseActivityInfo::QUERY_BACKGROUND_OPS),
    )?;

    if let Some(row) = query_stats_row {
        activity_info.running_queries = row.u64_or_zero("running_queries")?;
        activity_info.longest_query_duration = row.f64_or_zero("longest_query_duration")?;
        activity_info.avg_running_query_duration = row.f64_or_zero("avg_running_query_duration")?;
        activity_info.query_memory_usage = row.u64_or_zero("query_memory_usage")?;
    }

    if let Some(row) = performance_stats_row {
        activity_info.failed_queries_last_minute = row.u64_or_zero("failed_queries_last_minute")?;
        let queries_last_second = row.u64_or_zero("queries_last_second")?;
        activity_info.queries_per_second = queries_last_second as f64;
    }

    if let Some(row) = background_ops_row {
        activity_info.running_merges = row.u64_or_zero("running_merges")?;
        activity_info.running_mutations = row.u64_or_zero("running_mutations")?;
    }

    activity_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&activity_info, context).await?;

    Ok(activity_info)
}
