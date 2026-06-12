use super::ClickhouseMergeInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseMergeInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseMergeInfo> {
    let mut merge_info = ClickhouseMergeInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseMergeInfo::QUERY_TIMEOUT);

    let (merge_overview_row, mutation_overview_row, merge_queue_stats_row, fragmentation_stats_row, recent_failures_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseMergeInfo::QUERY_MERGE_OVERVIEW),
        metadata_queries.row(ClickhouseMergeInfo::QUERY_MUTATION_OVERVIEW),
        metadata_queries.row(ClickhouseMergeInfo::QUERY_MERGE_QUEUE_STATS),
        metadata_queries.row(ClickhouseMergeInfo::QUERY_FRAGMENTATION_STATS),
        metadata_queries.row(ClickhouseMergeInfo::QUERY_RECENT_FAILURES),
    )?;

    if let Some(row) = merge_overview_row {
        merge_info.running_merges = row.u64_or_zero("running_merges")?;
        merge_info.merge_bytes_in_progress = row.u64_or_zero("merge_bytes_in_progress")?;
        merge_info.merge_rows_in_progress = row.u64_or_zero("merge_rows_in_progress")?;
        merge_info.parts_being_merged = row.u64_or_zero("parts_being_merged")?;
        merge_info.longest_merge_duration = row.f64_or_zero("longest_merge_duration")?;
        merge_info.avg_running_merge_duration = row.f64_or_zero("avg_running_merge_duration")?;
        merge_info.avg_merge_throughput = row.f64_or_zero("avg_merge_throughput")?;
    }

    if let Some(row) = mutation_overview_row {
        merge_info.running_mutations = row.u64_or_zero("running_mutations")?;
        merge_info.queued_mutations = row.u64_or_zero("queued_mutations")?;
    }

    if let Some(row) = merge_queue_stats_row {
        merge_info.queued_merges = row.u64_or_zero("queued_merges")?;
        merge_info.background_cleanup_operations = row.u64_or_zero("background_cleanup_operations")?;
    }

    if let Some(row) = fragmentation_stats_row {
        merge_info.tables_needing_merges = row.u64_or_zero("tables_needing_merges")?;
    }

    if let Some(row) = recent_failures_row {
        merge_info.failed_merges_last_hour = row.u64_or_zero("failed_merges_last_hour")?;
        merge_info.failed_mutations_last_hour = row.u64_or_zero("failed_mutations_last_hour")?;
    }

    merge_info.estimated_completion_time = ClickhouseMergeInfo::calculate_estimated_completion(&merge_info);
    merge_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&merge_info, context).await?;

    Ok(merge_info)
}
