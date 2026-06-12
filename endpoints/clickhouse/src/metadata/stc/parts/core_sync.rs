use super::ClickhousePartInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhousePartInfo, context: ClickhouseAsync) -> ResultEP<ClickhousePartInfo> {
    let mut part_info = ClickhousePartInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhousePartInfo::QUERY_TIMEOUT);

    let (part_overview_row, fragmentation_stats_row, part_activity_row, detached_parts_count_row, compression_quality_row) = tokio::try_join!(
        metadata_queries.row(ClickhousePartInfo::QUERY_PART_OVERVIEW),
        metadata_queries.row(ClickhousePartInfo::QUERY_FRAGMENTATION_STATS),
        metadata_queries.row(ClickhousePartInfo::QUERY_PART_ACTIVITY),
        metadata_queries.row(ClickhousePartInfo::QUERY_DETACHED_PARTS_COUNT),
        metadata_queries.row(ClickhousePartInfo::QUERY_COMPRESSION_QUALITY),
    )?;

    if let Some(row) = part_overview_row {
        part_info.total_active_parts = row.u64_or_zero("total_active_parts")?;
        part_info.total_inactive_parts = row.u64_or_zero("total_inactive_parts")?;
        part_info.total_disk_usage = row.u64_or_zero("total_disk_usage")?;
        part_info.total_uncompressed_size = row.u64_or_zero("total_uncompressed_size")?;
        part_info.total_rows = row.u64_or_zero("total_rows")?;
        part_info.avg_compression_ratio = row.f64_or_zero("avg_compression_ratio")?;
        part_info.avg_part_size = row.u64_or_zero("avg_part_size")?;
        part_info.largest_part_size = row.u64_or_zero("largest_part_size")?;
        part_info.smallest_part_size = row.u64_or_zero("smallest_part_size")?;
    }

    if let Some(row) = fragmentation_stats_row {
        part_info.fragmented_tables = row.u64_or_zero("fragmented_tables")?;
    }

    if let Some(row) = part_activity_row {
        part_info.parts_created_last_hour = row.u64_or_zero("parts_created_last_hour")?;
        part_info.parts_removed_last_hour = row.u64_or_zero("parts_removed_last_hour")?;
    }

    if let Some(row) = detached_parts_count_row {
        part_info.total_detached_parts = row.u64_or_zero("total_detached_parts")?;
    }

    if let Some(row) = compression_quality_row {
        part_info.poorly_compressed_parts = row.u64_or_zero("poorly_compressed_parts")?;
        part_info.old_parts = row.u64_or_zero("old_parts")?;
    }

    part_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&part_info, context).await?;

    Ok(part_info)
}
