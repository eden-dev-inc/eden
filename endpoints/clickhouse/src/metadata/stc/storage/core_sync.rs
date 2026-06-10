use super::ClickhouseStorageInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(crate) async fn sync_metadata(storage: &ClickhouseStorageInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseStorageInfo> {
    let mut storage_info = ClickhouseStorageInfo::default();
    let requests = storage.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseStorageInfo::QUERY_TIMEOUT);

    let (storage_overview_row, parts_overview_row, table_sizes_row, merge_operations_row, fragmentation_check_row, partition_stats_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseStorageInfo::QUERY_STORAGE_OVERVIEW),
        metadata_queries.row(ClickhouseStorageInfo::QUERY_PARTS_OVERVIEW),
        metadata_queries.row(ClickhouseStorageInfo::QUERY_TABLE_SIZES),
        metadata_queries.row(ClickhouseStorageInfo::QUERY_MERGE_OPERATIONS),
        metadata_queries.row(ClickhouseStorageInfo::QUERY_FRAGMENTATION_CHECK),
        metadata_queries.row(ClickhouseStorageInfo::QUERY_PARTITION_STATS),
    )?;

    if let Some(row) = storage_overview_row {
        storage_info.total_disk_usage = row.u64_or_zero("total_disk_usage")?;
        storage_info.total_tables = row.u64_or_zero("total_tables")?;
        storage_info.total_rows = row.u64_or_zero("total_rows")?;
        storage_info.total_databases = row.u64_or_zero("total_databases")?;
        storage_info.total_uncompressed_size = row.u64_or_zero("total_uncompressed_size")?;
        storage_info.total_compressed_size = row.u64_or_zero("total_compressed_size")?;
        storage_info.avg_compression_ratio = row.f64_or_zero("avg_compression_ratio")?;
    }

    if let Some(row) = parts_overview_row {
        storage_info.total_parts = row.u64_or_zero("total_parts")?;
        storage_info.active_parts = row.u64_or_zero("active_parts")?;
        storage_info.inactive_parts = row.u64_or_zero("inactive_parts")?;
    }

    if let Some(row) = table_sizes_row {
        storage_info.avg_table_size = row.u64_or_zero("avg_table_size")?;
        storage_info.largest_table_size = row.u64_or_zero("largest_table_size")?;
        storage_info.poorly_compressed_tables = row.u64_or_zero("poorly_compressed_count")?;
    }

    if let Some(row) = merge_operations_row {
        storage_info.active_merges = row.u64_or_zero("active_merges")?;
    }

    if let Some(row) = fragmentation_check_row {
        storage_info.fragmented_tables = row.u64_or_zero("fragmented_tables")?;
    }

    if let Some(row) = partition_stats_row {
        storage_info.total_partitions = row.u64_or_zero("total_partitions")?;
    }

    storage_info.reclaimable_space = ClickhouseStorageInfo::calculate_reclaimable_space(&storage_info);
    storage_info.tables_needing_optimization = ClickhouseStorageInfo::calculate_optimization_needs(&storage_info);
    storage_info.failed_merges_last_hour = 0;
    storage_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&storage_info, context).await?;

    Ok(storage_info)
}
