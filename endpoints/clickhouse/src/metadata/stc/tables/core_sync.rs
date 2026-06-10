use super::ClickhouseTableInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseTableInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseTableInfo> {
    let mut table_info = ClickhouseTableInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseTableInfo::QUERY_TIMEOUT);

    let (table_overview_row, table_health_row, optimization_stats_row, partition_age_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseTableInfo::QUERY_TABLE_OVERVIEW),
        metadata_queries.row(ClickhouseTableInfo::QUERY_TABLE_HEALTH),
        metadata_queries.row(ClickhouseTableInfo::QUERY_OPTIMIZATION_STATS),
        metadata_queries.row(ClickhouseTableInfo::QUERY_PARTITION_AGE),
    )?;

    if let Some(row) = table_overview_row {
        table_info.total_tables = row.u64_or_zero("total_tables")?;
        table_info.total_data_size = row.u64_or_zero("total_data_size")?;
        table_info.total_parts = row.u64_or_zero("total_parts")?;
        table_info.total_partitions = row.u64_or_zero("total_partitions")?;
        table_info.total_rows = row.u64_or_zero("total_rows")?;
        table_info.avg_compression_ratio = row.f64_or_zero("avg_compression_ratio")?;
        table_info.largest_table_size = row.u64_or_zero("largest_table_size")?;
        table_info.tables_with_excessive_parts = row.u64_or_zero("tables_with_excessive_parts")?;
    }

    if let Some(row) = table_health_row {
        table_info.broken_parts = row.u64_or_zero("broken_parts")?;
        table_info.recently_active_tables = row.u64_or_zero("recently_active_tables")?;
    }

    if let Some(row) = optimization_stats_row {
        table_info.tables_needing_optimization = row.u64_or_zero("tables_needing_optimization")?;
    }

    if let Some(row) = partition_age_row {
        table_info.tables_with_old_partitions = row.u64_or_zero("tables_with_old_partitions")?;
    }

    table_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&table_info, context).await?;

    Ok(table_info)
}
