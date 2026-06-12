use super::ClickhouseDatabaseStats;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseDatabaseStats, context: ClickhouseAsync) -> ResultEP<ClickhouseDatabaseStats> {
    let mut db_stats = ClickhouseDatabaseStats::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseDatabaseStats::QUERY_TIMEOUT);

    let (database_overview_row, table_stats_row, compression_stats_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseDatabaseStats::QUERY_DATABASE_OVERVIEW),
        metadata_queries.row(ClickhouseDatabaseStats::QUERY_TABLE_STATS),
        metadata_queries.row(ClickhouseDatabaseStats::QUERY_COMPRESSION_STATS),
    )?;

    if let Some(row) = database_overview_row {
        db_stats.total_databases = row.u64_or_zero("total_databases")?;
        db_stats.total_tables = row.u64_or_zero("total_tables")?;
        db_stats.total_disk_usage = row.u64_or_zero("total_disk_usage")?;
        db_stats.total_rows = row.u64_or_zero("total_rows")?;
        db_stats.total_parts = row.u64_or_zero("total_parts")?;
        db_stats.active_parts = row.u64_or_zero("active_parts")?;
        db_stats.total_compressed_size = row.u64_or_zero("total_compressed_size")?;
        db_stats.total_uncompressed_size = row.u64_or_zero("total_uncompressed_size")?;
        db_stats.tables_needing_optimization = row.u64_or_zero("tables_needing_optimization")?;
    }

    if let Some(row) = table_stats_row {
        db_stats.temporary_tables = row.u64_or_zero("temporary_tables")?;
        db_stats.detached_parts = row.u64_or_zero("detached_parts")?;
    }

    if let Some(row) = compression_stats_row {
        db_stats.avg_compression_ratio = row.f64_or_zero("avg_compression_ratio")?;
    }

    db_stats.detailed_stats = super::detailed_sync::collect_detailed_stats_if_needed(&db_stats, context).await?;

    Ok(db_stats)
}
