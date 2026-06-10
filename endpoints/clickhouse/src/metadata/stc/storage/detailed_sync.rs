use super::{ClickhouseStorageDetailedMetrics, ClickhouseStorageInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(crate) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseStorageInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseStorageDetailedMetrics>> {
    let has_large_tables = core_info.largest_table_size > ClickhouseStorageInfo::LARGE_TABLE_THRESHOLD;
    let has_poor_compression = core_info.poorly_compressed_tables > 0;
    let has_fragmentation = core_info.fragmented_tables > 0;
    let has_active_merges = core_info.active_merges > 0;
    let has_many_partitions = core_info.total_partitions > ClickhouseStorageInfo::LARGE_PARTITION_THRESHOLD;
    let has_high_storage_usage = core_info.total_disk_usage > 1_099_511_627_776;

    collect_if_needed::<ClickhouseStorageDetailedMetrics, _, _>(
        ClickhouseStorageInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseStorageInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_large_tables || has_high_storage_usage,
                    &mut detailed_metrics.largest_tables,
                    ClickhouseStorageInfo::DETAIL_QUERY_LARGE_TABLES,
                    || {
                        format!(
                            "SELECT
                        database, name as table_name, engine,
                        total_bytes, total_rows,
                        total_bytes_uncompressed as data_uncompressed_bytes, total_bytes as data_compressed_bytes,
                        total_bytes / nullIf(total_bytes_uncompressed, 0) as compression_ratio,
                        formatReadableSize(total_bytes) as readable_size,
                        partition_key, sorting_key, primary_key
                    FROM system.tables
                    WHERE engine NOT IN ('View', 'MaterializedView', 'Dictionary')
                        AND total_bytes > 0
                    ORDER BY total_bytes DESC
                    LIMIT {}",
                            ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_large_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_poor_compression,
                    &mut detailed_metrics.poorly_compressed_tables,
                    ClickhouseStorageInfo::DETAIL_QUERY_COMPRESSION_TABLES,
                    || {
                        format!(
                            "SELECT
                        database, name as table_name, engine,
                        total_bytes, total_rows,
                        total_bytes_uncompressed as data_uncompressed_bytes, total_bytes as data_compressed_bytes,
                        total_bytes / nullIf(total_bytes_uncompressed, 0) as compression_ratio,
                        formatReadableSize(total_bytes) as readable_size,
                        '' as compression_codec
                    FROM system.tables
                    WHERE engine NOT IN ('View', 'MaterializedView', 'Dictionary')
                        AND total_bytes > 0
                        AND total_bytes / nullIf(total_bytes_uncompressed, 0) < {}
                    ORDER BY compression_ratio ASC
                    LIMIT {}",
                            ClickhouseStorageInfo::POOR_COMPRESSION_THRESHOLD,
                            ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_compression_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_fragmentation,
                    &mut detailed_metrics.fragmented_tables,
                    ClickhouseStorageInfo::DETAIL_QUERY_FRAGMENTED_TABLES,
                    || {
                        format!(
                            "SELECT
                        database, table,
                        count() as parts_count,
                        sum(bytes_on_disk) as total_size,
                        sum(rows) as total_rows,
                        max(modification_time) as last_modification,
                        min(modification_time) as oldest_partition,
                        max(modification_time) as newest_partition
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table
                    HAVING count() > {}
                    ORDER BY parts_count DESC
                    LIMIT {}",
                            ClickhouseStorageInfo::HIGH_FRAGMENTATION_THRESHOLD,
                            ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_fragmented_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_active_merges,
                    &mut detailed_metrics.active_merges,
                    ClickhouseStorageInfo::DETAIL_QUERY_ACTIVE_MERGES,
                    || {
                        format!(
                            "SELECT
                        database, table,
                        elapsed, progress,
                        num_parts, result_part_name,
                        bytes_read_uncompressed, bytes_written_uncompressed,
                        rows_read, rows_written,
                        columns_written, memory_usage,
                        thread_id
                    FROM system.merges
                    ORDER BY elapsed DESC
                    LIMIT {}",
                            ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_active_merges,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.database_storage_stats,
                    ClickhouseStorageInfo::DETAIL_QUERY_DATABASE_STATS,
                    format!(
                        "SELECT
                    database,
                    count() as table_count,
                    sum(total_bytes) as total_size,
                    sum(total_rows) as total_rows,
                    avg(total_bytes / nullIf(total_bytes_uncompressed, 0)) as avg_compression_ratio,
                    sum(total_bytes_uncompressed) as total_uncompressed,
                    sum(total_bytes) as total_compressed,
                    formatReadableSize(sum(total_bytes)) as readable_size
                FROM system.tables
                WHERE engine NOT IN ('View', 'MaterializedView', 'Dictionary')
                    AND total_bytes > 0
                GROUP BY database
                ORDER BY total_size DESC
                LIMIT {}",
                        ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_database_stats,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_many_partitions,
                    &mut detailed_metrics.partition_info,
                    ClickhouseStorageInfo::DETAIL_QUERY_PARTITION_INFO,
                    || {
                        format!(
                            "SELECT
                        database, table, partition,
                        count() as parts_in_partition,
                        sum(bytes_on_disk) as partition_size,
                        sum(rows) as partition_rows,
                        min(modification_time) as partition_min_date,
                        max(modification_time) as partition_max_date,
                        max(modification_time) as last_modified
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table, partition
                    ORDER BY partition_size DESC
                    LIMIT {}",
                            ClickhouseStorageInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_partition_info,
                )
                .await?;

            detailed_metrics.optimization_candidates =
                ClickhouseStorageInfo::generate_optimization_candidates(core_info, &detailed_metrics);
            detailed_metrics.efficiency_analysis = ClickhouseStorageInfo::generate_efficiency_analysis(core_info, &detailed_metrics);

            Ok(detailed_metrics)
        },
    )
    .await
}
