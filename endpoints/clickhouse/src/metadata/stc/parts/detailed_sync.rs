use super::{ClickhousePartDetailedMetrics, ClickhousePartInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhousePartInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhousePartDetailedMetrics>> {
    let has_fragmentation = core_info.fragmented_tables > 0;
    let has_large_parts = core_info.largest_part_size > ClickhousePartInfo::LARGE_PART_SIZE_THRESHOLD;
    let has_poor_compression = core_info.poorly_compressed_parts > 0;
    let has_old_parts = core_info.old_parts > 100;
    let has_detached_parts = core_info.total_detached_parts > 0;
    let high_activity = core_info.parts_created_last_hour > 50 || core_info.parts_removed_last_hour > 50;

    collect_if_needed::<ClickhousePartDetailedMetrics, _, _>(
        ClickhousePartInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhousePartInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_fragmentation,
                    &mut detailed_metrics.highly_fragmented_tables,
                    ClickhousePartInfo::DETAIL_QUERY_FRAGMENTED_TABLES,
                    || {
                        format!(
                            "SELECT
                    database, table,
                    count() as part_count,
                    sum(bytes_on_disk) as total_size,
                    sum(rows) as total_rows,
                    count(DISTINCT partition) as partition_count,
                    max(modification_time) as last_modification,
                    min(modification_time) as first_modification,
                    any(engine) as engine,
                    avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as avg_compression_ratio
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table
                    HAVING part_count > {}
                    ORDER BY part_count DESC
                    LIMIT {}",
                            ClickhousePartInfo::HIGH_PART_COUNT_THRESHOLD,
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_fragmented_tables,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_large_parts,
                    &mut detailed_metrics.largest_parts,
                    ClickhousePartInfo::DETAIL_QUERY_LARGEST_PARTS,
                    || {
                        format!(
                            "SELECT
                    database, table, name as part_name,
                    partition, bytes_on_disk, data_uncompressed_bytes,
                    rows, modification_time,
                    data_uncompressed_bytes / nullif(bytes_on_disk, 0) as compression_ratio,
                    level, has_lightweight_delete as is_mutation,
                    marks as marks_count, primary_key_bytes_in_memory
                    FROM system.parts
                    WHERE active = 1 AND bytes_on_disk > {}
                    ORDER BY bytes_on_disk DESC
                    LIMIT {}",
                            ClickhousePartInfo::LARGE_PART_SIZE_THRESHOLD,
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_large_parts,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_poor_compression,
                    &mut detailed_metrics.poorly_compressed_parts_details,
                    ClickhousePartInfo::DETAIL_QUERY_POOR_COMPRESSION,
                    || {
                        format!(
                            "SELECT
                    database, table, name as part_name,
                    partition, bytes_on_disk, data_uncompressed_bytes,
                    data_uncompressed_bytes / nullif(bytes_on_disk, 0) as compression_ratio,
                    rows, modification_time,
                    marks as marks_count, level
                    FROM system.parts
                    WHERE active = 1
                        AND bytes_on_disk > 0
                        AND data_uncompressed_bytes / nullif(bytes_on_disk, 0) < {}
                    ORDER BY compression_ratio ASC
                    LIMIT {}",
                            ClickhousePartInfo::POOR_COMPRESSION_THRESHOLD,
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_poor_compression_parts,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    high_activity,
                    &mut detailed_metrics.recent_parts,
                    ClickhousePartInfo::DETAIL_QUERY_RECENT_PARTS,
                    || {
                        format!(
                            "SELECT
                    database, table, name as part_name,
                    partition, bytes_on_disk, rows,
                    modification_time, level, has_lightweight_delete as is_mutation,
                    data_uncompressed_bytes / nullif(bytes_on_disk, 0) as compression_ratio
                    FROM system.parts
                    WHERE active = 1
                        AND modification_time >= now() - INTERVAL 1 HOUR
                    ORDER BY modification_time DESC
                    LIMIT {}",
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_recent_parts,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_detached_parts,
                    &mut detailed_metrics.detached_parts_details,
                    ClickhousePartInfo::DETAIL_QUERY_DETACHED_PARTS,
                    || {
                        format!(
                            "SELECT
                    database, table, partition_id,
                    name as part_name, disk, reason,
                    min_block_number, max_block_number,
                    level
                    FROM system.detached_parts
                    ORDER BY database, table, name
                    LIMIT {}",
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_detached_parts,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_old_parts,
                    &mut detailed_metrics.old_parts_details,
                    ClickhousePartInfo::DETAIL_QUERY_OLD_PARTS,
                    || {
                        format!(
                            "SELECT
                    database, table, name as part_name,
                    partition, bytes_on_disk, rows,
                    modification_time,
                    now() - modification_time as age_seconds,
                    level, marks as marks_count
                    FROM system.parts
                    WHERE active = 1
                        AND modification_time < now() - INTERVAL {} DAY
                    ORDER BY modification_time ASC
                    LIMIT {}",
                            ClickhousePartInfo::OLD_PART_THRESHOLD_DAYS,
                            ClickhousePartInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_old_parts,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.size_distribution,
                    ClickhousePartInfo::DETAIL_QUERY_SIZE_DISTRIBUTION,
                    "SELECT
                CASE
                    WHEN bytes_on_disk < 1048576 THEN 'Under 1MB'
                    WHEN bytes_on_disk < 10485760 THEN '1-10MB'
                    WHEN bytes_on_disk < 104857600 THEN '10-100MB'
                    WHEN bytes_on_disk < 1073741824 THEN '100MB-1GB'
                    WHEN bytes_on_disk < 10737418240 THEN '1-10GB'
                    ELSE 'Over 10GB'
                END as size_category,
                count() as part_count,
                sum(bytes_on_disk) as total_size,
                sum(rows) as total_rows,
                avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as avg_compression_ratio
                FROM system.parts
                WHERE active = 1
                GROUP BY size_category
                ORDER BY
                    CASE size_category
                        WHEN 'Under 1MB' THEN 1
                        WHEN '1-10MB' THEN 2
                        WHEN '10-100MB' THEN 3
                        WHEN '100MB-1GB' THEN 4
                        WHEN '1-10GB' THEN 5
                        ELSE 6
                    END",
                    super::parsers::parse_size_distribution,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.partition_analysis,
                    ClickhousePartInfo::DETAIL_QUERY_PARTITION_ANALYSIS,
                    format!(
                        "SELECT
                database, table, partition,
                count() as part_count,
                sum(bytes_on_disk) as total_size,
                sum(rows) as total_rows,
                max(modification_time) as latest_part_time,
                min(modification_time) as earliest_part_time,
                avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as avg_compression_ratio,
                sum(marks) as total_marks
                FROM system.parts
                WHERE active = 1
                GROUP BY database, table, partition
                HAVING part_count > 20 OR total_size > {}
                ORDER BY part_count DESC, total_size DESC
                LIMIT {}",
                        ClickhousePartInfo::LARGE_PART_SIZE_THRESHOLD,
                        ClickhousePartInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_partition_analysis,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
