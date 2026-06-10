use super::{ClickhouseDatabaseDetailedStats, ClickhouseDatabaseStats};
use crate::metadata::stc::utils::{collect_if_needed, query};
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_stats_if_needed(
    core_stats: &ClickhouseDatabaseStats,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseDatabaseDetailedStats>> {
    let needs_fragmentation_details = core_stats.tables_needing_optimization > 0;
    let needs_size_details = core_stats.total_disk_usage > ClickhouseDatabaseStats::LARGE_TABLE_SIZE_THRESHOLD;
    let has_detached_parts = core_stats.detached_parts > 0;

    collect_if_needed::<ClickhouseDatabaseDetailedStats, _, _>(
        needs_fragmentation_details || needs_size_details || has_detached_parts,
        context,
        ClickhouseDatabaseStats::QUERY_TIMEOUT,
        |detail_queries, mut detailed_stats| async move {
            let db_breakdown_input = query(format!(
                "SELECT
                database,
                count(DISTINCT table) as table_count,
                sum(bytes_on_disk) as total_size,
                sum(rows) as total_rows,
                count() as total_parts,
                avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as avg_compression_ratio
                FROM system.parts
                WHERE active = 1
                GROUP BY database
                ORDER BY total_size DESC
                LIMIT {}",
                ClickhouseDatabaseStats::MAX_DETAILED_RESULTS
            ));

            detail_queries
                .assign(
                    &mut detailed_stats.database_breakdown,
                    &db_breakdown_input,
                    "database_breakdown",
                    super::parsers::parse_database_info,
                )
                .await?;

            let largest_tables_input = query(format!(
                "SELECT
                database, table,
                sum(bytes_on_disk) as total_size,
                sum(rows) as total_rows,
                count() as part_count,
                max(modification_time) as last_modified,
                any(engine) as engine,
                sum(data_uncompressed_bytes) as uncompressed_size,
                sum(bytes_on_disk) as compressed_size
                FROM system.parts
                WHERE active = 1
                GROUP BY database, table
                ORDER BY total_size DESC
                LIMIT {}",
                ClickhouseDatabaseStats::MAX_DETAILED_RESULTS
            ));

            detail_queries
                .assign(
                    &mut detailed_stats.largest_tables,
                    &largest_tables_input,
                    "largest_tables",
                    super::parsers::parse_table_info,
                )
                .await?;

            if needs_fragmentation_details {
                let fragmented_input = query(format!(
                    "SELECT
                    database, table,
                    count() as part_count,
                    sum(bytes_on_disk) as total_size,
                    count(DISTINCT partition) as partition_count,
                    max(modification_time) as last_modified
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table
                    HAVING part_count > {}
                    ORDER BY part_count DESC
                    LIMIT {}",
                    ClickhouseDatabaseStats::HIGH_PART_COUNT_THRESHOLD,
                    ClickhouseDatabaseStats::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_stats.fragmented_tables,
                        &fragmented_input,
                        "fragmented_tables",
                        super::parsers::parse_fragmented_tables,
                    )
                    .await?;
            }

            let recent_mods_input = query(format!(
                "SELECT
                database, table,
                max(modification_time) as last_modified,
                sum(bytes_on_disk) as current_size,
                count() as recent_parts
                FROM system.parts
                WHERE active = 1 AND modification_time >= now() - INTERVAL 1 HOUR
                GROUP BY database, table
                ORDER BY last_modified DESC
                LIMIT {}",
                ClickhouseDatabaseStats::MAX_DETAILED_RESULTS
            ));

            detail_queries
                .assign(
                    &mut detailed_stats.recent_modifications,
                    &recent_mods_input,
                    "recent_modifications",
                    super::parsers::parse_table_modifications,
                )
                .await?;

            Ok(detailed_stats)
        },
    )
    .await
}
