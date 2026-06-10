use super::{ClickhouseTableDetailedMetrics, ClickhouseTableInfo};
use crate::metadata::stc::utils::{collect_if_needed, query};
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseTableInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseTableDetailedMetrics>> {
    let needs_problematic_table_details = core_info.tables_with_excessive_parts > 0;
    let needs_large_table_details = core_info.largest_table_size > ClickhouseTableInfo::LARGE_TABLE_THRESHOLD;
    let needs_broken_parts_details = core_info.broken_parts > 0;
    let needs_partition_analysis = core_info.tables_with_old_partitions > 0;

    collect_if_needed::<ClickhouseTableDetailedMetrics, _, _>(
        needs_problematic_table_details || needs_large_table_details || needs_broken_parts_details || needs_partition_analysis,
        context,
        ClickhouseTableInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            if needs_problematic_table_details {
                let problematic_query_input = query(format!(
                    "SELECT
                    database, table,
                    count() as part_count,
                    sum(bytes_on_disk) as total_size,
                    sum(rows) as total_rows,
                    max(modification_time) as last_modification,
                    uniq(partition) as partition_count,
                    avg(data_uncompressed_bytes / nullif(bytes_on_disk, 0)) as compression_ratio
                FROM system.parts
                WHERE active = 1
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                GROUP BY database, table
                HAVING part_count > {}
                ORDER BY part_count DESC
                LIMIT {}",
                    ClickhouseTableInfo::EXCESSIVE_PARTS_THRESHOLD,
                    ClickhouseTableInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.problematic_tables,
                        &problematic_query_input,
                        "problematic_tables",
                        super::parsers::parse_problematic_tables,
                    )
                    .await?;
            }

            if needs_large_table_details {
                let large_tables_query_input = query(format!(
                    "SELECT
                    p.database as database, p.table as table,
                    sum(p.bytes_on_disk) as total_size,
                    sum(p.rows) as total_rows,
                    count() as part_count,
                    uniq(p.partition) as partition_count,
                    max(p.modification_time) as last_modification,
                    avg(p.data_uncompressed_bytes / nullif(p.bytes_on_disk, 0)) as compression_ratio,
                    any(t.engine) as engine
                FROM system.parts p
                JOIN system.tables t ON p.database = t.database AND p.table = t.name
                WHERE p.active = 1
                    AND p.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                GROUP BY p.database, p.table
                HAVING total_size > {}
                ORDER BY total_size DESC
                LIMIT {}",
                    ClickhouseTableInfo::LARGE_TABLE_THRESHOLD,
                    ClickhouseTableInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.largest_tables,
                        &large_tables_query_input,
                        "largest_tables",
                        super::parsers::parse_large_tables,
                    )
                    .await?;
            }

            if needs_broken_parts_details {
                let broken_parts_query_input = query(format!(
                    "SELECT
                    database, table,
                    count() as broken_part_count,
                    max(event_time) as last_error_time,
                    groupArray(exception)[1] as sample_exception
                FROM system.part_log
                WHERE event_time > now() - INTERVAL 24 HOUR
                    AND exception != ''
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                GROUP BY database, table
                ORDER BY broken_part_count DESC
                LIMIT {}",
                    ClickhouseTableInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.tables_with_broken_parts,
                        &broken_parts_query_input,
                        "broken_parts",
                        super::parsers::parse_broken_parts_tables,
                    )
                    .await?;
            }

            if needs_partition_analysis {
                let partition_analysis_query_input = query(format!(
                    "SELECT
                    database, table, partition,
                    sum(bytes_on_disk) as partition_size,
                    sum(rows) as partition_rows,
                    count() as part_count,
                    min(modification_time) as oldest_date,
                    max(modification_time) as newest_date
                FROM system.parts
                WHERE active = 1
                    AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                GROUP BY database, table, partition
                ORDER BY oldest_date ASC
                LIMIT {}",
                    ClickhouseTableInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.partition_analysis,
                        &partition_analysis_query_input,
                        "partition_analysis",
                        |rows| Ok(Some(super::parsers::parse_partition_info(rows)?)),
                    )
                    .await?;
            }

            let storage_by_db_query_input = query(
                "SELECT
                database,
                count(DISTINCT table) as table_count,
                sum(bytes_on_disk) as total_size,
                sum(rows) as total_rows,
                count() as total_parts
            FROM system.parts
            WHERE active = 1
                AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
            GROUP BY database
            ORDER BY total_size DESC"
                    .to_string(),
            );

            detail_queries
                .assign(
                    &mut detailed_metrics.storage_by_database,
                    &storage_by_db_query_input,
                    "storage_by_database",
                    |rows| Ok(Some(super::parsers::parse_storage_by_database(rows)?)),
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
