use super::{ClickhouseMergeDetailedMetrics, ClickhouseMergeInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseMergeInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseMergeDetailedMetrics>> {
    let has_long_merges = core_info.longest_merge_duration > ClickhouseMergeInfo::LONG_MERGE_THRESHOLD;
    let has_large_merges = core_info.merge_bytes_in_progress > ClickhouseMergeInfo::LARGE_MERGE_THRESHOLD;
    let has_fragmentation = core_info.tables_needing_merges > 0;

    collect_if_needed::<ClickhouseMergeDetailedMetrics, _, _>(
        ClickhouseMergeInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseMergeInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_long_merges,
                    &mut detailed_metrics.long_running_merges,
                    ClickhouseMergeInfo::DETAIL_QUERY_LONG_RUNNING_MERGES,
                    || {
                        format!(
                            "SELECT
                    database, table,
                    elapsed,
                    progress,
                    total_size_bytes_compressed,
                    total_size_marks,
                    num_parts,
                    result_part_name,
                    merge_type,
                    merge_algorithm,
                    source_part_names[1] as first_source_part,
                    is_mutation
                FROM system.merges
                WHERE elapsed > {}
                ORDER BY elapsed DESC
                LIMIT {}",
                            ClickhouseMergeInfo::LONG_MERGE_THRESHOLD,
                            ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_long_merges,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_large_merges,
                    &mut detailed_metrics.large_merge_operations,
                    ClickhouseMergeInfo::DETAIL_QUERY_LARGE_MERGES,
                    || {
                        format!(
                            "SELECT
                    database, table,
                    total_size_bytes_compressed,
                    total_size_marks,
                    num_parts,
                    elapsed,
                    progress,
                    merge_type,
                    merge_algorithm,
                    result_part_name,
                    is_mutation
                FROM system.merges
                WHERE total_size_bytes_compressed > {}
                ORDER BY total_size_bytes_compressed DESC
                LIMIT {}",
                            ClickhouseMergeInfo::LARGE_MERGE_THRESHOLD,
                            ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_large_merges,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.mutation_operations,
                    ClickhouseMergeInfo::DETAIL_QUERY_MUTATIONS,
                    format!(
                        "SELECT
                database, table,
                mutation_id,
                command,
                create_time,
                block_numbers.number[1] as block_number,
                parts_to_do_names,
                is_done,
                latest_failed_part,
                latest_fail_time,
                latest_fail_reason
                FROM system.mutations
                WHERE is_done = 0 OR latest_fail_time >= now() - INTERVAL 1 HOUR
                ORDER BY create_time DESC
                LIMIT {}",
                        ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_mutations,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_fragmentation,
                    &mut detailed_metrics.fragmented_tables,
                    ClickhouseMergeInfo::DETAIL_QUERY_FRAGMENTED_TABLES,
                    || {
                        format!(
                            "SELECT
                    database, table,
                    count() as part_count,
                    sum(bytes_on_disk) as total_size,
                    count(DISTINCT partition) as partition_count,
                    max(modification_time) as last_modified,
                    any(engine) as engine
                    FROM system.parts
                    WHERE active = 1
                    GROUP BY database, table
                    HAVING part_count > {}
                    ORDER BY part_count DESC
                    LIMIT {}",
                            ClickhouseMergeInfo::HIGH_PART_COUNT_THRESHOLD,
                            ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_fragmented_tables,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.merge_queue_analysis,
                    ClickhouseMergeInfo::DETAIL_QUERY_QUEUE_ANALYSIS,
                    format!(
                        "SELECT
                database, table,
                'MERGE_PARTS' as type,
                now() as create_time,
                0 as required_quorum,
                '' as source_replica,
                '' as new_part_name,
                '' as parts_to_merge,
                merges_in_queue > 0 as is_currently_executing,
                0 as num_tries,
                last_queue_update as last_attempt_time,
                last_queue_update_exception as last_exception,
                '' as postpone_reason
                FROM system.replicas
                WHERE merges_in_queue > 0 OR inserts_in_queue > 0
                ORDER BY queue_size DESC
                LIMIT {}",
                        ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_queue_analysis,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.background_process_breakdown,
                    ClickhouseMergeInfo::DETAIL_QUERY_BACKGROUND_PROCESSES,
                    format!(
                        "SELECT
                metric as task_name,
                'background_pool' as type,
                toString(value) as description
                FROM system.metrics
                WHERE metric LIKE '%Background%'
                ORDER BY metric
                LIMIT {}",
                        ClickhouseMergeInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_background_processes,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
