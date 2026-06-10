use super::{ClickhouseMutationDetailedMetrics, ClickhouseMutationInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseMutationInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseMutationDetailedMetrics>> {
    let has_long_mutations = core_info.longest_mutation_duration > ClickhouseMutationInfo::LONG_MUTATION_THRESHOLD;
    let has_failed_mutations = core_info.failed_mutations_last_24h > 0;
    let has_stuck_mutations = core_info.stuck_mutations > 0;
    let has_large_mutations = core_info.total_parts_to_mutate > ClickhouseMutationInfo::LARGE_MUTATION_THRESHOLD;

    collect_if_needed::<ClickhouseMutationDetailedMetrics, _, _>(
        ClickhouseMutationInfo::should_collect_detailed_metrics(core_info),
        context,
        ClickhouseMutationInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            detail_queries
                .assign_sql_if(
                    has_long_mutations,
                    &mut detailed_metrics.long_running_mutations,
                    ClickhouseMutationInfo::DETAIL_QUERY_LONG_RUNNING_MUTATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, mutation_id,
                    command, create_time,
                    now() - create_time as duration,
                    parts_to_do,
                    length(parts_to_do_names) as parts_completed,
                    latest_failed_part,
                    latest_fail_time,
                    latest_fail_reason,
                    block_numbers.number[1] as block_number
                FROM system.mutations
                WHERE is_done = 0
                    AND now() - create_time > {}
                ORDER BY duration DESC
                LIMIT {}",
                            ClickhouseMutationInfo::LONG_MUTATION_THRESHOLD,
                            ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_long_mutations,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_failed_mutations,
                    &mut detailed_metrics.failed_mutation_details,
                    ClickhouseMutationInfo::DETAIL_QUERY_FAILED_MUTATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, mutation_id,
                    command, create_time,
                    latest_failed_part,
                    latest_fail_time,
                    latest_fail_reason,
                    parts_to_do,
                    length(parts_to_do_names) as parts_completed_before_failure,
                    block_numbers.number[1] as block_number
                FROM system.mutations
                WHERE latest_fail_time >= now() - INTERVAL 24 HOUR
                    AND latest_failed_part != ''
                ORDER BY latest_fail_time DESC
                LIMIT {}",
                            ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_failed_mutations,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_stuck_mutations,
                    &mut detailed_metrics.stuck_mutation_details,
                    ClickhouseMutationInfo::DETAIL_QUERY_STUCK_MUTATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, mutation_id,
                    command, create_time,
                    now() - create_time as stuck_duration,
                    parts_to_do,
                    length(parts_to_do_names) as parts_completed,
                    latest_fail_time,
                    latest_fail_reason,
                    latest_failed_part
                FROM system.mutations
                WHERE is_done = 0
                    AND latest_fail_time < now() - INTERVAL 6 HOUR
                    AND parts_to_do > 0
                ORDER BY stuck_duration DESC
                LIMIT {}",
                            ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_stuck_mutations,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_large_mutations,
                    &mut detailed_metrics.large_mutation_operations,
                    ClickhouseMutationInfo::DETAIL_QUERY_LARGE_MUTATIONS,
                    || {
                        format!(
                            "SELECT
                    database, table, mutation_id,
                    command, create_time,
                    parts_to_do,
                    length(parts_to_do_names) as parts_completed,
                    parts_to_do + length(parts_to_do_names) as total_parts,
                    now() - create_time as duration,
                    latest_fail_time,
                    is_done
                FROM system.mutations
                WHERE parts_to_do > {}
                ORDER BY total_parts DESC
                LIMIT {}",
                            ClickhouseMutationInfo::LARGE_MUTATION_THRESHOLD,
                            ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_large_mutations,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.recent_completions,
                    ClickhouseMutationInfo::DETAIL_QUERY_RECENT_COMPLETIONS,
                    format!(
                        "SELECT
                database, table, mutation_id,
                command, create_time,
                latest_fail_time as completion_time,
                latest_fail_time - create_time as total_duration,
                length(parts_to_do_names) as parts_processed,
                block_numbers.number[1] as block_number
                FROM system.mutations
                WHERE is_done = 1
                    AND create_time >= now() - INTERVAL 1 HOUR
                ORDER BY completion_time DESC
                LIMIT {}",
                        ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_mutation_completions,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.command_type_breakdown,
                    ClickhouseMutationInfo::DETAIL_QUERY_COMMAND_BREAKDOWN,
                    format!(
                        "SELECT
                extractAll(command, '^(ALTER TABLE|UPDATE|DELETE|DROP|ADD)')[1] as command_type,
                count() as total_count,
                countIf(is_done = 0) as active_count,
                countIf(is_done = 1) as completed_count,
                countIf(latest_failed_part != '') as failed_count,
                avg(latest_fail_time - create_time) as avg_duration
                FROM system.mutations
                WHERE command != ''
                GROUP BY command_type
                ORDER BY total_count DESC
                LIMIT {}",
                        ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_command_stats,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_metrics.tables_with_multiple_mutations,
                    ClickhouseMutationInfo::DETAIL_QUERY_MULTIPLE_MUTATIONS,
                    format!(
                        "SELECT
                database, table,
                count() as mutation_count,
                countIf(is_done = 0) as active_mutation_count,
                sum(parts_to_do) as total_parts_to_mutate,
                max(now() - create_time) as oldest_mutation_age,
                countIf(latest_failed_part != '') as failed_mutation_count
                FROM system.mutations
                GROUP BY database, table
                HAVING mutation_count > 1
                ORDER BY active_mutation_count DESC, mutation_count DESC
                LIMIT {}",
                        ClickhouseMutationInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_table_mutation_info,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
