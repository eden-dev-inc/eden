use super::{ClickhouseDictionaryDetailedMetrics, ClickhouseDictionaryInfo};
use crate::metadata::stc::utils::{collect_if_needed, query};
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(super) async fn collect_detailed_metrics_if_needed(
    core_info: &ClickhouseDictionaryInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseDictionaryDetailedMetrics>> {
    let has_failed_dictionaries = core_info.failed_dictionaries > 0;
    let has_slow_loading = core_info.avg_load_time > ClickhouseDictionaryInfo::SLOW_LOAD_THRESHOLD;
    let has_high_memory_usage = core_info.total_memory_usage > ClickhouseDictionaryInfo::HIGH_MEMORY_THRESHOLD;
    let has_poor_performance = core_info.low_performance_dictionaries > 0;
    let needs_reload = core_info.dictionaries_needing_reload > 0;

    collect_if_needed::<ClickhouseDictionaryDetailedMetrics, _, _>(
        has_failed_dictionaries || has_slow_loading || has_high_memory_usage || has_poor_performance || needs_reload,
        context,
        ClickhouseDictionaryInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_metrics| async move {
            if has_failed_dictionaries {
                let failed_dict_input = query(format!(
                    "SELECT
                    name, database, source,
                    last_exception, loading_start_time as last_exception_time,
                    loading_start_time, loading_duration,
                    origin, type, toString(key.names) as key,
                    lifetime_min, lifetime_max
                FROM system.dictionaries
                WHERE status = 'FAILED'
                ORDER BY loading_start_time DESC
                LIMIT {}",
                    ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.failed_dictionary_details,
                        &failed_dict_input,
                        "failed_dictionaries",
                        super::parsers::parse_failed_dictionaries,
                    )
                    .await?;
            }

            if has_slow_loading {
                let slow_dict_input = query(format!(
                    "SELECT
                    name, database, source,
                    loading_duration, loading_start_time,
                    element_count, bytes_allocated,
                    status, origin, type
                FROM system.dictionaries
                WHERE loading_duration > {}
                ORDER BY loading_duration DESC
                LIMIT {}",
                    ClickhouseDictionaryInfo::SLOW_LOAD_THRESHOLD,
                    ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.slow_loading_dictionaries,
                        &slow_dict_input,
                        "slow_dictionaries",
                        super::parsers::parse_slow_dictionaries,
                    )
                    .await?;
            }

            if has_high_memory_usage {
                let memory_dict_input = query(format!(
                    "SELECT
                    name, database, source,
                    bytes_allocated, element_count,
                    loading_duration, last_successful_update_time,
                    status, type, origin
                FROM system.dictionaries
                WHERE bytes_allocated > {}
                ORDER BY bytes_allocated DESC
                LIMIT {}",
                    ClickhouseDictionaryInfo::HIGH_MEMORY_THRESHOLD,
                    ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.memory_intensive_dictionaries,
                        &memory_dict_input,
                        "memory_dictionaries",
                        super::parsers::parse_memory_dictionaries,
                    )
                    .await?;
            }

            if has_poor_performance {
                let poor_perf_input = query(format!(
                    "SELECT
                    name, database, source,
                    toUInt64(hit_rate * query_count) as hits,
                    toUInt64((1 - hit_rate) * query_count) as misses,
                    hit_rate,
                    element_count, bytes_allocated,
                    last_successful_update_time, type
                FROM system.dictionaries
                WHERE status = 'LOADED'
                    AND hit_rate < {}
                ORDER BY hit_rate ASC
                LIMIT {}",
                    ClickhouseDictionaryInfo::LOW_HIT_RATE_THRESHOLD,
                    ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
                ));

                detail_queries
                    .assign(
                        &mut detailed_metrics.poor_performance_dictionaries,
                        &poor_perf_input,
                        "poor_performance_dictionaries",
                        super::parsers::parse_poor_performance_dictionaries,
                    )
                    .await?;
            }

            let recent_updates_input = query(format!(
                "SELECT
                name, database, source,
                last_successful_update_time,
                element_count, bytes_allocated,
                loading_duration, status, type
                FROM system.dictionaries
                WHERE last_successful_update_time >= now() - INTERVAL 1 HOUR
                ORDER BY last_successful_update_time DESC
                LIMIT {}",
                ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
            ));

            detail_queries
                .assign(
                    &mut detailed_metrics.recently_updated_dictionaries,
                    &recent_updates_input,
                    "recent_updates",
                    super::parsers::parse_dictionary_updates,
                )
                .await?;

            let source_breakdown_input = query(format!(
                "SELECT
                source,
                count() as dictionary_count,
                countIf(status = 'LOADED') as loaded_count,
                countIf(status = 'FAILED') as failed_count,
                sum(bytes_allocated) as total_memory,
                sum(element_count) as total_elements,
                avg(loading_duration) as avg_load_time
                FROM system.dictionaries
                GROUP BY source
                ORDER BY dictionary_count DESC
                LIMIT {}",
                ClickhouseDictionaryInfo::MAX_DETAILED_RESULTS
            ));

            detail_queries
                .assign(
                    &mut detailed_metrics.source_breakdown,
                    &source_breakdown_input,
                    "source_breakdown",
                    super::parsers::parse_source_info,
                )
                .await?;

            Ok(detailed_metrics)
        },
    )
    .await
}
