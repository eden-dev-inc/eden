use super::{ClickhouseSettingsDetailedInfo, ClickhouseSettingsInfo};
use crate::metadata::stc::utils::collect_if_needed;
use clickhouse_core::ClickhouseAsync;
use error::ResultEP;

pub(crate) async fn collect_detailed_settings_if_needed(
    core_info: &ClickhouseSettingsInfo,
    context: ClickhouseAsync,
) -> ResultEP<Option<ClickhouseSettingsDetailedInfo>> {
    let has_inconsistent_settings = core_info.inconsistent_settings_count > 0;
    let has_deprecated_settings = core_info.deprecated_settings_count > 0;

    collect_if_needed::<ClickhouseSettingsDetailedInfo, _, _>(
        ClickhouseSettingsInfo::should_collect_detailed_settings(core_info),
        context,
        ClickhouseSettingsInfo::QUERY_TIMEOUT,
        |detail_queries, mut detailed_settings| async move {
            detail_queries
                .assign_sql_if(
                    has_inconsistent_settings,
                    &mut detailed_settings.inconsistent_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_INCONSISTENT_SETTINGS,
                    || {
                        format!(
                            "SELECT
                    name, groupArray(value) as values, groupArray(hostName()) as hosts
                    FROM cluster('default', system.settings)
                    GROUP BY name
                    HAVING count(DISTINCT value) > 1
                    ORDER BY name
                    LIMIT {}",
                            ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_inconsistent_settings,
                )
                .await?;

            detail_queries
                .assign_sql_if(
                    has_deprecated_settings,
                    &mut detailed_settings.deprecated_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_DEPRECATED_SETTINGS,
                    || {
                        format!(
                            "SELECT
                    name, value, `default` as default_value, description,
                    changed, type, readonly
                    FROM system.settings
                    WHERE name IN (
                        'use_uncompressed_cache',
                        'compile_expressions',
                        'min_count_to_compile_expression',
                        'group_by_overflow_mode',
                        'totals_mode',
                        'empty_result_for_aggregation_by_empty_set',
                        'force_index_by_date',
                        'force_primary_key'
                    ) AND value != `default`
                    ORDER BY name
                    LIMIT {}",
                            ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                        )
                    },
                    super::parsers::parse_deprecated_settings,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_settings.memory_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_MEMORY_SETTINGS,
                    format!(
                        "SELECT
                name, value, `default` as default_value, description,
                changed, type, readonly
                FROM system.settings
                WHERE (name LIKE '%memory%' OR name LIKE '%Memory%' OR name LIKE '%cache%')
                    AND value != `default`
                ORDER BY name
                LIMIT {}",
                        ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_memory_settings,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_settings.performance_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_PERFORMANCE_SETTINGS,
                    format!(
                        "SELECT
                name, value, `default` as default_value, description,
                changed, type, readonly
                FROM system.settings
                WHERE (name LIKE '%thread%' OR name LIKE '%Thread%' OR
                       name LIKE '%parallel%' OR name LIKE '%Parallel%' OR
                       name LIKE '%async%' OR name LIKE '%Async%' OR
                       name LIKE '%queue%' OR name LIKE '%Queue%')
                    AND value != `default`
                ORDER BY name
                LIMIT {}",
                        ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_performance_settings,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_settings.security_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_SECURITY_SETTINGS,
                    format!(
                        "SELECT
                name, value, `default` as default_value, description,
                changed, type, readonly
                FROM system.settings
                WHERE (name LIKE '%security%' OR name LIKE '%Security%' OR
                       name LIKE '%auth%' OR name LIKE '%Auth%' OR
                       name LIKE '%ssl%' OR name LIKE '%SSL%' OR
                       name LIKE '%tls%' OR name LIKE '%TLS%' OR
                       name LIKE '%password%' OR name LIKE '%Password%')
                    AND value != `default`
                ORDER BY name
                LIMIT {}",
                        ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_security_settings,
                )
                .await?;

            detail_queries
                .assign_sql(
                    &mut detailed_settings.resource_limit_settings,
                    ClickhouseSettingsInfo::DETAIL_QUERY_RESOURCE_SETTINGS,
                    format!(
                        "SELECT
                name, value, `default` as default_value, description,
                changed, type, readonly
                FROM system.settings
                WHERE (name LIKE '%max_%' OR name LIKE '%limit%' OR name LIKE '%Limit%' OR
                       name LIKE '%timeout%' OR name LIKE '%Timeout%')
                    AND value != `default`
                ORDER BY name
                LIMIT {}",
                        ClickhouseSettingsInfo::MAX_DETAILED_RESULTS
                    ),
                    super::parsers::parse_resource_limit_settings,
                )
                .await?;

            detailed_settings.optimization_recommendations = ClickhouseSettingsInfo::generate_optimization_recommendations(core_info);
            detailed_settings.dangerous_settings = ClickhouseSettingsInfo::identify_dangerous_settings(core_info, &detailed_settings);

            Ok(detailed_settings)
        },
    )
    .await
}
