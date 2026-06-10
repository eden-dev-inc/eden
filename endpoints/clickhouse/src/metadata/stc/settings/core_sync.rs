use super::ClickhouseSettingsInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(crate) async fn sync_metadata(settings: &ClickhouseSettingsInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseSettingsInfo> {
    let mut settings_info = ClickhouseSettingsInfo::default();
    let requests = settings.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseSettingsInfo::QUERY_TIMEOUT);

    let (settings_overview_row, memory_limits_row, timeout_settings_row, cluster_settings_consistency_row, deprecated_settings_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseSettingsInfo::QUERY_SETTINGS_OVERVIEW),
        metadata_queries.row(ClickhouseSettingsInfo::QUERY_MEMORY_LIMITS),
        metadata_queries.row(ClickhouseSettingsInfo::QUERY_TIMEOUT_SETTINGS),
        metadata_queries.row(ClickhouseSettingsInfo::QUERY_CLUSTER_SETTINGS_CONSISTENCY),
        metadata_queries.row(ClickhouseSettingsInfo::QUERY_DEPRECATED_SETTINGS_COUNT),
    )?;

    if let Some(row) = settings_overview_row {
        settings_info.total_settings_count = row.u64_or_zero("total_settings_count")?;
        settings_info.custom_settings_count = row.u64_or_zero("custom_settings_count")?;
        settings_info.memory_settings_count = row.u64_or_zero("memory_settings_count")?;
        settings_info.performance_settings_count = row.u64_or_zero("performance_settings_count")?;
    }

    if let Some(row) = memory_limits_row {
        settings_info.total_memory_limit = row.u64_or_zero("max_memory_usage")?;
        settings_info.max_query_memory_limit = row.u64_or_zero("max_query_memory_usage")?;
        settings_info.max_threads = row.u64_or_zero("max_threads")?;
        settings_info.max_connections = row.u64_or_zero("max_connections")?;
    }

    if let Some(row) = timeout_settings_row {
        settings_info.query_timeout_seconds = row.u64_or_zero("query_timeout")?;
    }

    if let Some(row) = cluster_settings_consistency_row {
        settings_info.inconsistent_settings_count = row.u64_or_zero("inconsistent_settings_count")?;
    }

    if let Some(row) = deprecated_settings_row {
        settings_info.deprecated_settings_count = row.u64_or_zero("deprecated_settings_count")?;
    }

    settings_info.dangerous_settings_count = ClickhouseSettingsInfo::calculate_dangerous_settings_count(&settings_info);
    settings_info.security_settings_count = ClickhouseSettingsInfo::calculate_security_settings_count(&settings_info);
    settings_info.settings_needing_optimization = ClickhouseSettingsInfo::calculate_optimization_needs(&settings_info);
    settings_info.detailed_settings = super::detailed_sync::collect_detailed_settings_if_needed(&settings_info, context).await?;

    Ok(settings_info)
}
