use super::ClickhouseDictionaryInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseDictionaryInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseDictionaryInfo> {
    let mut dictionary_info = ClickhouseDictionaryInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseDictionaryInfo::QUERY_TIMEOUT);

    let (dictionary_overview_row, dictionary_performance_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseDictionaryInfo::QUERY_DICTIONARY_OVERVIEW),
        metadata_queries.row(ClickhouseDictionaryInfo::QUERY_DICTIONARY_PERFORMANCE),
    )?;

    if let Some(row) = dictionary_overview_row {
        dictionary_info.total_dictionaries = row.u64_or_zero("total_dictionaries")?;
        dictionary_info.loaded_dictionaries = row.u64_or_zero("loaded_dictionaries")?;
        dictionary_info.failed_dictionaries = row.u64_or_zero("failed_dictionaries")?;
        dictionary_info.loading_dictionaries = row.u64_or_zero("loading_dictionaries")?;
        dictionary_info.total_memory_usage = row.u64_or_zero("total_memory_usage")?;
        dictionary_info.total_elements = row.u64_or_zero("total_elements")?;
        dictionary_info.avg_load_time = row.f64_or_zero("avg_load_time")?;
        dictionary_info.external_dictionaries = row.u64_or_zero("external_dictionaries")?;
    }

    if let Some(row) = dictionary_performance_row {
        dictionary_info.total_cache_hits = row.u64_or_zero("total_cache_hits")?;
        dictionary_info.total_cache_misses = row.u64_or_zero("total_cache_misses")?;
        dictionary_info.high_performance_dictionaries = row.u64_or_zero("high_performance_dictionaries")?;
        dictionary_info.low_performance_dictionaries = row.u64_or_zero("low_performance_dictionaries")?;
        dictionary_info.dictionaries_needing_reload = row.u64_or_zero("dictionaries_needing_reload")?;
    }

    dictionary_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&dictionary_info, context).await?;

    Ok(dictionary_info)
}
