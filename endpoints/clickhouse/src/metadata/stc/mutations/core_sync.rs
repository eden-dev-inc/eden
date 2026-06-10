use super::ClickhouseMutationInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseMutationInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseMutationInfo> {
    let mut mutation_info = ClickhouseMutationInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseMutationInfo::QUERY_TIMEOUT);

    let (mutation_overview_row, mutation_progress_row, recent_mutation_activity_row, stuck_mutations_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseMutationInfo::QUERY_MUTATION_OVERVIEW),
        metadata_queries.row(ClickhouseMutationInfo::QUERY_MUTATION_PROGRESS),
        metadata_queries.row(ClickhouseMutationInfo::QUERY_RECENT_MUTATION_ACTIVITY),
        metadata_queries.row(ClickhouseMutationInfo::QUERY_STUCK_MUTATIONS),
    )?;

    if let Some(row) = mutation_overview_row {
        mutation_info.total_mutations = row.u64_or_zero("total_mutations")?;
        mutation_info.active_mutations = row.u64_or_zero("active_mutations")?;
        mutation_info.completed_mutations = row.u64_or_zero("completed_mutations")?;
        mutation_info.failed_mutations = row.u64_or_zero("failed_mutations")?;
        mutation_info.waiting_mutations = row.u64_or_zero("waiting_mutations")?;
        mutation_info.avg_completion_time = row.f64_or_zero("avg_completion_time")?;
    }

    if let Some(row) = mutation_progress_row {
        mutation_info.total_parts_to_mutate = row.u64_or_zero("total_parts_to_mutate")?;
        mutation_info.total_parts_mutated = row.u64_or_zero("total_parts_mutated")?;
        mutation_info.longest_mutation_duration = row.f64_or_zero("longest_mutation_duration")?;
        mutation_info.avg_mutation_progress = row.f64_or_zero("avg_mutation_progress")?;
        mutation_info.tables_with_active_mutations = row.u64_or_zero("tables_with_active_mutations")?;
    }

    if let Some(row) = recent_mutation_activity_row {
        mutation_info.failed_mutations_last_24h = row.u64_or_zero("failed_mutations_last_24h")?;
        mutation_info.completed_mutations_last_hour = row.u64_or_zero("completed_mutations_last_hour")?;
    }

    if let Some(row) = stuck_mutations_row {
        mutation_info.stuck_mutations = row.u64_or_zero("stuck_mutations")?;
    }

    mutation_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&mutation_info, context).await?;

    Ok(mutation_info)
}
