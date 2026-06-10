use super::*;

mod growth;
mod index_partition;
mod stats_lob_constraint;
mod table_details;

impl OracleTableInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleTableInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleTableDetailedMetrics>> {
        let needs_table_details = core_info.tables_stale_stats > 0 || core_info.tables_no_stats > 0 || core_info.large_tables > 10;
        let needs_index_details = core_info.unusable_indexes > 0 || core_info.invisible_indexes > 5;
        let needs_partition_details = core_info.partitioned_tables > 0 && core_info.total_partitions > 50;
        let needs_statistics_details = core_info.has_stale_statistics(Self::STALE_STATS_THRESHOLD);
        let needs_lob_details = core_info.tables_with_lobs > 0 && core_info.total_lob_size_bytes > 1_073_741_824;
        let needs_constraint_details = core_info.tables_with_fks > 0 || core_info.tables_with_checks > 0;
        let needs_growth_details = core_info.high_growth_tables > 0 || core_info.high_activity_tables > 10;

        if !crate::metadata::stc::utils::should_collect(&[
            needs_table_details,
            needs_index_details,
            needs_partition_details,
            needs_statistics_details,
            needs_lob_details,
            needs_constraint_details,
            needs_growth_details,
        ]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleTableDetailedMetrics {
            problem_tables: Vec::new(),
            index_analysis: None,
            partition_analysis: None,
            statistics_analysis: None,
            lob_analysis: None,
            constraint_analysis: None,
            growth_analysis: None,
        };

        Self::collect_problem_tables(&mut detailed_metrics, context.clone()).await?;

        if needs_index_details {
            Self::collect_index_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_partition_details {
            Self::collect_partition_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_statistics_details {
            Self::collect_statistics_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_lob_details {
            Self::collect_lob_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_constraint_details {
            Self::collect_constraint_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_growth_details {
            Self::collect_growth_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        Ok(Some(detailed_metrics))
    }
}
