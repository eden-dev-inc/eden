use super::*;

mod file_details;
mod growth_fragmentation;
mod special_tablespaces;
mod tablespace_details;

impl OracleStorageInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleStorageInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleStorageDetailedMetrics>> {
        let needs_tablespace_details = core_info.tablespaces_warning > 0 || core_info.tablespaces_critical > 0;
        let needs_datafile_details = core_info.files_near_maxsize > 0 || core_info.autoextend_events_24h > 10;
        let needs_growth_analysis = core_info.storage_added_24h > Self::HIGH_GROWTH_THRESHOLD;
        let needs_fragmentation_analysis = core_info.total_extents > Self::FRAGMENTATION_THRESHOLD;
        let needs_special_tablespace_details = core_info.undo_utilization_pct > 80.0 || core_info.temp_utilization_pct > 50.0;

        if !crate::metadata::stc::utils::should_collect(&[
            needs_tablespace_details,
            needs_datafile_details,
            needs_growth_analysis,
            needs_fragmentation_analysis,
            needs_special_tablespace_details,
        ]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleStorageDetailedMetrics {
            problem_tablespaces: Vec::new(),
            problem_datafiles: None,
            growth_analysis: None,
            fragmentation_analysis: None,
            special_tablespaces: None,
            file_limit_issues: None,
        };

        Self::collect_problem_tablespaces(&mut detailed_metrics, context.clone()).await?;

        if needs_datafile_details {
            Self::collect_datafile_details(&mut detailed_metrics, context.clone()).await?;
            Self::collect_file_limit_issues(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_growth_analysis {
            Self::collect_growth_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_fragmentation_analysis {
            Self::collect_fragmentation_analysis(&mut detailed_metrics, context.clone()).await?;
        }

        if needs_special_tablespace_details {
            Self::collect_special_tablespaces(&mut detailed_metrics, context.clone()).await?;
        }

        Ok(Some(detailed_metrics))
    }
}
