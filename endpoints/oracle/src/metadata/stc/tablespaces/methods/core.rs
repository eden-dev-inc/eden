use super::*;
use function_name::named;

impl OracleTablespaceInfo {
    pub(crate) const HIGH_USAGE_THRESHOLD: f64 = 80.0;
    pub(crate) const CRITICAL_USAGE_THRESHOLD: f64 = 95.0;
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut tablespace_info = OracleTablespaceInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "tablespace_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            tablespace_info.total_tablespaces = row.get_u64("total_tablespaces")?;
            tablespace_info.temp_tablespaces = row.get_u64("temp_tablespaces")?;
            tablespace_info.undo_tablespaces = row.get_u64("undo_tablespaces")?;
            tablespace_info.permanent_tablespaces = row.get_u64("permanent_tablespaces")?;
            tablespace_info.total_allocated_bytes = row.get_u64("total_allocated_bytes")?;
            tablespace_info.total_used_bytes = row.get_u64("total_used_bytes")?;
            tablespace_info.total_free_bytes = row.get_u64("total_free_bytes")?;
            tablespace_info.total_max_bytes = row.get_u64("total_max_bytes")?;
            tablespace_info.avg_usage_percent = row.get_f64("avg_usage_percent")?;
            tablespace_info.high_usage_tablespaces = row.get_u64("high_usage_tablespaces")?;
            tablespace_info.critical_usage_tablespaces = row.get_u64("critical_usage_tablespaces")?;
            tablespace_info.offline_tablespaces = row.get_u64("offline_tablespaces")?;
            tablespace_info.readonly_tablespaces = row.get_u64("readonly_tablespaces")?;
            tablespace_info.largest_tablespace_bytes = row.get_u64("largest_tablespace_bytes")?;
            tablespace_info.bigfile_tablespaces = row.get_u64("bigfile_tablespaces")?;
            tablespace_info.locally_managed = row.get_u64("locally_managed")?;
            tablespace_info.dictionary_managed = row.get_u64("dictionary_managed")?;
            tablespace_info.uniform_extents = row.get_u64("uniform_extents")?;
        }

        if let Some(row) = run_single_row(&requests, "datafile_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            tablespace_info.total_datafiles = row.get_u64("total_datafiles")?;
            tablespace_info.autoextend_datafiles = row.get_u64("autoextend_datafiles")?;
            tablespace_info.autoextend_enabled = row.get_u64("autoextend_enabled")?;
            tablespace_info.high_usage_datafiles = row.get_u64("high_usage_datafiles")?;
        }

        tablespace_info.tablespace_health_score = Self::calculate_health_score(&tablespace_info);
        tablespace_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&tablespace_info, context).await?;

        Ok(tablespace_info)
    }

    pub(crate) fn calculate_health_score(tablespace_info: &OracleTablespaceInfo) -> f64 {
        let mut score = 100.0;

        if tablespace_info.critical_usage_tablespaces > 0 {
            score -= 40.0;
        }

        if tablespace_info.total_tablespaces > 0 {
            let high_usage_penalty = (tablespace_info.high_usage_tablespaces as f64 / tablespace_info.total_tablespaces as f64) * 20.0;
            score -= high_usage_penalty;
        }

        if tablespace_info.offline_tablespaces > 0 {
            score -= 25.0;
        }

        if tablespace_info.autoextend_coverage() < 50.0 {
            score -= 10.0;
        } else if tablespace_info.autoextend_coverage() < 80.0 {
            score -= 5.0;
        }

        if tablespace_info.locally_managed_percentage() < 80.0 {
            score -= 5.0;
        }

        score.clamp(0.0, 100.0)
    }
}
