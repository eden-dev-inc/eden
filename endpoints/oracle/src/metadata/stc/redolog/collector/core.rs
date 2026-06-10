use super::*;
use function_name::named;

impl OracleRedoLogInfo {
    pub(crate) const HIGH_SWITCH_FREQUENCY_THRESHOLD: u64 = 12;
    pub(crate) const HIGH_ARCHIVE_LAG_THRESHOLD: f64 = 300.0;
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut redo_info = OracleRedoLogInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "core_redo_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            redo_info.current_sequence = row.get_u64("current_sequence")?;
            redo_info.total_log_groups = row.get_u64("total_log_groups")?;
            redo_info.active_log_groups = row.get_u64("active_log_groups")?;
            redo_info.inactive_log_groups = row.get_u64("inactive_log_groups")?;
            redo_info.current_log_group = row.get_u64("current_log_group")?;
            redo_info.log_file_size = row.get_u64("log_file_size")?;
            redo_info.redo_size_today = row.get_u64("redo_size_today")?;
        }

        if let Some(row) = run_single_row(&requests, "redo_performance", context.clone(), Self::QUERY_TIMEOUT).await? {
            redo_info.avg_redo_write_time = row.get_f64("avg_redo_write_time")?;
            redo_info.log_buffer_hit_ratio = row.get_f64("log_buffer_hit_ratio")?;
            redo_info.time_since_last_switch = row.get_f64("time_since_last_switch")?;
        }

        if let Some(row) = run_single_row(&requests, "log_switch_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            redo_info.switches_last_hour = row.get_u64("switches_last_hour")?;
            redo_info.log_switch_frequency = row.get_f64("log_switch_frequency")?;
        }

        if let Some(row) = run_single_row(&requests, "scn_info", context.clone(), Self::QUERY_TIMEOUT).await? {
            redo_info.current_scn = row.get_u64("current_scn")?;
            redo_info.checkpoint_scn = row.get_u64("checkpoint_scn")?;
            redo_info.scn_gap = row.get_u64("scn_gap")?;
        }

        if let Some(row) = run_single_row(&requests, "archive_info", context.clone(), Self::QUERY_TIMEOUT).await? {
            redo_info.archive_lag_seconds = row.get_f64("archive_lag_seconds")?;
            redo_info.pending_archive_count = row.get_u64("pending_archive_count")?;
        }

        if redo_info.time_since_last_switch > 0.0 && redo_info.log_file_size > 0 {
            redo_info.redo_generation_rate = redo_info.log_file_size as f64 / redo_info.time_since_last_switch;
        }

        redo_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&redo_info, context).await?;
        Ok(redo_info)
    }
}
