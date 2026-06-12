use super::*;
use function_name::named;
impl OracleStorageInfo {
    #[allow(dead_code)]
    pub(crate) const HIGH_UTILIZATION_THRESHOLD: f64 = 85.0;
    #[allow(dead_code)]
    pub(crate) const CRITICAL_UTILIZATION_THRESHOLD: f64 = 95.0;
    pub(crate) const HIGH_GROWTH_THRESHOLD: u64 = 1_073_741_824; // 1GB growth in 24h
    pub(crate) const FRAGMENTATION_THRESHOLD: u64 = 1000; // Many small extents
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut storage_info = OracleStorageInfo::default();
        let requests = self.request();

        let tablespace_summary_rows = run_named_query(&requests, "tablespace_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        if let Some(row) = tablespace_summary_rows.first() {
            storage_info.total_tablespaces = row.get_u64("total_tablespaces")?;
            storage_info.online_tablespaces = row.get_u64("online_tablespaces")?;
            storage_info.offline_tablespaces = row.get_u64("offline_tablespaces")?;
            storage_info.readonly_tablespaces = row.get_u64("readonly_tablespaces")?;
            storage_info.total_allocated_storage = row.get_u64("total_allocated_storage")?;
            storage_info.total_used_storage = row.get_u64("total_used_storage")?;
            storage_info.total_free_space = row.get_u64("total_free_space")?;
            storage_info.largest_tablespace_size = row.get_u64("largest_tablespace_size")?;
            storage_info.tablespaces_warning = row.get_u64("tablespaces_warning")?;
            storage_info.tablespaces_critical = row.get_u64("tablespaces_critical")?;

            storage_info.storage_utilization_pct = ratio_percentage(storage_info.total_used_storage, storage_info.total_allocated_storage);
        }

        let datafile_rows = run_named_query(&requests, "datafile_summary", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = datafile_rows.first() {
            storage_info.total_data_files = row.get_u64("total_data_files")?;
            storage_info.autoextend_data_files = row.get_u64("autoextend_data_files")?;
            storage_info.files_near_maxsize = row.get_u64("files_near_maxsize")?;
        }

        let tempfile_rows = run_named_query(&requests, "tempfile_summary", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = tempfile_rows.first() {
            storage_info.total_temp_files = row.get_u64("total_temp_files")?;
            storage_info.total_temp_space = row.get_u64("total_temp_space")?;
        }

        let extent_rows = run_named_query(&requests, "extent_analysis", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = extent_rows.first() {
            storage_info.total_extents = row.get_u64("total_extents")?;
            storage_info.avg_extent_size = row.get_u64("avg_extent_size")?;
            storage_info.reclaimable_space = row.get_u64("small_extent_waste")?;
        }

        let undo_rows = run_named_query(&requests, "undo_analysis", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = undo_rows.first() {
            storage_info.total_undo_space = row.get_u64("total_undo_space")?;
            storage_info.used_undo_space = row.get_u64("used_undo_space")?;

            storage_info.undo_utilization_pct = ratio_percentage(storage_info.used_undo_space, storage_info.total_undo_space);
        }

        let temp_usage_rows = run_named_query(&requests, "temp_usage_analysis", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = temp_usage_rows.first() {
            let total_temp_from_usage = row.get_u64("total_temp_space")?;
            if total_temp_from_usage > 0 {
                storage_info.total_temp_space = total_temp_from_usage;
            }
            storage_info.used_temp_space = row.get_u64("used_temp_space")?;

            storage_info.temp_utilization_pct = ratio_percentage(storage_info.used_temp_space, storage_info.total_temp_space);
        }

        let growth_rows = run_named_query(&requests, "growth_tracking", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = growth_rows.first() {
            storage_info.autoextend_events_24h = row.get_u64("autoextend_events_24h")?;
            storage_info.storage_added_24h = row.get_u64("storage_added_24h")?;
        }

        // Conditionally collect detailed metrics only when problems are detected
        storage_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&storage_info, context).await?;

        Ok(storage_info)
    }
}
