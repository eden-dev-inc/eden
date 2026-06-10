use super::*;
use function_name::named;

impl OracleSegmentInfo {
    pub(crate) const HIGH_FRAGMENTATION_THRESHOLD: u64 = 50; // segments with >50 extents
    pub(crate) const HIGH_GROWTH_THRESHOLD: u64 = 10; // >10 growing segments
    pub(crate) const HIGH_SPACE_USAGE_THRESHOLD: f64 = 85.0; // >85% space usage
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10); // Longer timeout for segment queries
    pub(crate) const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut segment_info = OracleSegmentInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "segment_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            segment_info.total_segments = row.get_u64("total_segments")?;
            segment_info.table_segments = row.get_u64("table_segments")?;
            segment_info.index_segments = row.get_u64("index_segments")?;
            segment_info.lob_segments = row.get_u64("lob_segments")?;
            segment_info.temp_segments = row.get_u64("temp_segments")?;
            segment_info.total_allocated_space = row.get_u64("total_allocated_space")?;
            segment_info.largest_segment_size = row.get_u64("largest_segment_size")?;
            segment_info.large_segments_count = row.get_u64("large_segments_count")?;
            segment_info.total_extents = row.get_u64("total_extents")?;
            segment_info.avg_extent_size = row.get_u64("avg_extent_size")?;
        }

        if let Some(row) = run_single_row(&requests, "space_usage", context.clone(), Self::QUERY_TIMEOUT).await? {
            segment_info.total_used_space = row.get_u64("total_used_space")?;
            segment_info.total_free_space = row.get_u64("total_free_space")?;
            segment_info.tablespaces_with_issues = row.get_u64("tablespaces_with_issues")?;

            let total_tablespace_size = row.get_u64("total_tablespace_size")?;
            segment_info.tablespace_utilization_pct = if total_tablespace_size > 0 {
                (segment_info.total_used_space as f64 / total_tablespace_size as f64) * 100.0
            } else {
                0.0
            };
        }

        segment_info.space_utilization_pct = if segment_info.total_allocated_space > 0 {
            (segment_info.total_used_space as f64 / segment_info.total_allocated_space as f64) * 100.0
        } else {
            0.0
        };

        if let Some(row) = run_single_row(&requests, "growth_analysis", context.clone(), Self::QUERY_TIMEOUT).await? {
            segment_info.growing_segments_count = row.get_u64("growing_segments_count")?;
            segment_info.space_allocated_24h = row.get_u64("space_allocated_24h")?;
        }

        if let Some(row) = run_single_row(&requests, "fragmentation_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            segment_info.fragmented_segments_count = row.get_u64("fragmented_segments_count")?;
            segment_info.fragmentation_waste = row.get_u64("fragmentation_waste")?;
        }

        if let Some(row) = run_single_row(&requests, "chaining_analysis", context.clone(), Self::QUERY_TIMEOUT).await? {
            segment_info.chained_segments_count = row.get_u64("chained_segments_count")?;
        }

        segment_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&segment_info, context).await?;
        Ok(segment_info)
    }
}
