use super::*;

impl OracleSegmentInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleSegmentInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleSegmentDetailedMetrics>> {
        let needs_largest_segments =
            core_info.space_utilization_pct > Self::HIGH_SPACE_USAGE_THRESHOLD || core_info.large_segments_count > 20;
        let needs_fragmentation_details = core_info.fragmented_segments_count > Self::HIGH_FRAGMENTATION_THRESHOLD;
        let needs_growth_details = core_info.growing_segments_count > Self::HIGH_GROWTH_THRESHOLD;
        let needs_tablespace_details = core_info.tablespaces_with_issues > 0;
        let needs_chaining_details = core_info.chained_segments_count > 10;

        if !crate::metadata::stc::utils::should_collect(&[
            needs_largest_segments,
            needs_fragmentation_details,
            needs_growth_details,
            needs_tablespace_details,
            needs_chaining_details,
        ]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleSegmentDetailedMetrics {
            largest_segments: Vec::new(),
            fragmented_segments: None,
            growing_segments: None,
            tablespace_issues: None,
            chained_segments: None,
        };

        let largest_segments_query = crate::metadata::stc::utils::query_with_limit(
            "SELECT
                    owner,
                    segment_name,
                    segment_type,
                    tablespace_name,
                    bytes,
                    extents,
                    initial_extent,
                    next_extent,
                    max_extents,
                    ROUND(bytes / 1024 / 1024, 2) as size_mb,
                    CASE WHEN extents > 100 THEN 'HIGH'
                         WHEN extents > 50 THEN 'MEDIUM'
                         ELSE 'LOW' END as fragmentation_level
                FROM dba_segments
                WHERE bytes > 104857600
                ORDER BY bytes DESC"
                .to_string(),
            Self::MAX_DETAILED_RESULTS,
        );

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.largest_segments,
            &largest_segments_query,
            context.clone(),
            Self::QUERY_TIMEOUT,
            "largest_segments",
            Self::parse_segment_details,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_fragmentation_details,
            &mut detailed_metrics.fragmented_segments,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        owner,
                        segment_name,
                        segment_type,
                        tablespace_name,
                        bytes,
                        extents,
                        max_extents,
                        initial_extent,
                        next_extent,
                        ROUND(bytes / extents, 0) as avg_extent_size,
                        ROUND((extents - 1) * initial_extent / 1024 / 1024, 2) as wasted_space_mb
                    FROM dba_segments
                    WHERE extents > 100
                       AND (segment_type LIKE 'TABLE%' OR segment_type LIKE 'INDEX%')
                    ORDER BY extents DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "fragmented_segments",
            Self::parse_fragmented_segments,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_growth_details,
            &mut detailed_metrics.growing_segments,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        s.owner,
                        s.segment_name,
                        s.segment_type,
                        s.tablespace_name,
                        s.bytes as current_size,
                        s.extents as current_extents,
                        COUNT(e.extent_id) as new_extents_24h,
                        SUM(e.bytes) as growth_bytes_24h,
                        ROUND(SUM(e.bytes) / 1024 / 1024, 2) as growth_mb_24h
                    FROM dba_segments s
                    JOIN dba_extents e ON s.owner = e.owner
                                       AND s.segment_name = e.segment_name
                                       AND s.segment_type = e.segment_type
                    WHERE e.extent_id >= s.extents - 5
                    GROUP BY s.owner, s.segment_name, s.segment_type, s.tablespace_name, s.bytes, s.extents
                    HAVING COUNT(e.extent_id) > 0
                    ORDER BY SUM(e.bytes) DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "growing_segments",
            Self::parse_growing_segments,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_tablespace_details,
            &mut detailed_metrics.tablespace_issues,
            || {
                crate::metadata::stc::utils::query(
                    "SELECT
                    ts.tablespace_name,
                    ts.total_size_mb,
                    ts.used_size_mb,
                    ts.free_size_mb,
                    ts.usage_pct,
                    ts.largest_free_mb,
                    CASE WHEN ts.usage_pct > 95 THEN 'CRITICAL'
                         WHEN ts.usage_pct > 90 THEN 'WARNING'
                         ELSE 'NORMAL' END as status
                FROM (
                    SELECT
                        df.tablespace_name,
                        ROUND(SUM(df.bytes) / 1024 / 1024, 2) as total_size_mb,
                        ROUND((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / 1024 / 1024, 2) as used_size_mb,
                        ROUND(NVL(SUM(fs.bytes), 0) / 1024 / 1024, 2) as free_size_mb,
                        ROUND(((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / SUM(df.bytes)) * 100, 2) as usage_pct,
                        ROUND(NVL(MAX(fs.bytes), 0) / 1024 / 1024, 2) as largest_free_mb
                    FROM dba_data_files df
                    LEFT JOIN dba_free_space fs ON df.tablespace_name = fs.tablespace_name
                    GROUP BY df.tablespace_name
                ) ts
                WHERE ts.usage_pct > 85
                ORDER BY ts.usage_pct DESC"
                        .to_string(),
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "tablespace_issues",
            Self::parse_tablespace_issues,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_chaining_details,
            &mut detailed_metrics.chained_segments,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        owner,
                        table_name,
                        tablespace_name,
                        num_rows,
                        chain_cnt,
                        avg_row_len,
                        blocks,
                        avg_space,
                        CASE WHEN num_rows > 0 THEN ROUND((chain_cnt / num_rows) * 100, 2) ELSE 0 END as chain_pct
                    FROM dba_tables
                    WHERE chain_cnt > 0
                       AND num_rows > 1000
                    ORDER BY chain_cnt DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "chained_segments",
            Self::parse_chained_segments,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
