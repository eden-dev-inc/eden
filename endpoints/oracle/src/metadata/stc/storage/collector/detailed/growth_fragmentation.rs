use super::*;

impl OracleStorageInfo {
    pub(crate) async fn collect_growth_analysis(detailed_metrics: &mut OracleStorageDetailedMetrics, context: OracleAsync) -> ResultEP<()> {
        let growth_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    tablespace_name,
                    SUM(increment_by * 8192) as potential_growth_bytes,
                    COUNT(*) as autoextend_files,
                    AVG(increment_by * 8192) as avg_increment_size,
                    MAX(increment_by * 8192) as max_increment_size,
                    ROUND(SUM(increment_by * 8192) / 1024 / 1024, 2) as potential_growth_mb
                FROM dba_data_files
                WHERE autoextensible = 'YES'
                   AND increment_by > 0
                GROUP BY tablespace_name
                ORDER BY potential_growth_bytes DESC
                FETCH FIRST {} ROWS ONLY",
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.growth_analysis,
            &growth_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "growth_analysis",
            Self::parse_growth_analysis,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn collect_fragmentation_analysis(
        detailed_metrics: &mut OracleStorageDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let fragmentation_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    tablespace_name,
                    COUNT(*) as extent_count,
                    AVG(bytes) as avg_extent_size,
                    MIN(bytes) as min_extent_size,
                    MAX(bytes) as max_extent_size,
                    COUNT(CASE WHEN bytes < 65536 THEN 1 END) as small_extents,
                    SUM(CASE WHEN bytes < 65536 THEN bytes ELSE 0 END) as small_extent_bytes,
                    ROUND(AVG(bytes) / 1024, 2) as avg_extent_kb
                FROM dba_extents
                GROUP BY tablespace_name
                HAVING COUNT(*) > 100
                ORDER BY extent_count DESC
                FETCH FIRST {} ROWS ONLY",
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.fragmentation_analysis,
            &fragmentation_query,
            context,
            Self::QUERY_TIMEOUT,
            "fragmentation_analysis",
            Self::parse_fragmentation_analysis,
        )
        .await?;

        Ok(())
    }
}
