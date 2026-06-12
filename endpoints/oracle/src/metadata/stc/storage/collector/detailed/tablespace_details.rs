use super::*;

impl OracleStorageInfo {
    pub(crate) async fn collect_problem_tablespaces(
        detailed_metrics: &mut OracleStorageDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let problem_tablespaces_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    ts.tablespace_name,
                    ts.status,
                    ts.contents,
                    ts.extent_management,
                    ts.allocation_type,
                    ts_size.bytes as total_size,
                    ts_size.bytes - NVL(ts_free.bytes, 0) as used_size,
                    NVL(ts_free.bytes, 0) as free_size,
                    ROUND(((ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes) * 100, 2) as usage_pct,
                    NVL(ts_free.max_free, 0) as largest_free_extent,
                    ts_size.file_count,
                    ts_size.autoextend_count,
                    CASE
                        WHEN ((ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes) > 0.95 THEN 'CRITICAL'
                        WHEN ((ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes) > 0.85 THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as alert_level
                FROM dba_tablespaces ts
                LEFT JOIN (
                    SELECT
                        tablespace_name,
                        SUM(bytes) as bytes,
                        COUNT(*) as file_count,
                        COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_count
                    FROM dba_data_files
                    GROUP BY tablespace_name
                ) ts_size ON ts.tablespace_name = ts_size.tablespace_name
                LEFT JOIN (
                    SELECT
                        tablespace_name,
                        SUM(bytes) as bytes,
                        MAX(bytes) as max_free
                    FROM dba_free_space
                    GROUP BY tablespace_name
                ) ts_free ON ts.tablespace_name = ts_free.tablespace_name
                WHERE ts.contents != 'TEMPORARY'
                   AND ts_size.bytes IS NOT NULL
                   AND ((ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes) > 0.70
                ORDER BY usage_pct DESC
                FETCH FIRST {} ROWS ONLY",
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.problem_tablespaces,
            &problem_tablespaces_query,
            context,
            Self::QUERY_TIMEOUT,
            "problem_tablespaces",
            Self::parse_tablespace_details,
        )
        .await?;

        Ok(())
    }
}
