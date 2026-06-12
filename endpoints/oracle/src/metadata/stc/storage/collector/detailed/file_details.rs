use super::*;

impl OracleStorageInfo {
    pub(crate) async fn collect_datafile_details(
        detailed_metrics: &mut OracleStorageDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let datafile_details_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    file_name,
                    file_id,
                    tablespace_name,
                    bytes,
                    maxbytes,
                    increment_by,
                    autoextensible,
                    status,
                    ROUND(bytes / 1024 / 1024, 2) as size_mb,
                    CASE
                        WHEN maxbytes > 0 THEN ROUND((bytes / maxbytes) * 100, 2)
                        ELSE 0
                    END as pct_of_maxsize,
                    CASE
                        WHEN maxbytes > 0 AND bytes / maxbytes > 0.95 THEN 'CRITICAL'
                        WHEN maxbytes > 0 AND bytes / maxbytes > 0.85 THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as size_status
                FROM dba_data_files
                WHERE (maxbytes > 0 AND bytes / maxbytes > 0.80)
                   OR (autoextensible = 'YES' AND increment_by > 12800)
                ORDER BY pct_of_maxsize DESC, bytes DESC
                FETCH FIRST {} ROWS ONLY",
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.problem_datafiles,
            &datafile_details_query,
            context,
            Self::QUERY_TIMEOUT,
            "datafile_details",
            Self::parse_datafile_details,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn collect_file_limit_issues(
        detailed_metrics: &mut OracleStorageDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let file_limit_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    file_name,
                    tablespace_name,
                    bytes,
                    maxbytes,
                    increment_by,
                    ROUND(bytes / 1024 / 1024, 2) as current_size_mb,
                    ROUND(maxbytes / 1024 / 1024, 2) as max_size_mb,
                    ROUND((bytes / maxbytes) * 100, 2) as pct_of_max,
                    ROUND((maxbytes - bytes) / 1024 / 1024, 2) as remaining_mb,
                    CASE
                        WHEN bytes / maxbytes > 0.95 THEN 'CRITICAL'
                        WHEN bytes / maxbytes > 0.85 THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as risk_level
                FROM dba_data_files
                WHERE maxbytes > 0
                   AND autoextensible = 'YES'
                   AND bytes / maxbytes > 0.75
                UNION ALL
                SELECT
                    file_name,
                    tablespace_name,
                    bytes,
                    maxbytes,
                    increment_by,
                    ROUND(bytes / 1024 / 1024, 2) as current_size_mb,
                    ROUND(maxbytes / 1024 / 1024, 2) as max_size_mb,
                    ROUND((bytes / maxbytes) * 100, 2) as pct_of_max,
                    ROUND((maxbytes - bytes) / 1024 / 1024, 2) as remaining_mb,
                    CASE
                        WHEN bytes / maxbytes > 0.95 THEN 'CRITICAL'
                        WHEN bytes / maxbytes > 0.85 THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as risk_level
                FROM dba_temp_files
                WHERE maxbytes > 0
                   AND autoextensible = 'YES'
                   AND bytes / maxbytes > 0.75
                ORDER BY pct_of_max DESC
                FETCH FIRST {} ROWS ONLY",
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.file_limit_issues,
            &file_limit_query,
            context,
            Self::QUERY_TIMEOUT,
            "file_limit_issues",
            Self::parse_file_limit_issues,
        )
        .await?;

        Ok(())
    }
}
