use super::*;

impl OracleTablespaceInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleTablespaceInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleTablespaceDetailedMetrics>> {
        let needs_details = core_info.critical_usage_tablespaces > 0
            || core_info.high_usage_tablespaces > 0
            || core_info.offline_tablespaces > 0
            || core_info.autoextend_coverage() < 70.0;

        if !crate::metadata::stc::utils::should_collect(&[needs_details]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleTablespaceDetailedMetrics {
            problem_tablespaces: Vec::new(),
            datafile_analysis: None,
            usage_trends: None,
            autoextend_analysis: None,
            fragmentation_analysis: None,
        };

        let problem_tablespaces_input = crate::metadata::stc::utils::query_with_limit(
            format!(
                "SELECT
                    ts.tablespace_name,
                    ts.contents,
                    ts.status,
                    ts.logging,
                    ts.force_logging,
                    ts.extent_management,
                    ts.allocation_type,
                    ts.bigfile,
                    NVL(df.total_bytes, 0) as total_bytes,
                    NVL(df.total_bytes - fs.free_bytes, 0) as used_bytes,
                    NVL(fs.free_bytes, 0) as free_bytes,
                    NVL(df.max_bytes, 0) as max_bytes,
                    CASE
                        WHEN NVL(df.total_bytes, 0) > 0 THEN
                            ROUND(((NVL(df.total_bytes, 0) - NVL(fs.free_bytes, 0)) / NVL(df.total_bytes, 1)) * 100, 2)
                        ELSE 0
                    END as usage_percent,
                    NVL(df.file_count, 0) as datafile_count,
                    NVL(df.autoextend_count, 0) as autoextend_count,
                    ROUND(NVL(df.total_bytes, 0) / 1024 / 1024 / 1024, 2) as total_gb,
                    ROUND(NVL(df.total_bytes - fs.free_bytes, 0) / 1024 / 1024 / 1024, 2) as used_gb,
                    ROUND(NVL(fs.free_bytes, 0) / 1024 / 1024 / 1024, 2) as free_gb,
                    CASE
                        WHEN ts.status = 'OFFLINE' THEN 'CRITICAL'
                        WHEN NVL(df.total_bytes, 0) > 0 AND
                             ((NVL(df.total_bytes, 0) - NVL(fs.free_bytes, 0)) / NVL(df.total_bytes, 1)) * 100 > {} THEN 'CRITICAL'
                        WHEN NVL(df.total_bytes, 0) > 0 AND
                             ((NVL(df.total_bytes, 0) - NVL(fs.free_bytes, 0)) / NVL(df.total_bytes, 1)) * 100 > {} THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as issue_severity
                FROM dba_tablespaces ts
                LEFT JOIN (
                    SELECT tablespace_name,
                           SUM(bytes) as total_bytes,
                           SUM(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END) as max_bytes,
                           COUNT(*) as file_count,
                           COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_count
                    FROM dba_data_files
                    GROUP BY tablespace_name
                    UNION ALL
                    SELECT tablespace_name,
                           SUM(bytes) as total_bytes,
                           SUM(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END) as max_bytes,
                           COUNT(*) as file_count,
                           COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_count
                    FROM dba_temp_files
                    GROUP BY tablespace_name
                ) df ON ts.tablespace_name = df.tablespace_name
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) as free_bytes
                    FROM dba_free_space
                    GROUP BY tablespace_name
                ) fs ON ts.tablespace_name = fs.tablespace_name
                WHERE (ts.status = 'OFFLINE'
                    OR (NVL(df.total_bytes, 0) > 0 AND
                        ((NVL(df.total_bytes, 0) - NVL(fs.free_bytes, 0)) / NVL(df.total_bytes, 1)) * 100 > {}))
                ORDER BY
                    CASE issue_severity
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'WARNING' THEN 2
                        ELSE 3
                    END,
                    usage_percent DESC",
                Self::CRITICAL_USAGE_THRESHOLD,
                Self::HIGH_USAGE_THRESHOLD,
                Self::HIGH_USAGE_THRESHOLD,
            ),
            Self::MAX_DETAILED_RESULTS,
        );

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.problem_tablespaces,
            &problem_tablespaces_input,
            context.clone(),
            Self::QUERY_TIMEOUT,
            "problem_tablespaces",
            Self::parse_tablespace_details,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            core_info.high_usage_datafiles > 0 || core_info.autoextend_coverage() < 50.0,
            &mut detailed_metrics.datafile_analysis,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        file_id,
                        file_name,
                        tablespace_name,
                        bytes,
                        CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END as max_bytes,
                        autoextensible,
                        increment_by,
                        status,
                        online_status,
                        ROUND(bytes / 1024 / 1024 / 1024, 2) as size_gb,
                        ROUND(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END / 1024 / 1024 / 1024, 2) as max_gb,
                        CASE
                            WHEN bytes > 0 THEN ROUND((bytes / GREATEST(bytes, 1)) * 100, 2)
                            ELSE 0
                        END as usage_percent
                    FROM dba_data_files
                    WHERE autoextensible = 'NO' OR status != 'AVAILABLE'
                    UNION ALL
                    SELECT
                        file_id,
                        file_name,
                        tablespace_name,
                        bytes,
                        CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END as max_bytes,
                        autoextensible,
                        increment_by,
                        status,
                        online_status,
                        ROUND(bytes / 1024 / 1024 / 1024, 2) as size_gb,
                        ROUND(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END / 1024 / 1024 / 1024, 2) as max_gb,
                        CASE
                            WHEN bytes > 0 THEN ROUND((bytes / GREATEST(bytes, 1)) * 100, 2)
                            ELSE 0
                        END as usage_percent
                    FROM dba_temp_files
                    WHERE autoextensible = 'NO' OR status != 'AVAILABLE'
                    ORDER BY usage_percent DESC, size_gb DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "datafile_analysis",
            Self::parse_datafile_details,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
