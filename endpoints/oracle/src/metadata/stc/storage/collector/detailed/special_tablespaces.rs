use super::*;

impl OracleStorageInfo {
    pub(crate) async fn collect_special_tablespaces(
        detailed_metrics: &mut OracleStorageDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let special_tablespaces_query = crate::metadata::stc::utils::query(
            "SELECT
                ts.tablespace_name,
                ts.contents,
                ts.status,
                CASE
                    WHEN ts.contents = 'UNDO' THEN 'UNDO'
                    WHEN ts.contents = 'TEMPORARY' THEN 'TEMP'
                    ELSE 'OTHER'
                END as tablespace_type,
                COALESCE(df_size.bytes, tf_size.bytes, 0) as total_size,
                COALESCE(df_used.used_bytes, tf_used.used_bytes, 0) as used_size,
                ROUND((COALESCE(df_used.used_bytes, tf_used.used_bytes, 0) /
                       COALESCE(df_size.bytes, tf_size.bytes, 1)) * 100, 2) as usage_pct
            FROM dba_tablespaces ts
            LEFT JOIN (
                SELECT tablespace_name, SUM(bytes) as bytes
                FROM dba_data_files
                GROUP BY tablespace_name
            ) df_size ON ts.tablespace_name = df_size.tablespace_name
            LEFT JOIN (
                SELECT tablespace_name, SUM(bytes) as bytes
                FROM dba_temp_files
                GROUP BY tablespace_name
            ) tf_size ON ts.tablespace_name = tf_size.tablespace_name
            LEFT JOIN (
                SELECT tablespace_name, SUM(bytes) - SUM(NVL(free_bytes, 0)) as used_bytes
                FROM (
                    SELECT df.tablespace_name, df.bytes, fs.bytes as free_bytes
                    FROM dba_data_files df
                    LEFT JOIN dba_free_space fs ON df.file_id = fs.file_id
                )
                GROUP BY tablespace_name
            ) df_used ON ts.tablespace_name = df_used.tablespace_name
            LEFT JOIN (
                SELECT tablespace, SUM(blocks * 8192) as used_bytes
                FROM v$tempseg_usage
                GROUP BY tablespace
            ) tf_used ON ts.tablespace_name = tf_used.tablespace
            WHERE ts.contents IN ('UNDO', 'TEMPORARY')
               OR ts.tablespace_name LIKE '%UNDO%'
               OR ts.tablespace_name LIKE '%TEMP%'
            ORDER BY usage_pct DESC"
                .to_string(),
        );

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.special_tablespaces,
            &special_tablespaces_query,
            context,
            Self::QUERY_TIMEOUT,
            "special_tablespaces",
            Self::parse_special_tablespaces,
        )
        .await?;

        Ok(())
    }
}
