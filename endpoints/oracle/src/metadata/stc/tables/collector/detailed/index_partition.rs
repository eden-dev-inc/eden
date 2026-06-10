use super::*;

impl OracleTableInfo {
    pub(crate) async fn collect_index_analysis(detailed_metrics: &mut OracleTableDetailedMetrics, context: OracleAsync) -> ResultEP<()> {
        let index_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    i.owner,
                    i.index_name,
                    i.table_name,
                    i.index_type,
                    i.uniqueness,
                    i.status,
                    i.visibility,
                    i.degree,
                    i.compression,
                    i.distinct_keys,
                    i.leaf_blocks,
                    i.clustering_factor,
                    NVL(s.bytes, 0) as index_size_bytes,
                    ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) as index_size_mb,
                CASE
                    WHEN i.distinct_keys > 0 THEN ROUND(1.0 / i.distinct_keys, 6)
                    ELSE 0
                END as selectivity,
                TO_CHAR(i.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed
            FROM dba_indexes i
            {}
            WHERE i.owner {}
               AND (i.status = 'UNUSABLE' OR i.visibility = 'INVISIBLE' OR i.last_analyzed IS NULL)
            ORDER BY
                CASE i.status WHEN 'UNUSABLE' THEN 1 ELSE 2 END,
                NVL(s.bytes, 0) DESC
            FETCH FIRST {} ROWS ONLY",
            Self::segment_size_join("s", "INDEX", "i.owner", "i.index_name"),
            Self::user_schema_filter("i.owner"),
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.index_analysis,
            &index_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "index_analysis",
            Self::parse_index_details,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn collect_partition_analysis(
        detailed_metrics: &mut OracleTableDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let partition_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    p.table_owner,
                    p.table_name,
                    p.partition_name,
                    p.partition_position,
                    NVL(s.bytes, 0) as partition_size_bytes,
                    p.num_rows,
                    p.compression,
                    p.tablespace_name,
                    p.high_value,
                    TO_CHAR(p.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed,
                ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) as partition_size_mb
            FROM dba_tab_partitions p
            {}
            WHERE p.table_owner {}
               AND (p.last_analyzed IS NULL OR NVL(s.bytes, 0) > {})
            ORDER BY NVL(s.bytes, 0) DESC
            FETCH FIRST {} ROWS ONLY",
            Self::segment_size_join("s", "TABLE PARTITION", "p.table_owner", "p.table_name"),
            Self::user_schema_filter("p.table_owner"),
            Self::LARGE_TABLE_THRESHOLD,
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.partition_analysis,
            &partition_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "partition_analysis",
            Self::parse_partition_details,
        )
        .await?;

        Ok(())
    }
}
