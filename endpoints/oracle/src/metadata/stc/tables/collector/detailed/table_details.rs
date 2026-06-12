use super::*;

impl OracleTableInfo {
    pub(crate) async fn collect_problem_tables(detailed_metrics: &mut OracleTableDetailedMetrics, context: OracleAsync) -> ResultEP<()> {
        let problem_tables_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    t.owner,
                    t.table_name,
                    t.num_rows,
                    NVL(s.bytes, 0) as table_size_bytes,
                    t.avg_row_len,
                    t.blocks,
                    t.empty_blocks,
                    TO_CHAR(t.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed,
                    t.compression,
                    t.partitioned,
                    t.degree,
                    t.tablespace_name,
                    t.pct_free,
                    t.pct_used,
                    t.sample_size,
                    ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) as table_size_mb,
                    CASE
                        WHEN t.blocks > 0 THEN ROUND(t.num_rows / t.blocks, 2)
                        ELSE 0
                    END as rows_per_block,
                    CASE
                        WHEN t.blocks > 0 THEN ROUND(((t.blocks - NVL(t.empty_blocks, 0)) / t.blocks) * 100, 2)
                        ELSE 0
                    END as space_utilization_pct,
                    CASE
                        WHEN t.last_analyzed IS NULL THEN 'CRITICAL'
                        WHEN t.last_analyzed < SYSDATE - 30 THEN 'CRITICAL'
                        WHEN t.last_analyzed < SYSDATE - 7 THEN 'WARNING'
                        WHEN NVL(s.bytes, 0) > {} THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as issue_severity
                FROM dba_tables t
                {}
                WHERE t.owner {}
                   AND (t.last_analyzed IS NULL
                        OR t.last_analyzed < SYSDATE - 7
                        OR NVL(s.bytes, 0) > {})
                ORDER BY
                    CASE issue_severity
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'WARNING' THEN 2
                        ELSE 3
                    END,
                    NVL(s.bytes, 0) DESC
                FETCH FIRST {} ROWS ONLY",
            Self::segment_size_join("s", "TABLE", "t.owner", "t.table_name"),
            Self::user_schema_filter("t.owner"),
            Self::LARGE_TABLE_THRESHOLD,
            Self::LARGE_TABLE_THRESHOLD,
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.problem_tables,
            &problem_tables_query,
            context,
            Self::QUERY_TIMEOUT,
            "problem_tables",
            Self::parse_table_details,
        )
        .await?;

        Ok(())
    }
}
