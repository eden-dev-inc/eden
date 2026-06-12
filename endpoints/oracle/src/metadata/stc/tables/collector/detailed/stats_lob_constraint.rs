use super::*;

impl OracleTableInfo {
    pub(crate) async fn collect_statistics_analysis(
        detailed_metrics: &mut OracleTableDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let statistics_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    owner,
                    table_name,
                    num_rows,
                    blocks,
                    avg_row_len,
                    sample_size,
                    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed,
                    CASE
                        WHEN last_analyzed IS NULL THEN -1
                        ELSE TRUNC(SYSDATE - last_analyzed)
                    END as staleness_days,
                    CASE
                        WHEN last_analyzed IS NULL THEN 0
                        WHEN sample_size > 0 AND num_rows > 0 THEN
                            ROUND((sample_size / num_rows) * 100, 2)
                        ELSE 0
                    END as quality_score,
                    CASE
                        WHEN last_analyzed IS NULL THEN 'MISSING'
                        WHEN last_analyzed < SYSDATE - 30 THEN 'VERY_STALE'
                        WHEN last_analyzed < SYSDATE - 7 THEN 'STALE'
                        ELSE 'CURRENT'
                    END as stats_status
                FROM dba_tables
                WHERE owner {}
                   AND (last_analyzed IS NULL OR last_analyzed < SYSDATE - 7)
                ORDER BY
                    CASE
                        WHEN last_analyzed IS NULL THEN 1
                        WHEN last_analyzed < SYSDATE - 30 THEN 2
                        ELSE 3
                    END,
                    num_rows DESC
            FETCH FIRST {} ROWS ONLY",
            Self::user_schema_filter("owner"),
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.statistics_analysis,
            &statistics_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "statistics_analysis",
            Self::parse_statistics_details,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn collect_lob_analysis(detailed_metrics: &mut OracleTableDetailedMetrics, context: OracleAsync) -> ResultEP<()> {
        let lob_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    l.owner,
                    l.table_name,
                    l.column_name,
                    l.segment_name,
                    NVL(s.bytes, 0) as lob_size_bytes,
                    l.in_row,
                l.chunk,
                l.compression,
                l.deduplication,
                l.tablespace_name,
                ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) as lob_size_mb
            FROM dba_lobs l
            {}
            WHERE l.owner {}
               AND NVL(s.bytes, 0) > 0
            ORDER BY NVL(s.bytes, 0) DESC
            FETCH FIRST {} ROWS ONLY",
            Self::segment_size_join("s", "LOBSEGMENT", "l.owner", "l.segment_name"),
            Self::user_schema_filter("l.owner"),
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.lob_analysis,
            &lob_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "lob_analysis",
            Self::parse_lob_details,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn collect_constraint_analysis(
        detailed_metrics: &mut OracleTableDetailedMetrics,
        context: OracleAsync,
    ) -> ResultEP<()> {
        let constraint_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    c.owner,
                    c.constraint_name,
                    c.constraint_type,
                    c.table_name,
                    c.status,
                    c.validated,
                    c.deferrable,
                    c.deferred,
                    c.rely,
                    c.bad,
                    c.delete_rule,
                    c.r_table_name
                FROM dba_constraints c
                WHERE c.owner {}
                   AND c.constraint_type IN ('R', 'C', 'P', 'U')
                   AND (c.status = 'DISABLED' OR c.validated = 'NOT VALIDATED' OR c.bad = 'BAD')
                ORDER BY
                    CASE c.constraint_type
                        WHEN 'P' THEN 1
                        WHEN 'U' THEN 2
                        WHEN 'R' THEN 3
                        ELSE 4
                    END,
                    c.table_name
            FETCH FIRST {} ROWS ONLY",
            Self::user_schema_filter("c.owner"),
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.constraint_analysis,
            &constraint_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "constraint_analysis",
            Self::parse_constraint_details,
        )
        .await?;

        Ok(())
    }
}
