use super::*;

impl OracleTableInfo {
    pub(crate) async fn collect_growth_analysis(detailed_metrics: &mut OracleTableDetailedMetrics, context: OracleAsync) -> ResultEP<()> {
        let growth_analysis_query = crate::metadata::stc::utils::query(format!(
            "SELECT
                    m.table_owner,
                    m.table_name,
                    m.inserts,
                    m.updates,
                    m.deletes,
                    (m.inserts + m.updates + m.deletes) as total_dml,
                    NVL(s.bytes, 0) as table_size_bytes,
                    CASE
                        WHEN t.num_rows > 0 THEN ROUND((m.inserts + m.updates + m.deletes) / t.num_rows, 4)
                        ELSE 0
                    END as growth_rate_daily,
                    CASE
                        WHEN t.num_rows > 0 THEN
                            NVL(s.bytes, 0) * (1 + ((m.inserts + m.updates + m.deletes) / t.num_rows) * 30)
                        ELSE NVL(s.bytes, 0)
                    END as projected_size_30d,
                    CASE
                        WHEN t.num_rows > 0 AND ((m.inserts + m.updates + m.deletes) / t.num_rows) > 0.2 THEN 'CRITICAL'
                        WHEN t.num_rows > 0 AND ((m.inserts + m.updates + m.deletes) / t.num_rows) > 0.1 THEN 'HIGH'
                        WHEN t.num_rows > 0 AND ((m.inserts + m.updates + m.deletes) / t.num_rows) > 0.05 THEN 'MEDIUM'
                        ELSE 'LOW'
                END as growth_category
            FROM dba_tab_modifications m
            JOIN dba_tables t ON m.table_owner = t.owner AND m.table_name = t.table_name
            {}
            WHERE m.table_owner {}
               AND (m.inserts + m.updates + m.deletes) > 0
               AND t.num_rows > 0
            ORDER BY
                CASE growth_category
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'HIGH' THEN 2
                        WHEN 'MEDIUM' THEN 3
                        ELSE 4
                    END,
                    (m.inserts + m.updates + m.deletes) DESC
            FETCH FIRST {} ROWS ONLY",
            Self::segment_size_join("s", "TABLE", "m.table_owner", "m.table_name"),
            Self::user_schema_filter("m.table_owner"),
            Self::MAX_DETAILED_RESULTS
        ));

        crate::metadata::stc::utils::assign_optional(
            &mut detailed_metrics.growth_analysis,
            &growth_analysis_query,
            context,
            Self::QUERY_TIMEOUT,
            "growth_analysis",
            Self::parse_growth_details,
        )
        .await?;

        Ok(())
    }
}
