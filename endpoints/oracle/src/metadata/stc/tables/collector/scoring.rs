use super::*;
impl OracleTableInfo {
    pub(crate) fn calculate_health_score(table_info: &OracleTableInfo) -> f64 {
        let mut score = 100.0;

        // Statistics health (30% weight)
        if table_info.total_tables > 0 {
            let no_stats_penalty = (table_info.tables_no_stats as f64 / table_info.total_tables as f64) * 30.0;
            let stale_stats_penalty = (table_info.tables_stale_stats as f64 / table_info.total_tables as f64) * 15.0;
            score -= no_stats_penalty + stale_stats_penalty;
        }

        // Index health (20% weight)
        if table_info.total_indexes > 0 {
            let unusable_penalty = (table_info.unusable_indexes as f64 / table_info.total_indexes as f64) * 20.0;
            score -= unusable_penalty;
        }

        // Growth health (20% weight)
        if table_info.total_tables > 0 {
            let growth_penalty = (table_info.high_growth_tables as f64 / table_info.total_tables as f64) * 20.0;
            score -= growth_penalty;
        }

        // Size management (15% weight)
        if table_info.total_tables > 0 {
            let large_table_penalty = if table_info.large_tables > 20 {
                15.0
            } else if table_info.large_tables > 10 {
                10.0
            } else if table_info.large_tables > 5 {
                5.0
            } else {
                0.0
            };
            score -= large_table_penalty;
        }

        // Maintenance health (15% weight)
        if table_info.total_tables > 0 {
            let maintenance_penalty = if table_info.tables_analyzed_24h < (table_info.total_tables / 10) {
                15.0
            } else if table_info.tables_analyzed_24h < (table_info.total_tables / 5) {
                10.0
            } else {
                0.0
            };
            score -= maintenance_penalty;
        }

        score.clamp(0.0, 100.0)
    }
}
