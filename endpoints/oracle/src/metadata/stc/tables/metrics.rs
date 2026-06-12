use super::*;
impl OracleTableInfo {
    /// Checks if table statistics are generally stale
    pub fn has_stale_statistics(&self, threshold_pct: f64) -> bool {
        if self.tables_with_stats == 0 {
            return false;
        }
        let stale_pct = ratio_percentage(self.tables_stale_stats, self.tables_with_stats);
        stale_pct > threshold_pct
    }

    /// Checks if there are tables without statistics
    pub fn has_missing_statistics(&self) -> bool {
        self.tables_no_stats > 0
    }

    /// Checks if there are unusable indexes
    pub fn has_unusable_indexes(&self) -> bool {
        self.unusable_indexes > 0
    }

    /// Checks if there are high-growth tables
    pub fn has_high_growth_tables(&self) -> bool {
        self.high_growth_tables > 0
    }

    /// Checks if there are large tables requiring attention
    pub fn has_large_tables(&self, threshold_count: u64) -> bool {
        self.large_tables > threshold_count
    }

    /// Gets total table size in GB
    pub fn total_table_size_gb(&self) -> f64 {
        bytes_to_gb(self.total_table_size_bytes)
    }

    /// Gets total index size in GB
    pub fn total_index_size_gb(&self) -> f64 {
        bytes_to_gb(self.total_index_size_bytes)
    }

    /// Gets total LOB size in GB
    pub fn total_lob_size_gb(&self) -> f64 {
        bytes_to_gb(self.total_lob_size_bytes)
    }

    /// Gets largest table size in GB
    pub fn largest_table_size_gb(&self) -> f64 {
        bytes_to_gb(self.largest_table_size_bytes)
    }

    /// Gets average table size in MB
    pub fn avg_table_size_mb(&self) -> f64 {
        bytes_to_mb(self.avg_table_size_bytes)
    }

    /// Gets compression ratio
    pub fn compression_ratio(&self) -> f64 {
        ratio_percentage(self.compressed_tables, self.total_tables)
    }

    /// Gets partitioning ratio
    pub fn partitioning_ratio(&self) -> f64 {
        ratio_percentage(self.partitioned_tables, self.total_tables)
    }

    /// Gets statistics coverage
    pub fn statistics_coverage(&self) -> f64 {
        ratio_percentage(self.tables_with_stats, self.total_tables)
    }

    /// Gets index to table size ratio
    pub fn index_to_table_ratio(&self) -> f64 {
        ratio_percentage(self.total_index_size_bytes, self.total_table_size_bytes)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleTableHealthSummary {
        OracleTableHealthSummary {
            statistics_health: status_by_flags(
                self.tables_no_stats > 0 || self.has_stale_statistics(25.0),
                self.tables_no_stats > (self.total_tables / 4) || self.has_stale_statistics(50.0),
            ),
            index_health: status_by_count(self.unusable_indexes, 0, 5),
            growth_health: status_by_count(self.high_growth_tables, 5, 10),
            size_health: status_by_count(self.large_tables, 20, 50),
            maintenance_health: status_by_flags(self.tables_analyzed_24h < (self.total_tables / 10), false),
        }
    }
}
