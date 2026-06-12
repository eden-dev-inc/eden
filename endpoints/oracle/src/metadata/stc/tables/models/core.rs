use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleTableDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableInfo {
    pub total_tables: u64,
    pub partitioned_tables: u64,
    pub tables_with_stats: u64,
    pub tables_stale_stats: u64,
    pub tables_no_stats: u64,
    pub total_table_rows: u64,
    pub total_table_size_bytes: u64,
    pub total_index_size_bytes: u64,
    pub total_indexes: u64,
    pub unusable_indexes: u64,
    pub invisible_indexes: u64,
    pub compressed_tables: u64,
    pub tables_with_lobs: u64,
    pub total_lob_size_bytes: u64,
    pub empty_tables: u64,
    pub large_tables: u64,
    pub high_growth_tables: u64,
    pub largest_table_size_bytes: u64,
    pub avg_rows_per_table: u64,
    pub avg_table_size_bytes: u64,
    pub tables_analyzed_24h: u64,
    pub high_activity_tables: u64,
    pub total_partitions: u64,
    pub total_subpartitions: u64,
    pub tables_with_fks: u64,
    pub tables_with_checks: u64,
    pub table_health_score: f64,
    pub detailed_metrics: Option<OracleTableDetailedMetrics>,
}
