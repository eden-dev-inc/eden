use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableDetailedMetrics {
    pub problem_tables: Vec<OracleTableDetails>,
    pub index_analysis: Option<Vec<OracleIndexDetails>>,
    pub partition_analysis: Option<Vec<OraclePartitionDetails>>,
    pub statistics_analysis: Option<Vec<OracleTableStatistics>>,
    pub lob_analysis: Option<Vec<OracleLobDetails>>,
    pub constraint_analysis: Option<Vec<OracleConstraintDetails>>,
    pub growth_analysis: Option<Vec<OracleTableGrowth>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableDetails {
    pub owner: String,
    pub table_name: String,
    pub num_rows: u64,
    pub table_size_bytes: u64,
    pub avg_row_len: u64,
    pub blocks: u64,
    pub empty_blocks: u64,
    pub last_analyzed: Option<String>,
    pub compression: String,
    pub partitioned: String,
    pub degree: String,
    pub tablespace_name: String,
    pub pct_free: u64,
    pub pct_used: u64,
    pub sample_size: u64,
    pub table_size_mb: f64,
    pub rows_per_block: f64,
    pub space_utilization_pct: f64,
    pub issue_severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleIndexDetails {
    pub owner: String,
    pub index_name: String,
    pub table_name: String,
    pub index_type: String,
    pub uniqueness: String,
    pub status: String,
    pub visibility: String,
    pub degree: String,
    pub compression: String,
    pub distinct_keys: u64,
    pub leaf_blocks: u64,
    pub clustering_factor: u64,
    pub index_size_bytes: u64,
    pub index_size_mb: f64,
    pub selectivity: f64,
    pub last_analyzed: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OraclePartitionDetails {
    pub table_owner: String,
    pub table_name: String,
    pub partition_name: String,
    pub partition_position: u64,
    pub partition_size_bytes: u64,
    pub num_rows: u64,
    pub compression: String,
    pub tablespace_name: String,
    pub high_value: String,
    pub last_analyzed: Option<String>,
    pub partition_size_mb: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableStatistics {
    pub owner: String,
    pub table_name: String,
    pub num_rows: u64,
    pub blocks: u64,
    pub avg_row_len: u64,
    pub sample_size: u64,
    pub last_analyzed: Option<String>,
    pub staleness_days: i64,
    pub quality_score: f64,
    pub stats_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLobDetails {
    pub owner: String,
    pub table_name: String,
    pub column_name: String,
    pub segment_name: String,
    pub lob_size_bytes: u64,
    pub in_row: String,
    pub chunk: u64,
    pub compression: String,
    pub deduplication: String,
    pub tablespace_name: String,
    pub lob_size_mb: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleConstraintDetails {
    pub owner: String,
    pub constraint_name: String,
    pub constraint_type: String,
    pub table_name: String,
    pub status: String,
    pub validated: String,
    pub deferrable: String,
    pub deferred: String,
    pub rely: String,
    pub bad: String,
    pub delete_rule: Option<String>,
    pub r_table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableGrowth {
    pub table_owner: String,
    pub table_name: String,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub total_dml: u64,
    pub table_size_bytes: u64,
    pub growth_rate_daily: f64,
    pub projected_size_30d: u64,
    pub growth_category: String,
}
