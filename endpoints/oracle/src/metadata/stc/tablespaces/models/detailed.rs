use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceDetailedMetrics {
    pub problem_tablespaces: Vec<OracleTablespaceDetails>,
    pub datafile_analysis: Option<Vec<OracleDatafileDetails>>,
    pub usage_trends: Option<Vec<OracleTablespaceUsageTrend>>,
    pub autoextend_analysis: Option<Vec<OracleAutoextendAnalysis>>,
    pub fragmentation_analysis: Option<Vec<OracleFragmentationDetails>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceDetails {
    pub tablespace_name: String,
    pub contents: String,
    pub status: String,
    pub logging: String,
    pub force_logging: String,
    pub extent_management: String,
    pub allocation_type: String,
    pub bigfile: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub max_bytes: u64,
    pub usage_percent: f64,
    pub datafile_count: u64,
    pub autoextend_count: u64,
    pub total_gb: f64,
    pub used_gb: f64,
    pub free_gb: f64,
    pub issue_severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDatafileDetails {
    pub file_id: u64,
    pub file_name: String,
    pub tablespace_name: String,
    pub bytes: u64,
    pub max_bytes: u64,
    pub autoextensible: String,
    pub increment_by: u64,
    pub status: String,
    pub online_status: String,
    pub size_gb: f64,
    pub max_gb: f64,
    pub usage_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceUsageTrend {
    pub tablespace_name: String,
    pub current_usage_percent: f64,
    pub usage_24h_ago: Option<f64>,
    pub usage_7d_ago: Option<f64>,
    pub daily_growth_rate: f64,
    pub days_until_full: Option<i64>,
    pub growth_category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleAutoextendAnalysis {
    pub tablespace_name: String,
    pub usage_percent: f64,
    pub datafile_count: u64,
    pub autoextend_enabled: u64,
    pub remaining_space_bytes: u64,
    pub max_expansion_bytes: u64,
    pub recommendation: String,
    pub risk_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleFragmentationDetails {
    pub tablespace_name: String,
    pub total_free_bytes: u64,
    pub largest_free_extent: u64,
    pub free_extent_count: u64,
    pub avg_free_extent_size: u64,
    pub fragmentation_index: f64,
    pub free_space_fragmentation: f64,
    pub defrag_recommendation: String,
}
