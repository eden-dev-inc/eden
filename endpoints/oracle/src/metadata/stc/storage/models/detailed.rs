use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleStorageDetailedMetrics {
    pub problem_tablespaces: Vec<OracleTablespaceDetails>,
    pub problem_datafiles: Option<Vec<OracleDataFileDetails>>,
    pub growth_analysis: Option<Vec<OracleStorageGrowth>>,
    pub fragmentation_analysis: Option<Vec<OracleFragmentationDetails>>,
    pub special_tablespaces: Option<Vec<OracleSpecialTablespace>>,
    pub file_limit_issues: Option<Vec<OracleFileLimitIssue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceDetails {
    pub tablespace_name: String,
    pub status: String,
    pub contents: String,
    pub extent_management: String,
    pub allocation_type: String,
    pub total_size: u64,
    pub used_size: u64,
    pub free_size: u64,
    pub usage_pct: f64,
    pub largest_free_extent: u64,
    pub file_count: u64,
    pub autoextend_count: u64,
    pub alert_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleDataFileDetails {
    pub file_name: String,
    pub file_id: i32,
    pub tablespace_name: String,
    pub bytes: u64,
    pub maxbytes: u64,
    pub increment_by: u64,
    pub autoextensible: String,
    pub status: String,
    pub size_mb: f64,
    pub pct_of_maxsize: f64,
    pub size_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleStorageGrowth {
    pub tablespace_name: String,
    pub potential_growth_bytes: u64,
    pub autoextend_files: u64,
    pub avg_increment_size: u64,
    pub max_increment_size: u64,
    pub potential_growth_mb: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleFragmentationDetails {
    pub tablespace_name: String,
    pub extent_count: u64,
    pub avg_extent_size: u64,
    pub min_extent_size: u64,
    pub max_extent_size: u64,
    pub small_extents: u64,
    pub small_extent_bytes: u64,
    pub avg_extent_kb: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSpecialTablespace {
    pub tablespace_name: String,
    pub contents: String,
    pub status: String,
    pub tablespace_type: String,
    pub total_size: u64,
    pub used_size: u64,
    pub usage_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleFileLimitIssue {
    pub file_name: String,
    pub tablespace_name: String,
    pub bytes: u64,
    pub maxbytes: u64,
    pub increment_by: u64,
    pub current_size_mb: f64,
    pub max_size_mb: f64,
    pub pct_of_max: f64,
    pub remaining_mb: f64,
    pub risk_level: String,
}
