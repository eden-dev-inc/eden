use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleTablespaceDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceInfo {
    pub total_tablespaces: u64,
    pub temp_tablespaces: u64,
    pub undo_tablespaces: u64,
    pub permanent_tablespaces: u64,
    pub total_allocated_bytes: u64,
    pub total_used_bytes: u64,
    pub total_free_bytes: u64,
    pub total_max_bytes: u64,
    pub autoextend_enabled: u64,
    pub high_usage_tablespaces: u64,
    pub critical_usage_tablespaces: u64,
    pub offline_tablespaces: u64,
    pub readonly_tablespaces: u64,
    pub largest_tablespace_bytes: u64,
    pub avg_usage_percent: f64,
    pub bigfile_tablespaces: u64,
    pub locally_managed: u64,
    pub dictionary_managed: u64,
    pub uniform_extents: u64,
    pub total_datafiles: u64,
    pub autoextend_datafiles: u64,
    pub high_usage_datafiles: u64,
    pub tablespace_health_score: f64,
    pub detailed_metrics: Option<OracleTablespaceDetailedMetrics>,
}
