use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleStorageDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleStorageInfo {
    pub total_tablespaces: u64,
    pub online_tablespaces: u64,
    pub offline_tablespaces: u64,
    pub readonly_tablespaces: u64,
    pub total_allocated_storage: u64,
    pub total_used_storage: u64,
    pub total_free_space: u64,
    pub storage_utilization_pct: f64,
    pub tablespaces_warning: u64,
    pub tablespaces_critical: u64,
    pub total_data_files: u64,
    pub autoextend_data_files: u64,
    pub total_temp_files: u64,
    pub largest_tablespace_size: u64,
    pub storage_added_24h: u64,
    pub autoextend_events_24h: u64,
    pub avg_extent_size: u64,
    pub total_extents: u64,
    pub total_undo_space: u64,
    pub used_undo_space: u64,
    pub undo_utilization_pct: f64,
    pub total_temp_space: u64,
    pub used_temp_space: u64,
    pub temp_utilization_pct: f64,
    pub files_near_maxsize: u64,
    pub reclaimable_space: u64,
    pub detailed_metrics: Option<OracleStorageDetailedMetrics>,
}
