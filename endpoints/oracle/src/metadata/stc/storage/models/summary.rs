use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type StorageHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleStorageHealthSummary {
    pub space_health: StorageHealthStatus,
    pub growth_health: StorageHealthStatus,
    pub file_health: StorageHealthStatus,
    pub undo_health: StorageHealthStatus,
    pub temp_health: StorageHealthStatus,
}
