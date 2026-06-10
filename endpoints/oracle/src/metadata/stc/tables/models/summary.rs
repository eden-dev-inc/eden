use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type TableHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTableHealthSummary {
    pub statistics_health: TableHealthStatus,
    pub index_health: TableHealthStatus,
    pub growth_health: TableHealthStatus,
    pub size_health: TableHealthStatus,
    pub maintenance_health: TableHealthStatus,
}
