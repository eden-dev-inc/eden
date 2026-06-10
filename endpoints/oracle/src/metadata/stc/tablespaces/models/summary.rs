use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type TablespaceHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceHealthSummary {
    pub usage_health: TablespaceHealthStatus,
    pub availability_health: TablespaceHealthStatus,
    pub autoextend_health: TablespaceHealthStatus,
    pub management_health: TablespaceHealthStatus,
}
