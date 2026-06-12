use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type SessionHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionHealthSummary {
    pub utilization_health: SessionHealthStatus,
    pub performance_health: SessionHealthStatus,
    pub security_health: SessionHealthStatus,
    pub connection_health: SessionHealthStatus,
    pub resource_health: SessionHealthStatus,
}
