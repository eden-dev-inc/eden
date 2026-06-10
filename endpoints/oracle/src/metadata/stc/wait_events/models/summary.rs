use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::metadata::stc::common::HealthStatus;

pub type WaitEventHealthStatus = HealthStatus;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEventHealthSummary {
    pub wait_time_health: WaitEventHealthStatus,
    pub session_health: WaitEventHealthStatus,
    pub io_health: WaitEventHealthStatus,
    pub concurrency_health: WaitEventHealthStatus,
}
