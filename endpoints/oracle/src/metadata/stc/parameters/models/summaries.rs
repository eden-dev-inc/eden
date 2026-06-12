use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MemorySummary {
    pub memory_target: Option<u64>,
    pub sga_target: Option<u64>,
    pub pga_target: Option<u64>,
    pub total_allocated: Option<u64>,
    pub amm_enabled: bool,
    pub asmm_enabled: bool,
}

/// Security configuration summary
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SecuritySummary {
    pub audit_enabled: bool,
    pub remote_authentication: bool,
    pub high_risk_parameters: usize,
    pub security_score: f64,
}
