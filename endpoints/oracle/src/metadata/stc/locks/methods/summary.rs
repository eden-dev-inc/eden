use super::*;

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleLockSummary {
    pub total_locks: u64,
    pub blocking_percentage: f64,
    pub avg_wait_time: f64,
    pub severity: ContentionSeverity,
    pub blocking_chains_count: u64,
    pub contended_objects_count: u64,
    pub performance_impact: f64,
    pub needs_attention: bool,
}

#[allow(dead_code)]
impl OracleLockSummary {
    pub fn from_lock_info(lock_info: &OracleLockInfo) -> Self {
        OracleLockSummary {
            total_locks: lock_info.total_active_locks,
            blocking_percentage: ratio_percentage(lock_info.blocking_locks, lock_info.total_active_locks),
            avg_wait_time: lock_info.avg_lock_wait_time,
            severity: lock_info.contention_severity.clone(),
            blocking_chains_count: lock_info.blocking_chains.len() as u64,
            contended_objects_count: lock_info.contended_objects.len() as u64,
            performance_impact: lock_info.performance_impact_score,
            needs_attention: lock_info.requires_immediate_attention(),
        }
    }
}
