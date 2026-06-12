use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleSessionDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionInfo {
    pub total_user_sessions: u64,
    pub active_user_sessions: u64,
    pub inactive_user_sessions: u64,
    pub killed_sessions: u64,
    pub cached_sessions: u64,
    pub background_processes: u64,
    pub max_sessions: u64,
    pub session_utilization_pct: f64,
    pub unique_users: u64,
    pub unique_programs: u64,
    pub unique_machines: u64,
    pub avg_session_duration: f64,
    pub longest_session_duration: f64,
    pub new_sessions_last_hour: u64,
    pub disconnected_sessions_last_hour: u64,
    pub total_logons_since_startup: u64,
    pub failed_logins_last_hour: u64,
    pub sessions_waiting_for_locks: u64,
    pub sessions_using_temp: u64,
    pub total_temp_space_used: u64,
    pub high_pga_sessions: u64,
    pub total_pga_used: u64,
    pub dedicated_connections: u64,
    pub shared_connections: u64,
    pub detailed_metrics: Option<OracleSessionDetailedMetrics>,
}
