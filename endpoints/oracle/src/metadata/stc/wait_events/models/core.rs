use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::OracleWaitEventDetailedMetrics;

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEventInfo {
    pub total_wait_events: u64,
    pub total_time_waited_us: u64,
    pub total_waits: u64,
    pub avg_wait_time_us: f64,
    pub max_wait_time_us: u64,
    pub cpu_time_percent: f64,
    pub wait_time_percent: f64,
    pub sessions_waiting: u64,
    pub top_wait_class: String,
    pub top_wait_class_percent: f64,
    pub io_waits: u64,
    pub io_wait_time_us: u64,
    pub concurrency_waits: u64,
    pub concurrency_wait_time_us: u64,
    pub application_waits: u64,
    pub application_wait_time_us: u64,
    pub configuration_waits: u64,
    pub configuration_wait_time_us: u64,
    pub administrative_waits: u64,
    pub administrative_wait_time_us: u64,
    pub network_waits: u64,
    pub network_wait_time_us: u64,
    pub db_time_us: u64,
    pub background_wait_events: u64,
    pub wait_health_score: f64,
    pub detailed_metrics: Option<OracleWaitEventDetailedMetrics>,
}
