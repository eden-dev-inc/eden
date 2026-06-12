use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEventDetailedMetrics {
    pub top_wait_events: Vec<OracleWaitEventDetails>,
    pub wait_class_analysis: Option<Vec<OracleWaitClassDetails>>,
    pub session_wait_analysis: Option<Vec<OracleSessionWaitDetails>>,
    pub wait_trends: Option<Vec<OracleWaitTrendDetails>>,
    pub io_wait_breakdown: Option<Vec<OracleIOWaitDetails>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitEventDetails {
    pub event_name: String,
    pub wait_class: String,
    pub total_waits: u64,
    pub time_waited_us: u64,
    pub avg_wait_us: f64,
    pub max_wait_us: u64,
    pub time_waited_percent: f64,
    pub waits_per_sec: f64,
    pub avg_wait_ms: f64,
    pub sessions_waiting: u64,
    pub rank_by_time: u64,
    pub rank_by_waits: u64,
    pub issue_severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitClassDetails {
    pub wait_class: String,
    pub total_waits: u64,
    pub time_waited_us: u64,
    pub avg_wait_us: f64,
    pub time_waited_percent: f64,
    pub event_count: u64,
    pub sessions_waiting: u64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSessionWaitDetails {
    pub sid: u64,
    pub serial: u64,
    pub username: String,
    pub program: String,
    pub machine: String,
    pub wait_event: String,
    pub wait_class: String,
    pub wait_time_seconds: u64,
    pub seconds_in_wait: u64,
    pub state: String,
    pub p1: u64,
    pub p2: u64,
    pub p3: u64,
    pub sql_id: Option<String>,
    pub blocking_session: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleWaitTrendDetails {
    pub snapshot_time: String,
    pub event_name: String,
    pub waits: u64,
    pub time_waited_us: u64,
    pub avg_wait_us: f64,
    pub waits_per_sec: f64,
    pub trend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleIOWaitDetails {
    pub io_type: String,
    pub event_name: String,
    pub total_waits: u64,
    pub time_waited_us: u64,
    pub avg_wait_us: f64,
    pub avg_io_size_bytes: u64,
    pub io_requests_per_sec: f64,
    pub throughput_mb_per_sec: f64,
    pub io_time_percent: f64,
}
