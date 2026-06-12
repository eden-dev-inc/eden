use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum WaitEventSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum WaitEventCategory {
    Administrative,
    Application,
    Cluster,
    Commit,
    Concurrency,
    Configuration,
    Idle,
    Network,
    Other,
    Scheduler,
    SystemIo,
    UserIo,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum SqlPerformanceRating {
    Excellent,
    Good,
    Fair,
    Poor,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum FileType {
    Data,
    Temp,
    Undo,
    Redo,
    Archive,
    Control,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum BottleneckType {
    Cpu,
    Memory,
    Io,
    Network,
    Locks,
    Application,
    Configuration,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum BottleneckSeverity {
    Minor,
    Moderate,
    Major,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum TrendDirection {
    Improving,
    Stable,
    Degrading,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum TrendSeverity {
    Normal,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum WaitEventsTrend {
    Improving,
    #[default]
    Stable,
    Degrading,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RecommendationCategory {
    Performance,
    Memory,
    Io,
    Sql,
    Configuration,
    Maintenance,
    Monitoring,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RecommendationPriority {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ImplementationEffort {
    Low,
    Medium,
    High,
    VeryHigh,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum AlertType {
    Performance,
    Resource,
    Availability,
    Error,
    Threshold,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}
