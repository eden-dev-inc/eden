use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub enum ParameterCategory {
    Memory,
    Performance,
    Security,
    Storage,
    Network,
    Backup,
    Recovery,
    Optimizer,
    Processes,
    Sessions,
    Undo,
    Logging,
    Auditing,
    Encryption,
    Partitioning,
    Parallel,
    Clustering,
    Replication,
    Compatibility,
    Advanced,
    #[default]
    Other,
}

/// Performance impact levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub enum PerformanceImpact {
    #[default]
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// Security impact levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub enum SecurityImpact {
    #[default]
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// Risk levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub enum RiskLevel {
    #[default]
    Low,
    Medium,
    High,
    Critical,
}

/// Warning severity levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum WarningSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Recommendation categories
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RecommendationCategory {
    Performance,
    Security,
    Memory,
    Storage,
    Backup,
    Monitoring,
    Maintenance,
    Compliance,
}

/// Recommendation priorities
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RecommendationPriority {
    Low,
    Medium,
    High,
    Critical,
}

/// Implementation difficulty levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ImplementationDifficulty {
    Easy,
    Medium,
    Hard,
    Complex,
}

/// Parameter importance levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ParameterImportance {
    Low,
    Medium,
    High,
    Critical,
}

/// Parameter status for dashboard display
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ParameterStatus {
    Optimal,
    CanImprove,
    SubOptimal,
    Deprecated,
    Critical,
}
