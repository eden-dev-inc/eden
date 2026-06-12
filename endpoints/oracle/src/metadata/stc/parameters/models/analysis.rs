use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ParameterAnalysisSummary {
    /// Total number of parameters
    pub total_parameters: u64,
    /// Number of modified parameters
    pub modified_parameters: u64,
    /// Number of deprecated parameters in use
    pub deprecated_parameters: u64,
    /// Number of parameters with warnings
    pub warning_parameters: u64,
    /// Number of parameters needing optimization
    pub optimization_candidates: u64,
    /// Number of high-risk parameters
    pub high_risk_parameters: u64,
    /// Number of security-related parameters
    pub security_parameters: u64,
    /// Number of performance-critical parameters
    pub performance_parameters: u64,
    /// Overall configuration health score (0-100)
    pub health_score: f64,
    /// Memory allocation efficiency score
    pub memory_efficiency_score: f64,
    /// Security configuration score
    pub security_score: f64,
    /// Performance configuration score
    pub performance_score: f64,
}

/// Configuration warning
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ConfigurationWarning {
    /// Parameter name
    pub parameter_name: String,
    /// Warning severity
    pub severity: WarningSeverity,
    /// Warning message
    pub message: String,
    /// Recommended action
    pub recommended_action: String,
    /// Impact description
    pub impact: String,
}

/// Global configuration recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct GlobalRecommendation {
    /// Recommendation category
    pub category: RecommendationCategory,
    /// Priority level
    pub priority: RecommendationPriority,
    /// Recommendation title
    pub title: String,
    /// Detailed description
    pub description: String,
    /// Parameters involved
    pub affected_parameters: Vec<String>,
    /// Expected benefit
    pub expected_benefit: String,
    /// Implementation difficulty
    pub implementation_difficulty: ImplementationDifficulty,
    /// Whether restart is required
    pub requires_restart: bool,
}

/// Parameter recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ParameterRecommendation {
    /// Recommended value
    pub recommended_value: String,
    /// Reason for recommendation
    pub reason: String,
    /// Expected improvement
    pub expected_improvement: String,
    /// Risk assessment
    pub risk_assessment: String,
    /// Implementation notes
    pub implementation_notes: Option<String>,
}
