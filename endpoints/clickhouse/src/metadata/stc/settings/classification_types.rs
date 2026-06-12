use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum SettingImpactLevel {
    /// Low impact on system behavior
    Low,
    /// Medium impact on system behavior
    Medium,
    /// High impact on system behavior
    High,
    /// Critical impact on system behavior
    Critical,
}

/// Memory impact level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum MemoryImpactLevel {
    /// Low memory impact
    Low,
    /// Medium memory impact
    Medium,
    /// High memory impact
    High,
    /// Critical memory impact
    Critical,
}

/// Performance impact level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PerformanceImpactLevel {
    /// Positive impact on performance
    Positive,
    /// Neutral impact on performance
    Neutral,
    /// Negative impact on performance
    Negative,
}

/// Security level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum SecurityLevel {
    /// Low security relevance
    Low,
    /// Medium security relevance
    Medium,
    /// High security relevance
    High,
    /// Critical security relevance
    Critical,
}

/// Danger level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DangerLevel {
    /// Low danger level
    Low,
    /// Medium danger level
    Medium,
    /// High danger level
    High,
    /// Critical danger level
    Critical,
}

/// Optimization priority classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum OptimizationPriority {
    /// Low priority optimization
    Low,
    /// Medium priority optimization
    Medium,
    /// High priority optimization
    High,
    /// Critical priority optimization
    Critical,
}

/// Resource type classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ResourceType {
    /// Memory resource
    Memory,
    /// CPU resource
    CPU,
    /// Network resource
    Network,
    /// Time resource
    Time,
    /// Disk resource
    Disk,
    /// Other resource type
    Other,
}

/// Limit impact level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum LimitImpactLevel {
    /// Low impact from this limit
    Low,
    /// Medium impact from this limit
    Medium,
    /// High impact from this limit
    High,
}

/// Configuration drift severity classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DriftSeverity {
    /// Minor drift with no operational impact
    Minor,
    /// Moderate drift that should be addressed
    Moderate,
    /// Major drift requiring immediate attention
    Major,
}

/// Configuration health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ConfigurationHealthStatus {
    /// Configuration is healthy and well-optimized
    Healthy,
    /// Minor configuration issues that should be monitored
    Attention,
    /// Configuration issues that require investigation
    Warning,
    /// Critical configuration issues requiring immediate attention
    Critical,
}

/// Configuration complexity classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ConfigurationComplexity {
    /// Minimal customization
    Minimal,
    /// Low complexity configuration
    Low,
    /// Medium complexity configuration
    Medium,
    /// High complexity configuration
    High,
    /// Very high complexity configuration
    VeryHigh,
}

/// Maintenance burden classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MaintenanceBurden {
    /// Minimal maintenance required
    Minimal,
    /// Low maintenance burden
    Low,
    /// Medium maintenance burden
    Medium,
    /// High maintenance burden
    High,
    /// Very high maintenance burden
    VeryHigh,
}
