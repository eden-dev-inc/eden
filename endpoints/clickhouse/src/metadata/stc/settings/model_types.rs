use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseInconsistentSetting {
    /// Setting name
    pub name: String,
    /// Different values found across nodes
    pub values: String, // JSON array of values
    /// Hostnames where different values are found
    pub hosts: String, // JSON array of hosts
    /// Impact level of this inconsistency
    pub impact_level: SettingImpactLevel,
}

/// Deprecated setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDeprecatedSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Default value
    pub default_value: String,
    /// Setting description
    pub description: Option<String>,
    /// Recommended replacement setting
    pub replacement_setting: Option<String>,
    /// Reason for deprecation
    pub deprecation_reason: String,
    /// Whether the setting is readonly
    pub is_readonly: bool,
}

/// Dangerous setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDangerousSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Danger level
    pub danger_level: DangerLevel,
    /// Description of the risk
    pub risk_description: String,
    /// Steps to mitigate the risk
    pub mitigation_steps: Vec<String>,
    /// Potential impact if not addressed
    pub potential_impact: String,
}

/// Memory-related setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMemorySetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Default value
    pub default_value: String,
    /// Setting description
    pub description: Option<String>,
    /// Memory impact level
    pub memory_impact: MemoryImpactLevel,
    /// Recommended value
    pub recommended_value: Option<String>,
    /// Whether the setting is readonly
    pub is_readonly: bool,
}

/// Performance-related setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePerformanceSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Default value
    pub default_value: String,
    /// Setting description
    pub description: Option<String>,
    /// Performance impact level
    pub performance_impact: PerformanceImpactLevel,
    /// Recommended value
    pub recommended_value: Option<String>,
    /// Whether the setting is readonly
    pub is_readonly: bool,
}

/// Security-related setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSecuritySetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Default value
    pub default_value: String,
    /// Setting description
    pub description: Option<String>,
    /// Security level
    pub security_level: SecurityLevel,
    /// Recommended value
    pub recommended_value: Option<String>,
    /// Whether the setting is readonly
    pub is_readonly: bool,
}

/// Recent setting change information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseRecentSettingChange {
    /// Setting name
    pub name: String,
    /// Previous value
    pub previous_value: String,
    /// Current value
    pub current_value: String,
    /// When the change occurred
    pub change_time: DateTimeWrapper,
    /// User who made the change
    pub changed_by: Option<String>,
    /// Change reason or context
    pub change_reason: Option<String>,
}

/// Setting optimization recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSettingOptimization {
    /// Setting name
    pub setting_name: String,
    /// Current value
    pub current_value: String,
    /// Recommended value
    pub recommended_value: String,
    /// Reason for optimization
    pub optimization_reason: String,
    /// Expected benefit from the change
    pub expected_benefit: String,
    /// Priority of this optimization
    pub priority: OptimizationPriority,
}

/// Resource limit setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseResourceLimitSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub current_value: String,
    /// Default value
    pub default_value: String,
    /// Setting description
    pub description: Option<String>,
    /// Type of resource this setting limits
    pub resource_type: ResourceType,
    /// Impact level of this limit
    pub limit_impact: LimitImpactLevel,
    /// Recommended value
    pub recommended_value: Option<String>,
    /// Whether the setting is readonly
    pub is_readonly: bool,
}

/// Cluster configuration drift information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseClusterConfigDrift {
    /// Setting name with drift
    pub setting_name: String,
    /// Map of hostname to value
    pub node_values: HashMap<String, String>,
    /// Severity of the drift
    pub drift_severity: DriftSeverity,
    /// Impact on cluster operations
    pub cluster_impact: String,
}
