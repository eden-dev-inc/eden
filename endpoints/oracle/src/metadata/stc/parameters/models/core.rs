use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
/// Oracle database parameter information and analysis.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleParameterInfo {
    /// Parameter name
    pub name: String,
    /// Current parameter value
    pub value: Option<String>,
    /// Default parameter value
    pub default_value: Option<String>,
    /// Whether parameter can be modified
    pub is_modifiable: bool,
    /// Scope of modification (MEMORY, SPFILE, BOTH)
    pub modify_scope: String,
    /// Whether parameter is system modifiable
    pub is_system_modifiable: bool,
    /// Whether parameter is session modifiable
    pub is_session_modifiable: bool,
    /// Whether parameter requires restart to take effect
    pub is_instance_modifiable: bool,
    /// Parameter type (STRING, INTEGER, BOOLEAN, BIG_INTEGER)
    pub parameter_type: String,
    /// Parameter description
    pub description: Option<String>,
    /// Whether parameter value has been explicitly set
    pub is_modified: bool,
    /// Whether parameter is deprecated
    pub is_deprecated: bool,
    /// Whether parameter is a basic parameter
    pub is_basic: bool,
    /// Parameter category
    pub category: ParameterCategory,
    /// Current value in bytes (for size parameters)
    pub value_bytes: Option<u64>,
    /// Current value as number (for numeric parameters)
    pub value_numeric: Option<f64>,
    /// Minimum allowed value
    pub min_value: Option<f64>,
    /// Maximum allowed value
    pub max_value: Option<f64>,
    /// List of valid values (for enumerated parameters)
    pub valid_values: Vec<String>,
    /// Performance impact level
    pub performance_impact: PerformanceImpact,
    /// Security impact level
    pub security_impact: SecurityImpact,
    /// Optimization recommendation
    pub recommendation: Option<ParameterRecommendation>,
    /// Whether the current value is optimal
    pub is_optimal: bool,
    /// Risk level of the current configuration
    pub risk_level: RiskLevel,
    /// Related parameters that should be considered together
    pub related_parameters: Vec<String>,
    /// Collection timestamp
    pub collection_timestamp: DateTimeWrapper,
}

/// Oracle parameter collection for all database parameters
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleParametersCollection {
    /// All database parameters
    pub parameters: Vec<OracleParameterInfo>,
    /// Instance information
    pub instance_info: OracleInstanceInfo,
    /// Parameter analysis summary
    pub analysis_summary: ParameterAnalysisSummary,
    /// Configuration warnings
    pub warnings: Vec<ConfigurationWarning>,
    /// Optimization recommendations
    pub recommendations: Vec<GlobalRecommendation>,
    /// Collection timestamp
    pub collection_timestamp: DateTimeWrapper,
}

/// Oracle instance information
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleInstanceInfo {
    /// Instance name
    pub instance_name: String,
    /// Database name
    pub database_name: String,
    /// Oracle version
    pub version: String,
    /// Instance startup time
    pub startup_time: DateTimeWrapper,
    /// Instance status
    pub status: String,
    /// Database role (PRIMARY, STANDBY)
    pub database_role: String,
    /// Total SGA size in bytes
    pub sga_size: u64,
    /// Total PGA size in bytes
    pub pga_size: u64,
    /// Number of CPU cores
    pub cpu_count: u32,
    /// Total memory in bytes
    pub total_memory: u64,
    /// Character set
    pub character_set: String,
    /// National character set
    pub national_character_set: String,
    /// Archive log mode
    pub archive_log_mode: String,
    /// Flashback enabled
    pub flashback_on: bool,
    /// Force logging enabled
    pub force_logging: bool,
}
