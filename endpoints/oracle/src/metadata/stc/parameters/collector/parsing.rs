use super::*;
impl OracleParametersCollection {
    pub(crate) fn parse_parameter_category(category_str: &str) -> ParameterCategory {
        match category_str.to_uppercase().as_str() {
            "MEMORY" => ParameterCategory::Memory,
            "PERFORMANCE" => ParameterCategory::Performance,
            "SECURITY" => ParameterCategory::Security,
            "LOGGING" => ParameterCategory::Logging,
            "UNDO" => ParameterCategory::Undo,
            "SESSIONS" => ParameterCategory::Sessions,
            "BACKUP" => ParameterCategory::Backup,
            "NETWORK" => ParameterCategory::Network,
            _ => ParameterCategory::Other,
        }
    }

    pub(crate) fn parse_performance_impact(impact_str: &str) -> PerformanceImpact {
        match impact_str.to_uppercase().as_str() {
            "CRITICAL" => PerformanceImpact::Critical,
            "HIGH" => PerformanceImpact::High,
            "MEDIUM" => PerformanceImpact::Medium,
            "LOW" => PerformanceImpact::Low,
            _ => PerformanceImpact::None,
        }
    }

    pub(crate) fn parse_security_impact(impact_str: &str) -> SecurityImpact {
        match impact_str.to_uppercase().as_str() {
            "CRITICAL" => SecurityImpact::Critical,
            "HIGH" => SecurityImpact::High,
            "MEDIUM" => SecurityImpact::Medium,
            "LOW" => SecurityImpact::Low,
            _ => SecurityImpact::None,
        }
    }

    pub(crate) fn parse_bytes_value(value_str: &str) -> Option<u64> {
        // Handle various byte notations (K, M, G, T)
        let value_str = value_str.trim().to_uppercase();

        if let Some(num_str) = value_str.strip_suffix('K') {
            num_str.parse::<u64>().ok().map(|n| n * 1024)
        } else if let Some(num_str) = value_str.strip_suffix('M') {
            num_str.parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num_str) = value_str.strip_suffix('G') {
            num_str.parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num_str) = value_str.strip_suffix('T') {
            num_str.parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024 * 1024)
        } else {
            value_str.parse::<u64>().ok()
        }
    }

    pub(crate) fn is_parameter_modifiable(name: &str) -> bool {
        // Most Oracle parameters are modifiable, but some are not
        !matches!(
            name.to_uppercase().as_str(),
            "CONTROL_FILES" | "DB_NAME" | "DB_UNIQUE_NAME" | "INSTANCE_NAME" | "CLUSTER_DATABASE" | "CLUSTER_DATABASE_INSTANCES"
        )
    }

    pub(crate) fn is_system_modifiable(name: &str) -> bool {
        // Parameters that can be modified at system level
        matches!(
            name.to_uppercase().as_str(),
            "SGA_TARGET"
                | "PGA_AGGREGATE_TARGET"
                | "MEMORY_TARGET"
                | "PROCESSES"
                | "SESSIONS"
                | "OPTIMIZER_MODE"
                | "CURSOR_SHARING"
                | "STATISTICS_LEVEL"
        )
    }

    pub(crate) fn is_session_modifiable(name: &str) -> bool {
        // Parameters that can be modified at session level
        matches!(
            name.to_uppercase().as_str(),
            "OPTIMIZER_MODE"
                | "OPTIMIZER_INDEX_COST_ADJ"
                | "OPTIMIZER_INDEX_CACHING"
                | "CURSOR_SHARING"
                | "SQL_TRACE"
                | "TIMED_STATISTICS"
                | "SORT_AREA_SIZE"
        )
    }

    pub(crate) fn is_instance_modifiable(name: &str) -> bool {
        // Parameters that require instance restart
        matches!(
            name.to_uppercase().as_str(),
            "MEMORY_TARGET" | "MEMORY_MAX_TARGET" | "SGA_MAX_SIZE" | "PROCESSES" | "DB_CACHE_SIZE" | "SHARED_POOL_SIZE" | "JAVA_POOL_SIZE"
        )
    }

    pub(crate) fn get_modify_scope(name: &str) -> String {
        if Self::is_instance_modifiable(name) {
            "SPFILE".to_string()
        } else if Self::is_system_modifiable(name) {
            "BOTH".to_string()
        } else {
            "MEMORY".to_string()
        }
    }

    pub(crate) fn get_related_parameters(name: &str) -> Vec<String> {
        match name.to_uppercase().as_str() {
            "MEMORY_TARGET" => vec![
                "MEMORY_MAX_TARGET".to_string(),
                "SGA_TARGET".to_string(),
                "PGA_AGGREGATE_TARGET".to_string(),
            ],
            "SGA_TARGET" => vec![
                "SGA_MAX_SIZE".to_string(),
                "DB_CACHE_SIZE".to_string(),
                "SHARED_POOL_SIZE".to_string(),
                "LARGE_POOL_SIZE".to_string(),
            ],
            "PGA_AGGREGATE_TARGET" => vec!["PGA_AGGREGATE_LIMIT".to_string(), "WORKAREA_SIZE_POLICY".to_string()],
            "PROCESSES" => vec!["SESSIONS".to_string(), "TRANSACTIONS".to_string()],
            "UNDO_RETENTION" => vec!["UNDO_TABLESPACE".to_string(), "UNDO_MANAGEMENT".to_string()],
            _ => Vec::new(),
        }
    }
}
