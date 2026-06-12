use super::*;
impl OracleParameterInfo {
    /// Gets the parameter value in human-readable format
    pub fn display_value(&self) -> String {
        match &self.value {
            Some(val) => {
                if let Some(bytes) = self.value_bytes {
                    Self::format_bytes(bytes)
                } else {
                    val.clone()
                }
            }
            None => "Not Set".to_string(),
        }
    }

    /// Converts bytes to human-readable format
    pub fn format_bytes(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{:.2} {}", size, UNITS[unit_index])
        }
    }

    /// Checks if the parameter is critical for database operation
    pub fn is_critical(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "CONTROL_FILES"
                | "DB_NAME"
                | "DB_UNIQUE_NAME"
                | "INSTANCE_NAME"
                | "SGA_TARGET"
                | "PGA_AGGREGATE_TARGET"
                | "MEMORY_TARGET"
                | "PROCESSES"
        )
    }

    /// Gets the parameter importance level
    pub fn importance_level(&self) -> ParameterImportance {
        if self.is_critical() {
            ParameterImportance::Critical
        } else if matches!(self.performance_impact, PerformanceImpact::High | PerformanceImpact::Critical)
            || matches!(self.security_impact, SecurityImpact::High | SecurityImpact::Critical)
        {
            ParameterImportance::High
        } else if self.is_basic {
            ParameterImportance::Medium
        } else {
            ParameterImportance::Low
        }
    }

    /// Gets parameter status for dashboard display
    pub fn status(&self) -> ParameterStatus {
        if matches!(self.risk_level, RiskLevel::Critical) {
            ParameterStatus::Critical
        } else if self.is_deprecated && self.value.is_some() {
            ParameterStatus::Deprecated
        } else if !self.is_optimal {
            ParameterStatus::SubOptimal
        } else if self.recommendation.is_some() {
            ParameterStatus::CanImprove
        } else {
            ParameterStatus::Optimal
        }
    }

    /// Gets color code for UI display
    pub fn status_color(&self) -> &'static str {
        match self.status() {
            ParameterStatus::Optimal => "#28a745",    // Green
            ParameterStatus::CanImprove => "#ffc107", // Yellow
            ParameterStatus::SubOptimal => "#fd7e14", // Orange
            ParameterStatus::Deprecated => "#6c757d", // Gray
            ParameterStatus::Critical => "#dc3545",   // Red
        }
    }

    /// Gets the category description
    pub fn category_description(&self) -> &'static str {
        match self.category {
            ParameterCategory::Memory => "Memory Management",
            ParameterCategory::Performance => "Performance Tuning",
            ParameterCategory::Security => "Security Configuration",
            ParameterCategory::Storage => "Storage Management",
            ParameterCategory::Network => "Network Configuration",
            ParameterCategory::Backup => "Backup and Recovery",
            ParameterCategory::Recovery => "Recovery Settings",
            ParameterCategory::Optimizer => "Query Optimizer",
            ParameterCategory::Processes => "Process Management",
            ParameterCategory::Sessions => "Session Management",
            ParameterCategory::Undo => "Undo Management",
            ParameterCategory::Logging => "Logging Configuration",
            ParameterCategory::Auditing => "Audit Settings",
            ParameterCategory::Encryption => "Encryption Settings",
            ParameterCategory::Partitioning => "Partitioning",
            ParameterCategory::Parallel => "Parallel Processing",
            ParameterCategory::Clustering => "Cluster Configuration",
            ParameterCategory::Replication => "Replication Settings",
            ParameterCategory::Compatibility => "Compatibility Settings",
            ParameterCategory::Advanced => "Advanced Configuration",
            ParameterCategory::Other => "Other Settings",
        }
    }

    /// Checks if parameter requires restart to take effect
    pub fn requires_restart(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "MEMORY_TARGET"
                | "MEMORY_MAX_TARGET"
                | "SGA_MAX_SIZE"
                | "PROCESSES"
                | "DB_CACHE_SIZE"
                | "SHARED_POOL_SIZE"
                | "JAVA_POOL_SIZE"
                | "LARGE_POOL_SIZE"
                | "CONTROL_FILES"
                | "DB_NAME"
                | "INSTANCE_NAME"
                | "CLUSTER_DATABASE"
        )
    }

    /// Gets modification command for the parameter
    pub fn get_modification_command(&self, new_value: &str) -> String {
        if self.requires_restart() {
            format!("ALTER SYSTEM SET {}='{}' SCOPE=SPFILE;", self.name, new_value)
        } else if self.is_system_modifiable {
            format!("ALTER SYSTEM SET {}='{}' SCOPE=BOTH;", self.name, new_value)
        } else {
            format!("ALTER SESSION SET {}='{}';", self.name, new_value)
        }
    }

    /// Gets validation rules for the parameter
    pub fn get_validation_rules(&self) -> Vec<String> {
        let mut rules = Vec::new();

        match self.name.to_uppercase().as_str() {
            "PROCESSES" => {
                rules.push("Must be between 6 and 2147483647".to_string());
                rules.push("Should be set based on expected concurrent connections".to_string());
            }
            "SESSIONS" => {
                rules.push("Should be approximately 1.1 * PROCESSES + 5".to_string());
                rules.push("Must be between 1 and 2147483647".to_string());
            }
            "MEMORY_TARGET" => {
                rules.push("Should not exceed 80% of available system memory".to_string());
                rules.push("Must be in multiples of granule size".to_string());
            }
            "SGA_TARGET" => {
                rules.push("Should be 60-80% of total memory allocation".to_string());
                rules.push("Must not exceed SGA_MAX_SIZE".to_string());
            }
            "PGA_AGGREGATE_TARGET" => {
                rules.push("Should be 20-30% of total memory allocation".to_string());
                rules.push("Must be at least 10MB".to_string());
            }
            _ => {
                rules.push("Follow Oracle documentation guidelines".to_string());
            }
        }

        rules
    }
}
