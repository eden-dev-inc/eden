use super::*;

impl ClickhouseSettingsInfo {
    pub(super) fn generate_optimization_recommendations(core_info: &ClickhouseSettingsInfo) -> Vec<ClickhouseSettingOptimization> {
        let mut recommendations = Vec::new();

        if core_info.total_memory_limit > Self::DANGEROUS_MEMORY_THRESHOLD {
            recommendations.push(ClickhouseSettingOptimization {
                setting_name: "max_memory_usage".to_string(),
                current_value: core_info.total_memory_limit.to_string(),
                recommended_value: (Self::DANGEROUS_MEMORY_THRESHOLD / 2).to_string(),
                optimization_reason: "Current memory limit is very high and may cause system instability".to_string(),
                expected_benefit: "Improved system stability and reduced OOM risk".to_string(),
                priority: OptimizationPriority::High,
            });
        }

        if core_info.max_threads > Self::HIGH_THREAD_COUNT_THRESHOLD {
            recommendations.push(ClickhouseSettingOptimization {
                setting_name: "max_threads".to_string(),
                current_value: core_info.max_threads.to_string(),
                recommended_value: "64".to_string(),
                optimization_reason: "Too many threads can cause context switching overhead".to_string(),
                expected_benefit: "Better CPU utilization and reduced context switching".to_string(),
                priority: OptimizationPriority::Medium,
            });
        }

        if core_info.max_connections > Self::HIGH_CONNECTION_THRESHOLD {
            recommendations.push(ClickhouseSettingOptimization {
                setting_name: "max_connections".to_string(),
                current_value: core_info.max_connections.to_string(),
                recommended_value: "4096".to_string(),
                optimization_reason: "Very high connection limit may exhaust system resources".to_string(),
                expected_benefit: "Reduced memory usage and better connection management".to_string(),
                priority: OptimizationPriority::Medium,
            });
        }

        if core_info.query_timeout_seconds > Self::LONG_TIMEOUT_THRESHOLD {
            recommendations.push(ClickhouseSettingOptimization {
                setting_name: "max_execution_time".to_string(),
                current_value: core_info.query_timeout_seconds.to_string(),
                recommended_value: "1800".to_string(),
                optimization_reason: "Very long timeouts can mask inefficient queries".to_string(),
                expected_benefit: "Better query performance monitoring and resource control".to_string(),
                priority: OptimizationPriority::Low,
            });
        }

        recommendations
    }

    pub(super) fn identify_dangerous_settings(
        _core_info: &ClickhouseSettingsInfo,
        detailed: &ClickhouseSettingsDetailedInfo,
    ) -> Vec<ClickhouseDangerousSetting> {
        let mut dangerous = Vec::new();

        for memory_setting in &detailed.memory_settings {
            if memory_setting.memory_impact == MemoryImpactLevel::Critical {
                dangerous.push(ClickhouseDangerousSetting {
                    name: memory_setting.name.clone(),
                    current_value: memory_setting.current_value.clone(),
                    danger_level: DangerLevel::High,
                    risk_description: "High memory usage setting may cause system instability".to_string(),
                    mitigation_steps: vec![
                        "Monitor memory usage closely".to_string(),
                        "Consider reducing the limit".to_string(),
                        "Ensure adequate system memory".to_string(),
                    ],
                    potential_impact: "System crashes, OOM kills, performance degradation".to_string(),
                });
            }
        }

        for perf_setting in &detailed.performance_settings {
            if perf_setting.performance_impact == PerformanceImpactLevel::Negative {
                dangerous.push(ClickhouseDangerousSetting {
                    name: perf_setting.name.clone(),
                    current_value: perf_setting.current_value.clone(),
                    danger_level: DangerLevel::Medium,
                    risk_description: "Setting may negatively impact query performance".to_string(),
                    mitigation_steps: vec![
                        "Review and adjust the setting".to_string(),
                        "Monitor query performance metrics".to_string(),
                    ],
                    potential_impact: "Slower queries, increased resource usage".to_string(),
                });
            }
        }

        dangerous
    }

    pub(super) fn get_replacement_setting(deprecated_setting: &str) -> Option<String> {
        match deprecated_setting {
            "use_uncompressed_cache" => Some("use_uncompressed_cache".to_string()),
            "compile_expressions" => Some("compile_expressions".to_string()),
            "group_by_overflow_mode" => Some("group_by_overflow_mode".to_string()),
            _ => None,
        }
    }

    pub(super) fn get_deprecation_reason(deprecated_setting: &str) -> String {
        match deprecated_setting {
            "use_uncompressed_cache" => "Uncompressed cache is no longer recommended for modern systems".to_string(),
            "compile_expressions" => "Expression compilation has been superseded by better optimization".to_string(),
            "group_by_overflow_mode" => "Modern memory management makes this setting obsolete".to_string(),
            _ => "Setting is deprecated and may be removed in future versions".to_string(),
        }
    }

    pub(super) fn calculate_memory_impact(name: &str, value: &str) -> MemoryImpactLevel {
        if let Ok(numeric_value) = value.parse::<u64>() {
            match name {
                "max_memory_usage" | "max_query_memory_usage" => {
                    if numeric_value > 50_000_000_000 {
                        MemoryImpactLevel::Critical
                    } else if numeric_value > 10_000_000_000 {
                        MemoryImpactLevel::High
                    } else if numeric_value > 1_000_000_000 {
                        MemoryImpactLevel::Medium
                    } else {
                        MemoryImpactLevel::Low
                    }
                }
                _ => MemoryImpactLevel::Low,
            }
        } else {
            MemoryImpactLevel::Low
        }
    }

    pub(super) fn calculate_performance_impact(name: &str, value: &str) -> PerformanceImpactLevel {
        if let Ok(numeric_value) = value.parse::<u64>() {
            match name {
                "max_threads" => {
                    if !(2..=128).contains(&numeric_value) {
                        PerformanceImpactLevel::Negative
                    } else if !(4..=64).contains(&numeric_value) {
                        PerformanceImpactLevel::Neutral
                    } else {
                        PerformanceImpactLevel::Positive
                    }
                }
                "max_execution_time" => {
                    if numeric_value > 3600 {
                        PerformanceImpactLevel::Negative
                    } else {
                        PerformanceImpactLevel::Neutral
                    }
                }
                _ => PerformanceImpactLevel::Neutral,
            }
        } else {
            PerformanceImpactLevel::Neutral
        }
    }

    pub(super) fn calculate_security_level(name: &str, _value: &str) -> SecurityLevel {
        match name {
            n if n.contains("password") || n.contains("auth") => SecurityLevel::Critical,
            n if n.contains("ssl") || n.contains("tls") => SecurityLevel::High,
            n if n.contains("security") => SecurityLevel::Medium,
            _ => SecurityLevel::Low,
        }
    }

    pub(super) fn calculate_limit_impact(name: &str, value: &str) -> LimitImpactLevel {
        if let Ok(numeric_value) = value.parse::<u64>() {
            match name {
                "max_connections" => {
                    if numeric_value > 10000 {
                        LimitImpactLevel::High
                    } else if numeric_value > 1000 {
                        LimitImpactLevel::Medium
                    } else {
                        LimitImpactLevel::Low
                    }
                }
                "max_execution_time" => {
                    if numeric_value > 3600 {
                        LimitImpactLevel::High
                    } else if numeric_value > 300 {
                        LimitImpactLevel::Medium
                    } else {
                        LimitImpactLevel::Low
                    }
                }
                _ => LimitImpactLevel::Low,
            }
        } else {
            LimitImpactLevel::Low
        }
    }

    pub(super) fn calculate_dangerous_settings_count(info: &ClickhouseSettingsInfo) -> u64 {
        let mut dangerous = 0;

        if info.total_memory_limit > Self::DANGEROUS_MEMORY_THRESHOLD {
            dangerous += 1;
        }
        if info.max_threads > Self::HIGH_THREAD_COUNT_THRESHOLD {
            dangerous += 1;
        }
        if info.max_connections > Self::HIGH_CONNECTION_THRESHOLD {
            dangerous += 1;
        }
        if info.query_timeout_seconds > Self::LONG_TIMEOUT_THRESHOLD {
            dangerous += 1;
        }

        dangerous
    }

    pub(super) fn calculate_security_settings_count(info: &ClickhouseSettingsInfo) -> u64 {
        let mut security_related = 0;

        if info.max_connections > 0 {
            security_related += 1;
        }
        if info.query_timeout_seconds > 0 {
            security_related += 1;
        }
        if info.max_query_memory_limit > 0 {
            security_related += 1;
        }

        security_related
    }

    pub(super) fn calculate_optimization_needs(info: &ClickhouseSettingsInfo) -> u64 {
        let mut needs = 0;

        if info.total_memory_limit > Self::DANGEROUS_MEMORY_THRESHOLD {
            needs += 1;
        }
        if info.max_threads > Self::HIGH_THREAD_COUNT_THRESHOLD {
            needs += 1;
        }
        if info.max_connections > Self::HIGH_CONNECTION_THRESHOLD {
            needs += 1;
        }
        if info.query_timeout_seconds > Self::LONG_TIMEOUT_THRESHOLD {
            needs += 1;
        }
        if info.custom_settings_count > 50 {
            needs += 1;
        }

        needs
    }

    pub(super) fn determine_resource_type(name: &str) -> ResourceType {
        if name.contains("memory") || name.contains("Memory") {
            ResourceType::Memory
        } else if name.contains("thread") || name.contains("Thread") {
            ResourceType::CPU
        } else if name.contains("connection") || name.contains("Connection") {
            ResourceType::Network
        } else if name.contains("timeout") || name.contains("Timeout") {
            ResourceType::Time
        } else {
            ResourceType::Other
        }
    }

    pub(super) fn get_recommended_memory_value(name: &str) -> Option<String> {
        match name {
            "max_memory_usage" => Some("0".to_string()),
            "max_query_memory_usage" => Some("10000000000".to_string()),
            "max_memory_usage_for_user" => Some("0".to_string()),
            _ => None,
        }
    }

    pub(super) fn get_recommended_performance_value(name: &str) -> Option<String> {
        match name {
            "max_threads" => Some("0".to_string()),
            "max_parallel_replicas" => Some("1".to_string()),
            "parallel_replicas_count" => Some("0".to_string()),
            _ => None,
        }
    }

    pub(super) fn get_recommended_security_value(name: &str) -> Option<String> {
        match name {
            n if n.contains("ssl") => Some("1".to_string()),
            n if n.contains("auth") => Some("1".to_string()),
            _ => None,
        }
    }

    pub(super) fn get_recommended_limit_value(name: &str) -> Option<String> {
        match name {
            "max_connections" => Some("4096".to_string()),
            "max_execution_time" => Some("1800".to_string()),
            "max_query_size" => Some("268435456".to_string()),
            _ => None,
        }
    }
}
