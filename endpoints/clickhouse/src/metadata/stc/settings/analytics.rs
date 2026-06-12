use super::*;

impl ClickhouseSettingsInfo {
    pub fn has_inconsistent_settings(&self) -> bool {
        self.inconsistent_settings_count > 0
    }

    pub fn has_deprecated_settings(&self) -> bool {
        self.deprecated_settings_count > 0
    }

    pub fn has_dangerous_settings(&self) -> bool {
        self.dangerous_settings_count > 0
    }

    pub fn has_optimization_opportunities(&self) -> bool {
        self.settings_needing_optimization > 0
    }

    pub fn has_detailed_settings(&self) -> bool {
        self.detailed_settings.is_some()
    }

    pub fn get_customization_ratio(&self) -> f64 {
        if self.total_settings_count == 0 {
            return 0.0;
        }
        self.custom_settings_count as f64 / self.total_settings_count as f64
    }

    pub fn get_deprecation_ratio(&self) -> f64 {
        if self.custom_settings_count == 0 {
            return 0.0;
        }
        self.deprecated_settings_count as f64 / self.custom_settings_count as f64
    }

    pub fn get_danger_ratio(&self) -> f64 {
        if self.total_settings_count == 0 {
            return 0.0;
        }
        self.dangerous_settings_count as f64 / self.total_settings_count as f64
    }

    pub fn get_total_memory_limit_gb(&self) -> f64 {
        self.total_memory_limit as f64 / 1_073_741_824.0
    }

    pub fn get_max_query_memory_limit_gb(&self) -> f64 {
        self.max_query_memory_limit as f64 / 1_073_741_824.0
    }

    pub fn get_query_timeout_minutes(&self) -> f64 {
        self.query_timeout_seconds as f64 / 60.0
    }

    pub fn get_configuration_health_status(&self) -> ConfigurationHealthStatus {
        let danger_ratio = self.get_danger_ratio();
        let deprecation_ratio = self.get_deprecation_ratio();
        let has_inconsistencies = self.has_inconsistent_settings();

        if danger_ratio > 0.1 || deprecation_ratio > 0.3 || has_inconsistencies {
            ConfigurationHealthStatus::Critical
        } else if danger_ratio > 0.05 || deprecation_ratio > 0.2 || self.dangerous_settings_count > 0 {
            ConfigurationHealthStatus::Warning
        } else if danger_ratio > 0.0 || deprecation_ratio > 0.1 || self.settings_needing_optimization > 0 {
            ConfigurationHealthStatus::Attention
        } else {
            ConfigurationHealthStatus::Healthy
        }
    }

    pub fn get_configuration_complexity(&self) -> ConfigurationComplexity {
        let customization_ratio = self.get_customization_ratio();

        if customization_ratio > 0.5 {
            ConfigurationComplexity::VeryHigh
        } else if customization_ratio > 0.3 {
            ConfigurationComplexity::High
        } else if customization_ratio > 0.2 {
            ConfigurationComplexity::Medium
        } else if customization_ratio > 0.1 {
            ConfigurationComplexity::Low
        } else {
            ConfigurationComplexity::Minimal
        }
    }

    pub fn get_resource_allocation_efficiency(&self) -> f64 {
        let mut efficiency_score = 1.0;

        if self.total_memory_limit > Self::DANGEROUS_MEMORY_THRESHOLD {
            efficiency_score -= 0.3;
        }
        if self.max_threads > Self::HIGH_THREAD_COUNT_THRESHOLD {
            efficiency_score -= 0.2;
        }
        if self.max_connections > Self::HIGH_CONNECTION_THRESHOLD {
            efficiency_score -= 0.2;
        }
        if self.query_timeout_seconds > Self::LONG_TIMEOUT_THRESHOLD {
            efficiency_score -= 0.1;
        }

        efficiency_score -= self.dangerous_settings_count as f64 * 0.05;
        efficiency_score.clamp(0.0, 1.0)
    }

    pub fn get_security_posture_score(&self) -> f64 {
        let mut security_score = 1.0;

        if self.security_settings_count > 0 {
            security_score -= 0.1;
        }

        security_score -= self.deprecated_settings_count as f64 * 0.05;
        security_score -= self.dangerous_settings_count as f64 * 0.1;
        security_score -= self.inconsistent_settings_count as f64 * 0.02;
        security_score.clamp(0.0, 1.0)
    }

    pub fn get_maintenance_burden(&self) -> MaintenanceBurden {
        let total_issues = self.deprecated_settings_count
            + self.dangerous_settings_count
            + self.inconsistent_settings_count
            + self.settings_needing_optimization;

        if total_issues > 20 {
            MaintenanceBurden::VeryHigh
        } else if total_issues > 10 {
            MaintenanceBurden::High
        } else if total_issues > 5 {
            MaintenanceBurden::Medium
        } else if total_issues > 0 {
            MaintenanceBurden::Low
        } else {
            MaintenanceBurden::Minimal
        }
    }
}
