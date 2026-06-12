use super::*;
impl OracleParametersCollection {
    pub(crate) fn assess_risk_level(param: &OracleParameterInfo) -> RiskLevel {
        // High-risk parameters
        if matches!(param.name.to_uppercase().as_str(), "CONTROL_FILES" | "DB_RECOVERY_FILE_DEST" | "LOG_ARCHIVE_DEST_1")
            && (param.value.is_none() || param.value.as_ref().map(|s| s.is_empty()).unwrap_or(true))
        {
            return RiskLevel::Critical;
        }

        // Deprecated parameters in use
        if param.is_deprecated && param.value.is_some() {
            return RiskLevel::High;
        }

        // Memory parameters with extreme values
        if (param.name.contains("memory") || param.name.contains("target"))
            && let Some(value_bytes) = param.value_bytes
            && (!(64 * 1024 * 1024..=64 * 1024 * 1024 * 1024).contains(&value_bytes))
        {
            // < 64MB or > 64GB
            return RiskLevel::High;
        }

        // Security-related parameters
        if matches!(param.security_impact, SecurityImpact::High | SecurityImpact::Critical)
            && let Some(value) = &param.value
        {
            match param.name.to_uppercase().as_str() {
                "AUDIT_TRAIL" if value.to_uppercase() == "NONE" => return RiskLevel::Medium,
                "REMOTE_LOGIN_PASSWORDFILE" if value.to_uppercase() == "NONE" => {
                    return RiskLevel::Medium;
                }
                _ => {}
            }
        }

        RiskLevel::Low
    }

    pub(crate) fn is_parameter_optimal(param: &OracleParameterInfo, instance_info: &OracleInstanceInfo) -> bool {
        // If there's a recommendation, it's not optimal
        if param.recommendation.is_some() {
            return false;
        }

        // If it's deprecated and in use, it's not optimal
        if param.is_deprecated && param.value.is_some() {
            return false;
        }

        // If risk level is high or critical, it's not optimal
        if matches!(param.risk_level, RiskLevel::High | RiskLevel::Critical) {
            return false;
        }

        // Parameter-specific optimality checks
        match param.name.to_uppercase().as_str() {
            "OPTIMIZER_MODE" => param.value.as_ref().is_some_and(|v| v.to_uppercase() == "ALL_ROWS"),
            "STATISTICS_LEVEL" => param.value.as_ref().is_some_and(|v| v.to_uppercase() == "TYPICAL"),
            "MEMORY_TARGET" => param.value_bytes.is_some_and(|bytes| {
                let total_memory = instance_info.total_memory;
                let optimal_range = (total_memory * 60 / 100)..(total_memory * 80 / 100);
                optimal_range.contains(&bytes)
            }),
            _ => true,
        }
    }

    pub(crate) fn calculate_health_score(parameters: &[OracleParameterInfo]) -> f64 {
        let total = parameters.len() as f64;
        if total == 0.0 {
            return 100.0;
        }

        let mut score = 100.0;

        // Deduct points for deprecated parameters
        let deprecated_count = parameters.iter().filter(|p| p.is_deprecated && p.value.is_some()).count() as f64;
        score -= (deprecated_count / total) * 20.0;

        // Deduct points for high-risk parameters
        let high_risk_count = parameters.iter().filter(|p| matches!(p.risk_level, RiskLevel::Critical)).count() as f64;
        score -= (high_risk_count / total) * 30.0;

        // Deduct points for non-optimal parameters
        let non_optimal_count = parameters.iter().filter(|p| !p.is_optimal).count() as f64;
        score -= (non_optimal_count / total) * 10.0;

        score.max(0.0)
    }

    pub(crate) fn calculate_memory_efficiency_score(parameters: &[OracleParameterInfo], _instance_info: &OracleInstanceInfo) -> f64 {
        let mut score = 100.0_f64;

        // Check if AMM is enabled
        let memory_target = parameters.iter().find(|p| p.name.to_uppercase() == "MEMORY_TARGET");
        if memory_target.map(|mt| mt.value.is_none()).unwrap_or(true) {
            score -= 20.0; // No AMM
        }

        // Check SGA sizing
        let sga_target = parameters.iter().find(|p| p.name.to_uppercase() == "SGA_TARGET");
        if let Some(sga) = sga_target
            && let Some(bytes) = sga.value_bytes
            && bytes < 512 * 1024 * 1024
        {
            // Less than 512MB
            score -= 15.0;
        }

        // Check PGA sizing
        let pga_target = parameters.iter().find(|p| p.name.to_uppercase() == "PGA_AGGREGATE_TARGET");
        if let Some(pga) = pga_target
            && let Some(bytes) = pga.value_bytes
            && bytes < 256 * 1024 * 1024
        {
            // Less than 256MB
            score -= 15.0;
        }

        score.max(0.0_f64)
    }

    pub(crate) fn calculate_security_score(parameters: &[OracleParameterInfo]) -> f64 {
        let mut score = 100.0;

        // Check audit trail
        let audit_trail = parameters.iter().find(|p| p.name.to_uppercase() == "AUDIT_TRAIL");
        if let Some(audit) = audit_trail
            && let Some(value) = &audit.value
            && value.to_uppercase() == "NONE"
        {
            score -= 25.0;
        }

        // Check remote authentication
        let remote_auth = parameters.iter().find(|p| p.name.to_uppercase() == "REMOTE_LOGIN_PASSWORDFILE");
        if let Some(remote) = remote_auth
            && let Some(value) = &remote.value
            && value.to_uppercase() == "NONE"
        {
            score -= 20.0;
        }

        // Check for deprecated security parameters
        let deprecated_security = parameters
            .iter()
            .filter(|p| p.is_deprecated && matches!(p.security_impact, SecurityImpact::High | SecurityImpact::Critical))
            .count();
        score -= (deprecated_security as f64) * 10.0;

        score.max(0.0)
    }

    pub(crate) fn calculate_performance_score(parameters: &[OracleParameterInfo]) -> f64 {
        let mut score = 100.0;

        // Check optimizer mode
        let optimizer_mode = parameters.iter().find(|p| p.name.to_uppercase() == "OPTIMIZER_MODE");
        if let Some(opt) = optimizer_mode
            && let Some(value) = &opt.value
            && value.to_uppercase() != "ALL_ROWS"
        {
            score -= 15.0;
        }

        // Check statistics level
        let stats_level = parameters.iter().find(|p| p.name.to_uppercase() == "STATISTICS_LEVEL");
        if let Some(stats) = stats_level
            && let Some(value) = &stats.value
            && value.to_uppercase() != "TYPICAL"
        {
            score -= 20.0;
        }

        // Check for performance-critical parameters with issues
        let perf_issues = parameters
            .iter()
            .filter(|p| matches!(p.performance_impact, PerformanceImpact::High | PerformanceImpact::Critical) && !p.is_optimal)
            .count();
        score -= (perf_issues as f64) * 10.0;

        score.max(0.0)
    }
}
