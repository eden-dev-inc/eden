use super::*;
impl OracleParametersCollection {
    pub(crate) fn generate_global_recommendations(
        parameters: &[OracleParameterInfo],
        instance_info: &OracleInstanceInfo,
    ) -> Vec<GlobalRecommendation> {
        let mut recommendations = Vec::new();

        // Memory management recommendations
        Self::add_memory_recommendations(&mut recommendations, parameters, instance_info);

        // Performance recommendations
        Self::add_performance_recommendations(&mut recommendations, parameters);

        // Security recommendations
        Self::add_security_recommendations(&mut recommendations, parameters);

        // Backup and recovery recommendations
        Self::add_backup_recommendations(&mut recommendations, parameters);

        recommendations
    }

    // Parameter analysis
    pub(crate) fn add_memory_recommendations(
        recommendations: &mut Vec<GlobalRecommendation>,
        parameters: &[OracleParameterInfo],
        _instance_info: &OracleInstanceInfo,
    ) {
        let memory_target = parameters.iter().find(|p| p.name.to_uppercase() == "MEMORY_TARGET");

        if memory_target.map(|mt| mt.value.is_none()).unwrap_or(true) {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::High,
                title: "Enable Automatic Memory Management".to_string(),
                description: "Consider enabling AMM by setting MEMORY_TARGET for simplified memory management".to_string(),
                affected_parameters: vec!["MEMORY_TARGET".to_string(), "MEMORY_MAX_TARGET".to_string()],
                expected_benefit: "Automatic balancing between SGA and PGA based on workload".to_string(),
                implementation_difficulty: ImplementationDifficulty::Medium,
                requires_restart: true,
            });
        }

        // Check for undersized memory components
        let sga_target = parameters.iter().find(|p| p.name.to_uppercase() == "SGA_TARGET");
        if let Some(sga) = sga_target
            && let Some(bytes) = sga.value_bytes
            && bytes < 1024 * 1024 * 1024
        {
            // Less than 1GB
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::Medium,
                title: "Increase SGA Target".to_string(),
                description: "SGA_TARGET appears to be undersized for modern workloads".to_string(),
                affected_parameters: vec!["SGA_TARGET".to_string()],
                expected_benefit: "Better caching and reduced physical I/O".to_string(),
                implementation_difficulty: ImplementationDifficulty::Easy,
                requires_restart: false,
            });
        }
    }

    pub(crate) fn add_performance_recommendations(recommendations: &mut Vec<GlobalRecommendation>, parameters: &[OracleParameterInfo]) {
        let optimizer_mode = parameters.iter().find(|p| p.name.to_uppercase() == "OPTIMIZER_MODE");
        if let Some(opt) = optimizer_mode
            && let Some(value) = &opt.value
            && value.to_uppercase() != "ALL_ROWS"
        {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Performance,
                priority: RecommendationPriority::Medium,
                title: "Set Optimizer Mode to ALL_ROWS".to_string(),
                description: "Current optimizer mode may not be optimal for OLTP workloads".to_string(),
                affected_parameters: vec!["OPTIMIZER_MODE".to_string()],
                expected_benefit: "Better execution plans for typical SQL workloads".to_string(),
                implementation_difficulty: ImplementationDifficulty::Easy,
                requires_restart: false,
            });
        }

        let stats_level = parameters.iter().find(|p| p.name.to_uppercase() == "STATISTICS_LEVEL");
        if let Some(stats) = stats_level
            && let Some(value) = &stats.value
            && value.to_uppercase() != "TYPICAL"
        {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Performance,
                priority: RecommendationPriority::High,
                title: "Set Statistics Level to TYPICAL".to_string(),
                description: "STATISTICS_LEVEL should be TYPICAL to enable AWR and automatic statistics".to_string(),
                affected_parameters: vec!["STATISTICS_LEVEL".to_string()],
                expected_benefit: "Enables AWR reports and automatic optimizer statistics collection".to_string(),
                implementation_difficulty: ImplementationDifficulty::Easy,
                requires_restart: true,
            });
        }
    }

    pub(crate) fn add_security_recommendations(recommendations: &mut Vec<GlobalRecommendation>, parameters: &[OracleParameterInfo]) {
        let audit_trail = parameters.iter().find(|p| p.name.to_uppercase() == "AUDIT_TRAIL");
        if let Some(audit) = audit_trail
            && let Some(value) = &audit.value
            && value.to_uppercase() == "NONE"
        {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Security,
                priority: RecommendationPriority::High,
                title: "Enable Database Auditing".to_string(),
                description: "Database auditing is currently disabled which may not meet security compliance requirements".to_string(),
                affected_parameters: vec!["AUDIT_TRAIL".to_string()],
                expected_benefit: "Security compliance and audit trail for database activities".to_string(),
                implementation_difficulty: ImplementationDifficulty::Easy,
                requires_restart: true,
            });
        }

        // Check for deprecated security parameters
        let deprecated_security: Vec<&OracleParameterInfo> = parameters
            .iter()
            .filter(|p| p.is_deprecated && matches!(p.security_impact, SecurityImpact::High | SecurityImpact::Critical))
            .collect();

        if !deprecated_security.is_empty() {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Security,
                priority: RecommendationPriority::Medium,
                title: "Remove Deprecated Security Parameters".to_string(),
                description: "Some deprecated security-related parameters are still in use".to_string(),
                affected_parameters: deprecated_security.iter().map(|p| p.name.clone()).collect(),
                expected_benefit: "Improved security posture and future compatibility".to_string(),
                implementation_difficulty: ImplementationDifficulty::Medium,
                requires_restart: true,
            });
        }
    }

    pub(crate) fn add_backup_recommendations(recommendations: &mut Vec<GlobalRecommendation>, parameters: &[OracleParameterInfo]) {
        let archive_dest = parameters.iter().find(|p| p.name.to_uppercase() == "LOG_ARCHIVE_DEST_1");
        if let Some(dest) = archive_dest
            && (dest.value.is_none() || dest.value.as_ref().map(|s| s.is_empty()).unwrap_or(true))
        {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Backup,
                priority: RecommendationPriority::Critical,
                title: "Configure Archive Log Destination".to_string(),
                description: "Archive log destination is not configured, which prevents proper backup and recovery".to_string(),
                affected_parameters: vec!["LOG_ARCHIVE_DEST_1".to_string(), "LOG_ARCHIVE_FORMAT".to_string()],
                expected_benefit: "Enables point-in-time recovery and complete backup strategy".to_string(),
                implementation_difficulty: ImplementationDifficulty::Medium,
                requires_restart: false,
            });
        }

        let recovery_dest = parameters.iter().find(|p| p.name.to_uppercase() == "DB_RECOVERY_FILE_DEST");
        if let Some(recovery) = recovery_dest
            && (recovery.value.is_none() || recovery.value.as_ref().map(|s| s.is_empty()).unwrap_or(true))
        {
            recommendations.push(GlobalRecommendation {
                category: RecommendationCategory::Backup,
                priority: RecommendationPriority::High,
                title: "Configure Flash Recovery Area".to_string(),
                description: "Flash Recovery Area is not configured for centralized backup file management".to_string(),
                affected_parameters: vec!["DB_RECOVERY_FILE_DEST".to_string(), "DB_RECOVERY_FILE_DEST_SIZE".to_string()],
                expected_benefit: "Centralized backup file management and automatic cleanup".to_string(),
                implementation_difficulty: ImplementationDifficulty::Easy,
                requires_restart: false,
            });
        }
    }
}
