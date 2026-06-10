use super::*;

impl OracleParametersCollection {
    pub(crate) fn recommend_optimizer_mode(param: &OracleParameterInfo) -> Option<ParameterRecommendation> {
        let value = param.value.as_ref()?;
        if value.to_uppercase() == "ALL_ROWS" {
            return None;
        }

        Some(Self::recommendation(
            "ALL_ROWS",
            "Optimizer mode should be set to ALL_ROWS for best performance",
            "Better query execution plans",
            "Low risk - standard recommendation",
            Some("Can be changed dynamically"),
        ))
    }

    pub(crate) fn recommend_statistics_level(param: &OracleParameterInfo) -> Option<ParameterRecommendation> {
        let value = param.value.as_ref()?;
        if value.to_uppercase() == "TYPICAL" {
            return None;
        }

        Some(Self::recommendation(
            "TYPICAL",
            "STATISTICS_LEVEL should be TYPICAL for AWR and automatic statistics",
            "Enables automatic workload repository and statistics collection",
            "Low risk - standard recommendation",
            Some("Required for AWR reports"),
        ))
    }

    pub(crate) fn recommend_audit_trail(param: &OracleParameterInfo) -> Option<ParameterRecommendation> {
        let value = param.value.as_ref()?;
        if value.to_uppercase() != "NONE" {
            return None;
        }

        Some(Self::recommendation(
            "DB",
            "Auditing is disabled - consider enabling for security compliance",
            "Security auditing and compliance tracking",
            "Medium risk - enables audit logging",
            Some("Monitor audit table growth"),
        ))
    }

    pub(crate) fn recommend_archive_dest(param: &OracleParameterInfo) -> Option<ParameterRecommendation> {
        if param.value.as_ref().is_some_and(|value| !value.is_empty()) {
            return None;
        }

        Some(Self::recommendation(
            "LOCATION=/u01/app/oracle/archive",
            "Archive destination not configured - required for backup and recovery",
            "Enables point-in-time recovery and backup strategies",
            "High risk if not configured - data loss possible",
            Some("Ensure sufficient disk space for archive logs"),
        ))
    }
}
