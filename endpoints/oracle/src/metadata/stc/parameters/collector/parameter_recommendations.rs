use super::*;

mod memory_and_capacity;
mod policy_and_security;

impl OracleParametersCollection {
    pub(crate) fn recommendation(
        recommended_value: impl Into<String>,
        reason: impl Into<String>,
        expected_improvement: impl Into<String>,
        risk_assessment: impl Into<String>,
        implementation_notes: Option<&str>,
    ) -> ParameterRecommendation {
        ParameterRecommendation {
            recommended_value: recommended_value.into(),
            reason: reason.into(),
            expected_improvement: expected_improvement.into(),
            risk_assessment: risk_assessment.into(),
            implementation_notes: implementation_notes.map(str::to_string),
        }
    }

    pub(crate) fn generate_parameter_recommendation(
        param: &OracleParameterInfo,
        instance_info: &OracleInstanceInfo,
    ) -> Option<ParameterRecommendation> {
        match param.name.to_uppercase().as_str() {
            "MEMORY_TARGET" => Self::recommend_memory_target(param, instance_info),
            "SGA_TARGET" => Self::recommend_sga_target(param, instance_info),
            "PGA_AGGREGATE_TARGET" => Self::recommend_pga_target(param, instance_info),
            "PROCESSES" => Self::recommend_processes(param, instance_info),
            "SESSIONS" => Self::recommend_sessions(param, instance_info),
            "UNDO_RETENTION" => Self::recommend_undo_retention(param),
            "OPTIMIZER_MODE" => Self::recommend_optimizer_mode(param),
            "STATISTICS_LEVEL" => Self::recommend_statistics_level(param),
            "AUDIT_TRAIL" => Self::recommend_audit_trail(param),
            "LOG_ARCHIVE_DEST_1" => Self::recommend_archive_dest(param),
            _ => None,
        }
    }
}
