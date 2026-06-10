use super::*;

impl OracleParametersCollection {
    pub(crate) fn recommend_memory_target(
        param: &OracleParameterInfo,
        instance_info: &OracleInstanceInfo,
    ) -> Option<ParameterRecommendation> {
        if let Some(value_bytes) = param.value_bytes {
            let total_memory = instance_info.total_memory;
            let recommended_percentage = if total_memory > 8 * 1024 * 1024 * 1024 { 80 } else { 70 };
            let recommended_bytes = (total_memory as f64 * recommended_percentage as f64 / 100.0) as u64;

            if value_bytes < recommended_bytes / 2 || value_bytes > recommended_bytes * 2 {
                return Some(Self::recommendation(
                    format!("{}G", recommended_bytes / (1024 * 1024 * 1024)),
                    format!(
                        "Current value {}MB is not optimal for system with {}GB total memory",
                        value_bytes / (1024 * 1024),
                        total_memory / (1024 * 1024 * 1024)
                    ),
                    "Better memory utilization and automatic memory management",
                    "Low risk - Oracle will manage memory allocation automatically",
                    Some("Requires instance restart"),
                ));
            }
            return None;
        }

        Some(Self::recommendation(
            "Enable Automatic Memory Management",
            "MEMORY_TARGET is not set - consider enabling AMM",
            "Simplified memory management and better resource utilization",
            "Low risk - improves overall memory management",
            Some("Set based on available system memory"),
        ))
    }

    pub(crate) fn recommend_sga_target(
        param: &OracleParameterInfo,
        _instance_info: &OracleInstanceInfo,
    ) -> Option<ParameterRecommendation> {
        let value_bytes = param.value_bytes?;
        if value_bytes >= 512 * 1024 * 1024 {
            return None;
        }

        Some(Self::recommendation(
            "1G",
            "SGA_TARGET is very small - may cause performance issues",
            "Better caching and reduced I/O",
            "Low risk - more memory for caching",
            Some("Monitor memory usage after change"),
        ))
    }

    pub(crate) fn recommend_pga_target(
        param: &OracleParameterInfo,
        _instance_info: &OracleInstanceInfo,
    ) -> Option<ParameterRecommendation> {
        let value_bytes = param.value_bytes?;
        if value_bytes >= 256 * 1024 * 1024 {
            return None;
        }

        Some(Self::recommendation(
            "512M",
            "PGA_AGGREGATE_TARGET is too small for modern workloads",
            "Better sort and hash operation performance",
            "Low risk - improves query performance",
            Some("Monitor PGA usage with v$pgastat"),
        ))
    }

    pub(crate) fn recommend_processes(param: &OracleParameterInfo, _instance_info: &OracleInstanceInfo) -> Option<ParameterRecommendation> {
        let value = param.value_numeric?;
        if value >= 150.0 {
            return None;
        }

        Some(Self::recommendation(
            "300",
            "PROCESSES value is low for modern applications",
            "Support for more concurrent connections",
            "Low risk - allows more connections",
            Some("Requires instance restart"),
        ))
    }

    pub(crate) fn recommend_sessions(param: &OracleParameterInfo, _instance_info: &OracleInstanceInfo) -> Option<ParameterRecommendation> {
        let value = param.value_numeric?;
        if value >= 200.0 {
            return None;
        }

        Some(Self::recommendation(
            "500",
            "SESSIONS value may be insufficient for concurrent users",
            "Support for more concurrent sessions",
            "Low risk - prevents session limit errors",
            Some("Should be 1.1 * PROCESSES + 5"),
        ))
    }

    pub(crate) fn recommend_undo_retention(param: &OracleParameterInfo) -> Option<ParameterRecommendation> {
        let value = param.value_numeric?;
        if value >= 900.0 {
            return None;
        }

        Some(Self::recommendation(
            "3600",
            "UNDO_RETENTION is too low - may cause snapshot too old errors",
            "Reduced ORA-01555 errors and better query consistency",
            "Low risk - requires more undo space",
            Some("Monitor undo tablespace usage"),
        ))
    }
}
