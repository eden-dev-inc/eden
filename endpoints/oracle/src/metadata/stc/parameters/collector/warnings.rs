use super::*;
impl OracleParametersCollection {
    pub(crate) fn generate_warnings(parameters: &[OracleParameterInfo]) -> Vec<ConfigurationWarning> {
        let mut warnings = Vec::new();

        for param in parameters {
            // Check for deprecated parameters
            if param.is_deprecated && param.value.is_some() {
                warnings.push(ConfigurationWarning {
                    parameter_name: param.name.clone(),
                    severity: WarningSeverity::Warning,
                    message: "Parameter is deprecated".to_string(),
                    recommended_action: "Consider removing or replacing with modern equivalent".to_string(),
                    impact: "May not be supported in future versions".to_string(),
                });
            }

            // Check for high-risk configurations
            if matches!(param.risk_level, RiskLevel::Critical) {
                warnings.push(ConfigurationWarning {
                    parameter_name: param.name.clone(),
                    severity: WarningSeverity::Critical,
                    message: "Critical risk configuration detected".to_string(),
                    recommended_action: "Review and adjust parameter value".to_string(),
                    impact: "May cause performance or stability issues".to_string(),
                });
            }

            // Specific parameter checks
            Self::add_specific_parameter_warnings(&mut warnings, param);
        }

        warnings
    }

    pub(crate) fn add_specific_parameter_warnings(warnings: &mut Vec<ConfigurationWarning>, param: &OracleParameterInfo) {
        match param.name.to_uppercase().as_str() {
            "PROCESSES" => {
                if let Some(value) = param.value_numeric
                    && value < 100.0
                {
                    warnings.push(ConfigurationWarning {
                        parameter_name: param.name.clone(),
                        severity: WarningSeverity::Warning,
                        message: "PROCESSES value is very low".to_string(),
                        recommended_action: "Increase to at least 150 for modern applications".to_string(),
                        impact: "May prevent new connections during peak usage".to_string(),
                    });
                }
            }
            "MEMORY_TARGET" => {
                if param.value.is_none() {
                    warnings.push(ConfigurationWarning {
                        parameter_name: param.name.clone(),
                        severity: WarningSeverity::Info,
                        message: "Automatic Memory Management not enabled".to_string(),
                        recommended_action: "Consider enabling AMM for simplified memory management".to_string(),
                        impact: "Manual memory component sizing required".to_string(),
                    });
                }
            }
            "AUDIT_TRAIL" => {
                if let Some(value) = &param.value
                    && value.to_uppercase() == "NONE"
                {
                    warnings.push(ConfigurationWarning {
                        parameter_name: param.name.clone(),
                        severity: WarningSeverity::Warning,
                        message: "Database auditing is disabled".to_string(),
                        recommended_action: "Enable auditing for security compliance".to_string(),
                        impact: "No audit trail for security events".to_string(),
                    });
                }
            }
            _ => {}
        }
    }
}
