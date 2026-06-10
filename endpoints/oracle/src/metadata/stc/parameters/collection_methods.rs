use super::*;
impl OracleParametersCollection {
    /// Gets parameters by category
    pub fn get_parameters_by_category(&self, category: &ParameterCategory) -> Vec<&OracleParameterInfo> {
        self.parameters.iter().filter(|p| std::mem::discriminant(&p.category) == std::mem::discriminant(category)).collect()
    }

    /// Gets parameters that need immediate attention
    pub fn get_critical_parameters(&self) -> Vec<&OracleParameterInfo> {
        self.parameters
            .iter()
            .filter(|p| matches!(p.risk_level, RiskLevel::Critical) || matches!(p.status(), ParameterStatus::Critical))
            .collect()
    }

    /// Gets deprecated parameters that are still in use
    pub fn get_deprecated_parameters(&self) -> Vec<&OracleParameterInfo> {
        self.parameters.iter().filter(|p| p.is_deprecated && p.value.is_some()).collect()
    }

    /// Gets parameters with optimization opportunities
    pub fn get_optimization_candidates(&self) -> Vec<&OracleParameterInfo> {
        self.parameters.iter().filter(|p| !p.is_optimal || p.recommendation.is_some()).collect()
    }

    /// Gets memory-related parameters summary
    pub fn get_memory_summary(&self) -> MemorySummary {
        let memory_params: Vec<&OracleParameterInfo> =
            self.parameters.iter().filter(|p| matches!(p.category, ParameterCategory::Memory)).collect();

        let memory_target = memory_params.iter().find(|p| p.name.to_uppercase() == "MEMORY_TARGET").and_then(|p| p.value_bytes);

        let sga_target = memory_params.iter().find(|p| p.name.to_uppercase() == "SGA_TARGET").and_then(|p| p.value_bytes);

        let pga_target = memory_params.iter().find(|p| p.name.to_uppercase() == "PGA_AGGREGATE_TARGET").and_then(|p| p.value_bytes);

        MemorySummary {
            memory_target,
            sga_target,
            pga_target,
            total_allocated: memory_target.or_else(|| match (sga_target, pga_target) {
                (Some(sga), Some(pga)) => Some(sga + pga),
                (Some(sga), None) => Some(sga),
                (None, Some(pga)) => Some(pga),
                (None, None) => None,
            }),
            amm_enabled: memory_target.is_some(),
            asmm_enabled: sga_target.is_some(),
        }
    }

    /// Gets security configuration summary
    pub fn get_security_summary(&self) -> SecuritySummary {
        let security_params: Vec<&OracleParameterInfo> = self
            .parameters
            .iter()
            .filter(|p| {
                matches!(p.category, ParameterCategory::Security)
                    || matches!(p.security_impact, SecurityImpact::Medium | SecurityImpact::High | SecurityImpact::Critical)
            })
            .collect();

        let audit_enabled = security_params
            .iter()
            .find(|p| p.name.to_uppercase() == "AUDIT_TRAIL")
            .map(|p| p.value.as_ref().is_some_and(|v| v.to_uppercase() != "NONE"))
            .unwrap_or(false);

        let remote_auth = security_params
            .iter()
            .find(|p| p.name.to_uppercase() == "REMOTE_LOGIN_PASSWORDFILE")
            .map(|p| p.value.as_ref().is_some_and(|v| v.to_uppercase() != "NONE"))
            .unwrap_or(false);

        SecuritySummary {
            audit_enabled,
            remote_authentication: remote_auth,
            high_risk_parameters: security_params.iter().filter(|p| matches!(p.risk_level, RiskLevel::High | RiskLevel::Critical)).count(),
            security_score: self.analysis_summary.security_score,
        }
    }

    /// Gets the top recommendations by priority
    pub fn get_top_recommendations(&self, limit: usize) -> Vec<&GlobalRecommendation> {
        let mut recommendations = self.recommendations.iter().collect::<Vec<_>>();
        recommendations.sort_by(|a, b| {
            // Sort by priority (Critical first)
            let priority_order = |p: &RecommendationPriority| match p {
                RecommendationPriority::Critical => 0,
                RecommendationPriority::High => 1,
                RecommendationPriority::Medium => 2,
                RecommendationPriority::Low => 3,
            };
            priority_order(&a.priority).cmp(&priority_order(&b.priority))
        });
        recommendations.into_iter().take(limit).collect()
    }

    /// Exports parameter configuration as SQL script
    pub fn export_as_sql(&self, include_comments: bool) -> String {
        let mut sql = String::new();

        if include_comments {
            sql.push_str("-- Oracle Database Parameter Configuration\n");
            sql.push_str(&format!("-- Generated on: {}\n", Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
            sql.push_str(&format!("-- Database: {}\n", self.instance_info.database_name));
            sql.push_str(&format!("-- Instance: {}\n", self.instance_info.instance_name));
            sql.push_str("-- \n");
            sql.push_str("-- WARNING: Review all parameters before applying!\n");
            sql.push_str("-- Some parameters require instance restart.\n\n");
        }

        for param in &self.parameters {
            if param.is_modified {
                if include_comments {
                    sql.push_str(&format!("-- Parameter: {}\n", param.name));
                    if let Some(desc) = &param.description {
                        sql.push_str(&format!("-- Description: {}\n", desc));
                    }
                    sql.push_str(&format!("-- Category: {}\n", param.category_description()));
                    sql.push_str(&format!("-- Requires Restart: {}\n", param.requires_restart()));
                }

                if let Some(value) = &param.value {
                    sql.push_str(&format!(
                        "ALTER SYSTEM SET {}='{}' SCOPE={};\n",
                        param.name,
                        value,
                        if param.requires_restart() { "SPFILE" } else { "BOTH" }
                    ));
                }

                if include_comments {
                    sql.push('\n');
                }
            }
        }

        sql
    }
}

impl MemorySummary {
    pub fn total_allocated_formatted(&self) -> String {
        self.total_allocated.map(OracleParameterInfo::format_bytes).unwrap_or_else(|| "Not Configured".to_string())
    }

    pub fn memory_management_type(&self) -> &'static str {
        if self.amm_enabled {
            "Automatic Memory Management (AMM)"
        } else if self.asmm_enabled {
            "Automatic Shared Memory Management (ASMM)"
        } else {
            "Manual Memory Management"
        }
    }
}
