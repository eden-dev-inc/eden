use super::*;
use function_name::named;
impl OracleParametersCollection {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut collection = OracleParametersCollection::default();
        let requests = self.request();

        collection.collection_timestamp = DateTimeWrapper::from(Utc::now());

        let parameters_rows = run_named_query(&requests, "parameters", context.clone(), Self::QUERY_TIMEOUT).await?;
        let mut parameters_map: HashMap<String, OracleParameterInfo> = HashMap::new();

        for row in parameters_rows {
            let name = row.get_string("name")?;
            let mut param = OracleParameterInfo {
                name: name.clone(),
                value: row.get_opt_string("value")?,
                default_value: row.get_opt_string("default_value")?,
                parameter_type: row.get_string("type")?,
                description: row.get_opt_string("description")?,
                is_modified: row.get_string("ismodified")? == "TRUE",
                is_deprecated: row.get_string("isdeprecated")? == "TRUE",
                is_basic: row.get_string("isbasic")? == "TRUE",
                category: Self::parse_parameter_category(&row.get_string("category")?),
                collection_timestamp: DateTimeWrapper::from(Utc::now()),
                ..Default::default()
            };

            if let Some(value_str) = &param.value {
                if let Ok(numeric_value) = value_str.parse::<f64>() {
                    param.value_numeric = Some(numeric_value);
                }

                if param.name.contains("size") || param.name.contains("target") || param.name.contains("limit") {
                    param.value_bytes = Self::parse_bytes_value(value_str);
                }
            }

            parameters_map.insert(name, param);
        }

        let details_rows = run_named_query(&requests, "parameter_details", context.clone(), Self::QUERY_TIMEOUT).await?;
        for row in details_rows {
            let name = row.get_string("name")?;
            if let Some(param) = parameters_map.get_mut(&name) {
                param.performance_impact = Self::parse_performance_impact(&row.get_string("performance_impact")?);
                param.security_impact = Self::parse_security_impact(&row.get_string("security_impact")?);
            }
        }

        let instance_rows = run_named_query(&requests, "instance_info", context.clone(), Self::QUERY_TIMEOUT).await?;
        if let Some(row) = instance_rows.first() {
            collection.instance_info = OracleInstanceInfo {
                instance_name: row.get_string("instance_name")?,
                database_name: row.get_string("database_name")?,
                version: row.get_string("version")?,
                startup_time: row.get_datetime("startup_time")?,
                status: row.get_string("status")?,
                database_role: row.get_string("database_role")?,
                cpu_count: row.get_u32("cpu_count")?,
                archive_log_mode: row.get_string("log_mode")?,
                flashback_on: row.get_string("flashback_on")? == "YES",
                force_logging: row.get_string("force_logging")? == "YES",
                sga_size: row.get_u64("sga_target_mb")? * 1024 * 1024,
                pga_size: row.get_u64("pga_target_mb")? * 1024 * 1024,
                total_memory: (row.get_u64("memory_target_mb")? * 1024 * 1024)
                    .max((row.get_u64("sga_target_mb")? + row.get_u64("pga_target_mb")?) * 1024 * 1024),
                ..Default::default()
            };
        }

        let charset_rows = run_named_query(&requests, "character_sets", context.clone(), Self::QUERY_TIMEOUT).await?;
        for row in charset_rows {
            let param_type = row.get_string("param_type")?;
            let value = row.get_string("value")?;

            match param_type.as_str() {
                "DATABASE_CHARACTER_SET" => collection.instance_info.character_set = value,
                "NATIONAL_CHARACTER_SET" => collection.instance_info.national_character_set = value,
                _ => {}
            }
        }

        for (_, param) in parameters_map.iter_mut() {
            Self::analyze_parameter(param, &collection.instance_info);
        }

        collection.parameters = parameters_map.into_values().collect();
        collection.parameters.sort_by(|a, b| a.name.cmp(&b.name));

        collection.analysis_summary = Self::generate_analysis_summary(&collection.parameters, &collection.instance_info);
        collection.warnings = Self::generate_warnings(&collection.parameters);
        collection.recommendations = Self::generate_global_recommendations(&collection.parameters, &collection.instance_info);

        Ok(collection)
    }

    fn analyze_parameter(param: &mut OracleParameterInfo, instance_info: &OracleInstanceInfo) {
        param.is_modifiable = Self::is_parameter_modifiable(&param.name);
        param.is_system_modifiable = Self::is_system_modifiable(&param.name);
        param.is_session_modifiable = Self::is_session_modifiable(&param.name);
        param.is_instance_modifiable = Self::is_instance_modifiable(&param.name);
        param.modify_scope = Self::get_modify_scope(&param.name);
        param.related_parameters = Self::get_related_parameters(&param.name);
        param.recommendation = Self::generate_parameter_recommendation(param, instance_info);
        param.risk_level = Self::assess_risk_level(param);
        param.is_optimal = Self::is_parameter_optimal(param, instance_info);
    }

    fn generate_analysis_summary(parameters: &[OracleParameterInfo], instance_info: &OracleInstanceInfo) -> ParameterAnalysisSummary {
        ParameterAnalysisSummary {
            total_parameters: parameters.len() as u64,
            modified_parameters: parameters.iter().filter(|p| p.is_modified).count() as u64,
            deprecated_parameters: parameters.iter().filter(|p| p.is_deprecated && p.value.is_some()).count() as u64,
            warning_parameters: parameters.iter().filter(|p| matches!(p.risk_level, RiskLevel::High | RiskLevel::Critical)).count() as u64,
            optimization_candidates: parameters.iter().filter(|p| !p.is_optimal).count() as u64,
            high_risk_parameters: parameters.iter().filter(|p| matches!(p.risk_level, RiskLevel::Critical)).count() as u64,
            security_parameters: parameters
                .iter()
                .filter(|p| matches!(p.security_impact, SecurityImpact::High | SecurityImpact::Critical))
                .count() as u64,
            performance_parameters: parameters
                .iter()
                .filter(|p| matches!(p.performance_impact, PerformanceImpact::High | PerformanceImpact::Critical))
                .count() as u64,
            health_score: Self::calculate_health_score(parameters),
            memory_efficiency_score: Self::calculate_memory_efficiency_score(parameters, instance_info),
            security_score: Self::calculate_security_score(parameters),
            performance_score: Self::calculate_performance_score(parameters),
        }
    }
}
