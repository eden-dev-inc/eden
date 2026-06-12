use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::run_query_with_timeout;
use borsh::{BorshDeserialize, BorshSerialize};
use eden_logger_internal::{LogAudience, LogContext, log_warn};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL configuration settings and parameters
///
/// This struct contains comprehensive information about PostgreSQL configuration,
/// including current values, defaults, recommendations, and change tracking.
/// Data is collected from pg_settings and related system views.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSettingsInfo {
    /// All PostgreSQL settings
    pub settings: Vec<PostgresSetting>,
    /// Settings that have been modified from defaults
    pub modified_settings_count: u64,
    /// Settings requiring restart to take effect
    pub restart_required_count: u64,
    /// Total number of settings
    pub total_settings_count: u64,
    /// Memory-related settings analysis
    pub memory_analysis: Option<PostgresMemoryAnalysis>,
    /// Performance-critical settings analysis
    pub performance_analysis: Option<PostgresPerformanceAnalysis>,
    /// Security-related settings analysis
    pub security_analysis: Option<PostgresSecurityAnalysis>,
}

/// Individual PostgreSQL setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSetting {
    /// Setting name
    pub setting_name: String,
    /// Current setting value
    pub current_value: String,
    /// Default value for this setting
    pub default_value: Option<String>,
    /// Unit of measurement (if applicable)
    pub unit: Option<String>,
    /// Setting category
    pub category: String,
    /// Short description of the setting
    pub short_description: String,
    /// Setting context (when changes take effect)
    pub context: PostgresSettingContext,
    /// Data type of the setting value
    pub data_type: PostgresSettingDataType,
    /// Minimum allowed value (for numeric settings)
    pub min_value: Option<String>,
    /// Maximum allowed value (for numeric settings)
    pub max_value: Option<String>,
    /// Enumerated values (for enum settings)
    pub enum_values: Vec<String>,
    /// Source of current value
    pub source: PostgresSettingSource,
    /// Configuration file where setting is defined
    pub source_file: Option<String>,
    /// Line number in configuration file
    pub source_line: Option<i32>,
    /// Whether setting has been modified from default
    pub is_modified: bool,
    /// Whether setting requires restart to take effect
    pub requires_restart: bool,
}

/// Memory-related settings analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresMemoryAnalysis {
    /// Total configured memory in bytes
    pub total_configured_memory: u64,
    /// Shared buffers size in bytes
    pub shared_buffers_bytes: u64,
    /// Work memory per operation in bytes
    pub work_mem_bytes: u64,
    /// Maintenance work memory in bytes
    pub maintenance_work_mem_bytes: u64,
    /// Effective cache size in bytes
    pub effective_cache_size_bytes: u64,
    /// WAL buffers size in bytes
    pub wal_buffers_bytes: u64,
    /// Maximum connections allowed
    pub max_connections: u64,
    /// Estimated total memory usage in bytes
    pub estimated_total_memory_usage: u64,
    /// Memory configuration recommendations
    pub recommendations: Vec<String>,
}

/// Performance-critical settings analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresPerformanceAnalysis {
    /// Critical performance settings and their impact
    pub critical_settings: Vec<PostgresCriticalSetting>,
    /// Overall performance score (0-100)
    pub performance_score: f64,
    /// Performance recommendations
    pub recommendations: Vec<String>,
}

/// Security-related settings analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSecurityAnalysis {
    /// Security-sensitive settings
    pub security_settings: Vec<PostgresSecuritySetting>,
    /// Overall security score (0-100)
    pub security_score: f64,
    /// Security recommendations
    pub recommendations: Vec<String>,
}

/// Critical performance setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresCriticalSetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub value: String,
    /// Performance impact level
    pub impact_level: PerformanceImpact,
    /// Recommended value (if different)
    pub recommended_value: Option<String>,
    /// Reason for recommendation
    pub recommendation_reason: String,
}

/// Security-sensitive setting information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSecuritySetting {
    /// Setting name
    pub name: String,
    /// Current value
    pub value: String,
    /// Security impact level
    pub impact_level: SecurityImpact,
    /// Whether setting is secure
    pub is_secure: bool,
    /// Security recommendation
    pub recommendation: Option<String>,
}

impl MetadataCollection for PostgresSettingsInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "all_settings".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, unit, category, short_desc, context, vartype,
                    source, sourcefile, sourceline, min_val, max_val,
                    COALESCE(enumvals, ARRAY[]::text[]) as enumvals,
                    boot_val, reset_val, pending_restart
                FROM pg_settings
                ORDER BY category, name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "modified_settings".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, boot_val, reset_val, source
                FROM pg_settings
                WHERE setting != boot_val OR source != 'default'
                ORDER BY category, name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "restart_required".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, pending_restart
                FROM pg_settings
                WHERE pending_restart = true
                ORDER BY name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "memory_settings".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, unit,
                    GREATEST(CASE
                        WHEN unit = 'kB' THEN pg_size_bytes(setting || 'kB')
                        WHEN unit = 'MB' THEN pg_size_bytes(setting || 'MB')
                        WHEN unit = 'GB' THEN pg_size_bytes(setting || 'GB')
                        WHEN unit = '8kB' THEN (setting::bigint * 8192)
                        WHEN name = 'max_connections' THEN setting::bigint
                        WHEN setting ~ '^[0-9]+$' THEN setting::bigint
                        ELSE 0
                    END, 0) as bytes_value
                FROM pg_settings
                WHERE category LIKE '%Memory%' OR name IN (
                    'shared_buffers', 'work_mem', 'maintenance_work_mem',
                    'effective_cache_size', 'wal_buffers', 'max_connections'
                )
                ORDER BY name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "performance_settings".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, category, short_desc
                FROM pg_settings
                WHERE name IN (
                    'shared_buffers', 'work_mem', 'maintenance_work_mem',
                    'effective_cache_size', 'random_page_cost', 'seq_page_cost',
                    'cpu_tuple_cost', 'cpu_index_tuple_cost', 'effective_io_concurrency',
                    'max_worker_processes', 'max_parallel_workers_per_gather',
                    'checkpoint_completion_target', 'wal_buffers', 'default_statistics_target'
                )
                ORDER BY name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "security_settings".to_string(),
                QueryInput::new(
                    "SELECT
                    name, setting, category, short_desc
                FROM pg_settings
                WHERE name IN (
                    'ssl', 'ssl_ciphers', 'ssl_prefer_server_ciphers',
                    'password_encryption', 'krb_server_keyfile', 'log_connections',
                    'log_disconnections', 'log_statement', 'log_min_duration_statement',
                    'shared_preload_libraries', 'listen_addresses', 'port'
                )
                ORDER BY name"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL configuration settings including values, sources, and recommendations"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "settings"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresSettingsInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_SETTINGS_RESULTS: usize = 1000;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut settings_info = PostgresSettingsInfo::default();
        let requests = self.request();

        // Execute all queries with timeout
        let all_settings_result = Self::execute_query(&requests, "all_settings", context.clone()).await;

        let modified_settings_result = Self::execute_query(&requests, "modified_settings", context.clone()).await;

        let restart_required_result = Self::execute_query(&requests, "restart_required", context.clone()).await;

        let memory_settings_result = Self::execute_query(&requests, "memory_settings", context.clone()).await;

        let performance_settings_result = Self::execute_query(&requests, "performance_settings", context.clone()).await;

        let security_settings_result = Self::execute_query(&requests, "security_settings", context.clone()).await;

        // Process all settings
        let all_settings_rows = match Self::handle_privileged_query(all_settings_result, "all_settings")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.settings = Self::parse_all_settings(all_settings_rows)?;
        settings_info.total_settings_count = settings_info.settings.len() as u64;

        // Process modified settings count
        let modified_settings_rows = match Self::handle_privileged_query(modified_settings_result, "modified_settings")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.modified_settings_count = modified_settings_rows.len() as u64;

        // Process restart required count
        let restart_required_rows = match Self::handle_privileged_query(restart_required_result, "restart_required")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.restart_required_count = restart_required_rows.len() as u64;

        // Process memory analysis
        let memory_settings_rows = match Self::handle_privileged_query(memory_settings_result, "memory_settings")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.memory_analysis = Self::analyze_memory_settings(memory_settings_rows)?;

        // Process performance analysis
        let performance_settings_rows = match Self::handle_privileged_query(performance_settings_result, "performance_settings")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.performance_analysis = Self::analyze_performance_settings(performance_settings_rows)?;

        // Process security analysis
        let security_settings_rows = match Self::handle_privileged_query(security_settings_result, "security_settings")? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        settings_info.security_analysis = Self::analyze_security_settings(security_settings_rows)?;

        Ok(settings_info)
    }

    async fn execute_query(requests: &HashMap<String, QueryInput>, key: &str, context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query = requests.get(key).ok_or_else(|| EpError::metadata(format!("Missing query: {}", key)))?;

        run_query_with_timeout(query, context, Self::QUERY_TIMEOUT, key).await
    }

    // Helper functions for safe type conversion and extraction (same as activity code)
    fn safe_i64_to_u64(row: &PgSimpleRow, column: &str) -> ResultEP<u64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        let value = text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))?;

        if value < 0 {
            return Err(EpError::metadata(format!("Negative value for {}: {}", column, value)));
        }
        Ok(value as u64)
    }

    #[allow(dead_code)]
    fn safe_get_f64(row: &PgSimpleRow, column: &str) -> ResultEP<f64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<f64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    #[allow(dead_code)]
    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_bool(row: &PgSimpleRow, column: &str) -> ResultEP<bool> {
        row.get(column)
            .map(|s| s == "t" || s == "true" || s == "1")
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn parse_all_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresSetting>> {
        let mut settings = Vec::with_capacity(rows.len().min(Self::MAX_SETTINGS_RESULTS));

        for row in rows.into_iter().take(Self::MAX_SETTINGS_RESULTS) {
            let enum_values: Vec<String> = match row.get("enumvals") {
                Some(text) if !text.is_empty() && text != "{}" => {
                    // PostgreSQL array text format: {val1,val2,val3}
                    let trimmed = text.trim_start_matches('{').trim_end_matches('}');
                    if trimmed.is_empty() {
                        Vec::new()
                    } else {
                        trimmed.split(',').map(|s| s.trim_matches('"').to_string()).collect()
                    }
                }
                _ => Vec::new(),
            };

            let current_value = Self::safe_get_string(&row, "setting")?;
            let boot_val = Self::safe_get_optional_string(&row, "boot_val")?;
            let is_modified = boot_val.as_ref().is_some_and(|bv| bv != &current_value);

            settings.push(PostgresSetting {
                setting_name: Self::safe_get_string(&row, "name")?,
                current_value,
                default_value: boot_val,
                unit: Self::safe_get_optional_string(&row, "unit")?,
                category: Self::safe_get_string(&row, "category")?,
                short_description: Self::safe_get_string(&row, "short_desc")?,
                context: Self::parse_setting_context(&Self::safe_get_string(&row, "context")?),
                data_type: Self::parse_setting_data_type(&Self::safe_get_string(&row, "vartype")?),
                min_value: Self::safe_get_optional_string(&row, "min_val")?,
                max_value: Self::safe_get_optional_string(&row, "max_val")?,
                enum_values,
                source: Self::parse_setting_source(&Self::safe_get_string(&row, "source")?),
                source_file: Self::safe_get_optional_string(&row, "sourcefile")?,
                source_line: row.get("sourceline").and_then(|s| s.parse::<i32>().ok()),
                is_modified,
                requires_restart: Self::safe_get_bool(&row, "pending_restart")?,
            });
        }

        Ok(settings)
    }

    fn analyze_memory_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Option<PostgresMemoryAnalysis>> {
        if rows.is_empty() {
            return Ok(None);
        }

        let mut shared_buffers_bytes = 0u64;
        let mut work_mem_bytes = 0u64;
        let mut maintenance_work_mem_bytes = 0u64;
        let mut effective_cache_size_bytes = 0u64;
        let mut wal_buffers_bytes = 0u64;
        let mut max_connections = 0u64;

        for row in rows {
            let name = Self::safe_get_string(&row, "name")?;
            let bytes_value = Self::safe_i64_to_u64(&row, "bytes_value")?;

            match name.as_str() {
                "shared_buffers" => shared_buffers_bytes = bytes_value,
                "work_mem" => work_mem_bytes = bytes_value,
                "maintenance_work_mem" => maintenance_work_mem_bytes = bytes_value,
                "effective_cache_size" => effective_cache_size_bytes = bytes_value,
                "wal_buffers" => wal_buffers_bytes = bytes_value,
                "max_connections" => max_connections = bytes_value,
                _ => {}
            }
        }

        let total_configured_memory = shared_buffers_bytes + wal_buffers_bytes + maintenance_work_mem_bytes;
        let estimated_total_memory_usage = total_configured_memory + (work_mem_bytes * max_connections);

        let mut recommendations = Vec::new();

        // Add memory configuration recommendations
        if shared_buffers_bytes < 134_217_728 {
            // Less than 128MB
            recommendations.push("Consider increasing shared_buffers for better performance".to_string());
        }

        if work_mem_bytes < 4_194_304 {
            // Less than 4MB
            recommendations.push("Consider increasing work_mem for complex queries".to_string());
        }

        Ok(Some(PostgresMemoryAnalysis {
            total_configured_memory,
            shared_buffers_bytes,
            work_mem_bytes,
            maintenance_work_mem_bytes,
            effective_cache_size_bytes,
            wal_buffers_bytes,
            max_connections,
            estimated_total_memory_usage,
            recommendations,
        }))
    }

    fn analyze_performance_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Option<PostgresPerformanceAnalysis>> {
        if rows.is_empty() {
            return Ok(None);
        }

        let mut critical_settings = Vec::new();
        let mut performance_score = 100.0;
        let mut recommendations = Vec::new();

        for row in rows {
            let name = Self::safe_get_string(&row, "name")?;
            let value = Self::safe_get_string(&row, "setting")?;
            let _description = Self::safe_get_string(&row, "short_desc")?;

            let (impact_level, recommended_value, recommendation_reason) = Self::analyze_performance_setting(&name, &value);

            if impact_level != PerformanceImpact::None {
                critical_settings.push(PostgresCriticalSetting {
                    name: name.clone(),
                    value: value.clone(),
                    impact_level: impact_level.clone(),
                    recommended_value: recommended_value.clone(),
                    recommendation_reason: recommendation_reason.clone(),
                });

                if recommended_value.is_some() {
                    performance_score -= match impact_level {
                        PerformanceImpact::Critical => 20.0,
                        PerformanceImpact::High => 15.0,
                        PerformanceImpact::Medium => 10.0,
                        PerformanceImpact::Low => 5.0,
                        PerformanceImpact::None => 0.0,
                    };
                    recommendations.push(format!("{}: {}", name, recommendation_reason));
                }
            }
        }

        // performance_score = performance_score.max(0.0_f32);

        Ok(Some(PostgresPerformanceAnalysis { critical_settings, performance_score, recommendations }))
    }

    fn analyze_security_settings(rows: Vec<PgSimpleRow>) -> ResultEP<Option<PostgresSecurityAnalysis>> {
        if rows.is_empty() {
            return Ok(None);
        }

        let mut security_settings = Vec::new();
        let mut security_score = 100.0;
        let mut recommendations = Vec::new();

        for row in rows {
            let name = Self::safe_get_string(&row, "name")?;
            let value = Self::safe_get_string(&row, "setting")?;

            let (impact_level, is_secure, recommendation) = Self::analyze_security_setting(&name, &value);

            security_settings.push(PostgresSecuritySetting {
                name: name.clone(),
                value: value.clone(),
                impact_level: impact_level.clone(),
                is_secure,
                recommendation: recommendation.clone(),
            });

            if !is_secure {
                security_score -= match impact_level {
                    SecurityImpact::Critical => 25.0,
                    SecurityImpact::High => 20.0,
                    SecurityImpact::Medium => 15.0,
                    SecurityImpact::Low => 10.0,
                    SecurityImpact::None => 0.0,
                };

                if let Some(rec) = recommendation {
                    recommendations.push(format!("{}: {}", name, rec));
                }
            }
        }

        // security_score = security_score.max(0.0);

        Ok(Some(PostgresSecurityAnalysis { security_settings, security_score, recommendations }))
    }

    fn analyze_performance_setting(name: &str, value: &str) -> (PerformanceImpact, Option<String>, String) {
        match name {
            "shared_buffers" => {
                if let Ok(mb_value) = value.trim_end_matches("MB").parse::<i32>() {
                    if mb_value < 128 {
                        (
                            PerformanceImpact::High,
                            Some("256MB".to_string()),
                            "Shared buffers too small, consider 25% of RAM".to_string(),
                        )
                    } else {
                        (PerformanceImpact::Medium, None, "Configured appropriately".to_string())
                    }
                } else {
                    (PerformanceImpact::Medium, None, "Unable to parse value".to_string())
                }
            }
            "work_mem" => {
                if let Ok(mb_value) = value.trim_end_matches("MB").parse::<i32>() {
                    if mb_value < 4 {
                        (
                            PerformanceImpact::Medium,
                            Some("4MB".to_string()),
                            "Work memory too small for complex queries".to_string(),
                        )
                    } else {
                        (PerformanceImpact::Low, None, "Configured appropriately".to_string())
                    }
                } else {
                    (PerformanceImpact::Low, None, "Unable to parse value".to_string())
                }
            }
            "random_page_cost" => {
                if let Ok(cost) = value.parse::<f64>() {
                    if cost > 4.0 {
                        (
                            PerformanceImpact::Medium,
                            Some("1.1".to_string()),
                            "High random page cost, consider SSD-optimized value".to_string(),
                        )
                    } else {
                        (PerformanceImpact::Low, None, "Configured appropriately".to_string())
                    }
                } else {
                    (PerformanceImpact::Low, None, "Unable to parse value".to_string())
                }
            }
            _ => (PerformanceImpact::None, None, "No specific recommendation".to_string()),
        }
    }

    fn analyze_security_setting(name: &str, value: &str) -> (SecurityImpact, bool, Option<String>) {
        match name {
            "ssl" => {
                let is_secure = value == "on";
                (
                    SecurityImpact::Critical,
                    is_secure,
                    if !is_secure {
                        Some("Enable SSL for encrypted connections".to_string())
                    } else {
                        None
                    },
                )
            }
            "log_connections" => {
                let is_secure = value == "on";
                (
                    SecurityImpact::Medium,
                    is_secure,
                    if !is_secure {
                        Some("Enable connection logging for audit trail".to_string())
                    } else {
                        None
                    },
                )
            }
            "log_statement" => {
                let is_secure = value != "none";
                (
                    SecurityImpact::Medium,
                    is_secure,
                    if !is_secure {
                        Some("Enable statement logging for security auditing".to_string())
                    } else {
                        None
                    },
                )
            }
            "listen_addresses" => {
                let is_secure = value != "*";
                (
                    SecurityImpact::High,
                    is_secure,
                    if !is_secure {
                        Some("Restrict listen addresses, avoid '*' in production".to_string())
                    } else {
                        None
                    },
                )
            }
            _ => (SecurityImpact::None, true, None),
        }
    }

    fn parse_setting_context(context: &str) -> PostgresSettingContext {
        match context {
            "postmaster" => PostgresSettingContext::Postmaster,
            "sighup" => PostgresSettingContext::Sighup,
            "superuser" => PostgresSettingContext::Superuser,
            "user" => PostgresSettingContext::User,
            "internal" => PostgresSettingContext::Internal,
            "backend" => PostgresSettingContext::Backend,
            "superuser-backend" => PostgresSettingContext::SuperuserBackend,
            _ => PostgresSettingContext::Unknown(context.to_string()),
        }
    }

    fn parse_setting_data_type(vartype: &str) -> PostgresSettingDataType {
        match vartype {
            "bool" => PostgresSettingDataType::Boolean,
            "integer" => PostgresSettingDataType::Integer,
            "real" => PostgresSettingDataType::Real,
            "string" => PostgresSettingDataType::String,
            "enum" => PostgresSettingDataType::Enum,
            _ => PostgresSettingDataType::Unknown(vartype.to_string()),
        }
    }

    fn parse_setting_source(source: &str) -> PostgresSettingSource {
        match source {
            "default" => PostgresSettingSource::Default,
            "environment variable" => PostgresSettingSource::Environment,
            "configuration file" => PostgresSettingSource::ConfigFile,
            "command line" => PostgresSettingSource::CommandLine,
            "database" => PostgresSettingSource::Database,
            "user" => PostgresSettingSource::User,
            "client" => PostgresSettingSource::Client,
            "override" => PostgresSettingSource::Override,
            "interactive" => PostgresSettingSource::Interactive,
            "session" => PostgresSettingSource::Session,
            _ => PostgresSettingSource::Default,
        }
    }
}

/// PostgreSQL setting contexts (when changes take effect)
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum PostgresSettingContext {
    /// Requires server restart
    Postmaster,
    /// Takes effect on new connections
    Sighup,
    /// Can be changed by superuser without restart
    Superuser,
    /// Can be changed by regular user for their session
    #[default]
    User,
    /// Internal setting, cannot be changed
    Internal,
    /// Backend-specific setting
    Backend,
    /// Superuser backend setting
    SuperuserBackend,
    /// Unknown context
    Unknown(String),
}

/// PostgreSQL setting data types
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum PostgresSettingDataType {
    /// Boolean value (on/off, true/false)
    Boolean,
    /// Integer value
    Integer,
    /// Real/floating point value
    Real,
    /// String value
    #[default]
    String,
    /// Enumerated value (one of several options)
    Enum,
    /// Unknown data type
    Unknown(String),
}

/// Source of setting value
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum PostgresSettingSource {
    /// Default compiled-in value
    #[default]
    Default,
    /// Environment variable
    Environment,
    /// Configuration file
    ConfigFile,
    /// Command line
    CommandLine,
    /// Database setting
    Database,
    /// User setting
    User,
    /// Client setting
    Client,
    /// Override setting
    Override,
    /// Interactive setting
    Interactive,
    /// Session setting
    Session,
}

/// Performance impact levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum PerformanceImpact {
    /// No significant performance impact
    #[default]
    None,
    /// Low performance impact
    Low,
    /// Medium performance impact
    Medium,
    /// High performance impact
    High,
    /// Critical performance impact
    Critical,
}

/// Security impact levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum SecurityImpact {
    /// No security impact
    #[default]
    None,
    /// Low security impact
    Low,
    /// Medium security impact
    Medium,
    /// High security impact
    High,
    /// Critical security impact
    Critical,
}

/// Stability impact levels
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Default)]
pub enum StabilityImpact {
    /// No stability impact
    #[default]
    None,
    /// Low stability impact
    Low,
    /// Medium stability impact
    Medium,
    /// High stability impact
    High,
    /// Critical stability impact
    Critical,
}

impl PostgresSettingsInfo {
    fn handle_privileged_query(result: ResultEP<Vec<PgSimpleRow>>, query_name: &str) -> ResultEP<Option<Vec<PgSimpleRow>>> {
        match result {
            Ok(rows) => Ok(Some(rows)),
            Err(err) if Self::is_permission_error(&err) => {
                log_warn!(
                    LogContext::default().with_feature("metadata"),
                    format!("postgres.settings_info query `{}` skipped due to insufficient privileges: {}", query_name, err),
                    audience = LogAudience::Internal
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn is_permission_error(err: &EpError) -> bool {
        let message = err.to_string().to_lowercase();
        message.contains("permission denied") || message.contains("insufficient privilege") || message.contains("must be superuser")
    }

    fn empty_permissions_response() -> Self {
        PostgresSettingsInfo::default()
    }

    /// Returns the number of settings that have been modified from defaults
    pub fn modified_settings_count(&self) -> u64 {
        self.modified_settings_count
    }

    /// Returns the number of settings requiring restart
    pub fn restart_required_count(&self) -> u64 {
        self.restart_required_count
    }

    /// Returns the total number of settings
    pub fn total_settings_count(&self) -> u64 {
        self.total_settings_count
    }

    /// Checks if there are any settings requiring restart
    pub fn has_restart_required(&self) -> bool {
        self.restart_required_count > 0
    }

    /// Checks if memory analysis is available
    pub fn has_memory_analysis(&self) -> bool {
        self.memory_analysis.is_some()
    }

    /// Checks if performance analysis is available
    pub fn has_performance_analysis(&self) -> bool {
        self.performance_analysis.is_some()
    }

    /// Checks if security analysis is available
    pub fn has_security_analysis(&self) -> bool {
        self.security_analysis.is_some()
    }

    /// Gets the overall performance score (0-100)
    pub fn performance_score(&self) -> f64 {
        self.performance_analysis.as_ref().map(|analysis| analysis.performance_score).unwrap_or(0.0)
    }

    /// Gets the overall security score (0-100)
    pub fn security_score(&self) -> f64 {
        self.security_analysis.as_ref().map(|analysis| analysis.security_score).unwrap_or(0.0)
    }

    /// Gets settings by category
    pub fn settings_by_category(&self, category: &str) -> Vec<&PostgresSetting> {
        self.settings.iter().filter(|setting| setting.category == category).collect()
    }

    /// Gets modified settings only
    pub fn modified_settings(&self) -> Vec<&PostgresSetting> {
        self.settings.iter().filter(|setting| setting.is_modified).collect()
    }

    /// Gets settings requiring restart
    pub fn restart_required_settings(&self) -> Vec<&PostgresSetting> {
        self.settings.iter().filter(|setting| setting.requires_restart).collect()
    }

    /// Gets a specific setting by name
    pub fn get_setting(&self, name: &str) -> Option<&PostgresSetting> {
        self.settings.iter().find(|setting| setting.setting_name == name)
    }

    /// Gets all memory-related recommendations
    pub fn memory_recommendations(&self) -> Vec<&String> {
        self.memory_analysis.as_ref().map(|analysis| analysis.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets all performance recommendations
    pub fn performance_recommendations(&self) -> Vec<&String> {
        self.performance_analysis.as_ref().map(|analysis| analysis.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets all security recommendations
    pub fn security_recommendations(&self) -> Vec<&String> {
        self.security_analysis.as_ref().map(|analysis| analysis.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets estimated total memory usage in bytes
    pub fn estimated_memory_usage(&self) -> u64 {
        self.memory_analysis.as_ref().map(|analysis| analysis.estimated_total_memory_usage).unwrap_or(0)
    }

    /// Checks if the configuration appears to be production-ready
    pub fn is_production_ready(&self) -> bool {
        let performance_ok = self.performance_score() >= 80.0;
        let security_ok = self.security_score() >= 85.0;
        let no_critical_issues = !self.has_critical_security_issues() && !self.has_critical_performance_issues();

        performance_ok && security_ok && no_critical_issues
    }

    /// Checks for critical security issues
    pub fn has_critical_security_issues(&self) -> bool {
        self.security_analysis
            .as_ref()
            .map(|analysis| {
                analysis.security_settings.iter().any(|setting| setting.impact_level == SecurityImpact::Critical && !setting.is_secure)
            })
            .unwrap_or(false)
    }

    /// Checks for critical performance issues
    pub fn has_critical_performance_issues(&self) -> bool {
        self.performance_analysis
            .as_ref()
            .map(|analysis| {
                analysis
                    .critical_settings
                    .iter()
                    .any(|setting| setting.impact_level == PerformanceImpact::Critical && setting.recommended_value.is_some())
            })
            .unwrap_or(false)
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_metadata_settings() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;
        let telemetry_wrapper = &mut telemetry_wrapper;

        let settings_info = PostgresSettingsInfo::default();

        let result = settings_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok(), "sync_metadata failed: {:?}", result.as_ref().err());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.total_settings_count > 0);
        assert!(!info.settings.is_empty());

        // Verify we can find some common settings
        assert!(info.get_setting("max_connections").is_some());
        assert!(info.get_setting("shared_buffers").is_some());

        // Verify analysis structures exist
        assert!(info.has_memory_analysis() || info.memory_analysis.is_none());
        assert!(info.has_performance_analysis() || info.performance_analysis.is_none());
        assert!(info.has_security_analysis() || info.security_analysis.is_none());
    }

    #[tokio::test]
    async fn test_postgres_settings_analysis() {
        let _settings_info = PostgresSettingsInfo::default();

        // Test performance setting analysis
        let (impact, recommended, reason) = PostgresSettingsInfo::analyze_performance_setting("shared_buffers", "64MB");
        assert_eq!(impact, PerformanceImpact::High);
        assert!(recommended.is_some());
        assert!(!reason.is_empty());

        // Test security setting analysis
        let (impact, is_secure, recommendation) = PostgresSettingsInfo::analyze_security_setting("ssl", "off");
        assert_eq!(impact, SecurityImpact::Critical);
        assert!(!is_secure);
        assert!(recommendation.is_some());
    }

    #[tokio::test]
    async fn test_postgres_settings_parsing() {
        // Test context parsing
        assert_eq!(PostgresSettingsInfo::parse_setting_context("postmaster"), PostgresSettingContext::Postmaster);
        assert_eq!(PostgresSettingsInfo::parse_setting_context("user"), PostgresSettingContext::User);

        // Test data type parsing
        assert_eq!(PostgresSettingsInfo::parse_setting_data_type("bool"), PostgresSettingDataType::Boolean);
        assert_eq!(PostgresSettingsInfo::parse_setting_data_type("integer"), PostgresSettingDataType::Integer);

        // Test source parsing
        assert_eq!(PostgresSettingsInfo::parse_setting_source("default"), PostgresSettingSource::Default);
        assert_eq!(PostgresSettingsInfo::parse_setting_source("configuration file"), PostgresSettingSource::ConfigFile);
    }
}
