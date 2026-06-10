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

/// PostgreSQL extension information and metadata
///
/// Simplified struct containing essential information about installed extensions,
/// their versions, and basic health indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresExtensionInfo {
    /// List of installed extensions
    pub installed_extensions: Vec<PostgresExtension>,
    /// Total number of installed extensions
    pub total_extensions: u32,
    /// Number of extensions with available updates
    pub extensions_with_updates: u32,
    /// Number of untrusted extensions (security concern)
    pub untrusted_extensions: u32,
    /// Overall extension health score (0.0 to 100.0)
    pub health_score: f64,
    /// Detailed metrics collected only when issues are detected
    pub detailed_metrics: Option<PostgresExtensionDetailedMetrics>,
}

/// Individual extension information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresExtension {
    /// Extension name
    pub extension_name: String,
    /// Extension OID
    pub extension_oid: i32,
    /// Currently installed version
    pub installed_version: String,
    /// Default (latest available) version
    pub default_version: String,
    /// Whether extension can be updated
    pub can_update: bool,
    /// Extension schema
    pub extension_schema: String,
    /// Whether extension is relocatable to different schemas
    pub is_relocatable: bool,
    /// Whether extension is trusted (can be installed by non-superusers)
    pub is_trusted: bool,
    /// Extension description/comment
    pub description: String,
    /// Extension category (inferred from name)
    pub category: PostgresExtensionCategory,
    /// Security assessment
    pub security_level: PostgresExtensionSecurity,
}

/// Detailed extension metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresExtensionDetailedMetrics {
    /// Available extensions that could be installed (collected when health_score < 80)
    pub available_extensions: Option<Vec<PostgresAvailableExtension>>,
    /// Extension dependencies (collected when untrusted_extensions > 0)
    pub extension_dependencies: Option<Vec<PostgresExtensionDependency>>,
    /// Extension object counts (collected when many extensions installed)
    pub extension_objects: Option<HashMap<String, u32>>,
    /// Recommendations for extension management
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresExtensionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([(
            "extensions_summary".to_string(),
            QueryInput::new(
                "SELECT
                    e.oid::integer as oid, e.extname, e.extversion, e.extrelocatable,
                    n.nspname as schema_name,
                    COALESCE(obj_description(e.oid, 'pg_extension'), 'No description') as description,
                    COALESCE(ae.default_version, e.extversion) as default_version,
                    CASE WHEN ae.default_version IS NOT NULL AND ae.default_version != e.extversion
                         THEN true ELSE false END as can_update,
                    e.extname IN ('plpgsql', 'pg_stat_statements', 'pgcrypto', 'uuid-ossp', 'hstore', 'btree_gist', 'btree_gin') as trusted
                FROM pg_extension e
                JOIN pg_namespace n ON n.oid = e.extnamespace
                LEFT JOIN pg_available_extensions ae ON ae.name = e.extname
                ORDER BY e.extname"
                    .to_string(),
                Vec::new(),
            ),
        )])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL extension information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "extensions"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresExtensionInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const HEALTH_THRESHOLD: f64 = 80.0;
    const MANY_EXTENSIONS_THRESHOLD: u32 = 10;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut extension_info = PostgresExtensionInfo::default();
        let requests = self.request();

        // Execute core extensions query
        let extensions_rows = match Self::handle_privileged_query(
            run_query_with_timeout(
                requests.get("extensions_summary").ok_or_else(|| EpError::metadata("Missing query: extensions_summary".to_string()))?,
                context.clone(),
                Self::QUERY_TIMEOUT,
                "extensions_summary",
            )
            .await,
            "extensions_summary",
        )? {
            Some(rows) => rows,
            None => return Ok(Self::empty_permissions_response()),
        };
        if extensions_rows.is_empty() {
            extension_info.health_score = extension_info.calculate_health_score();
            return Ok(extension_info);
        }

        // Parse installed extensions
        extension_info.installed_extensions = Self::parse_extensions(extensions_rows)?;
        extension_info.total_extensions = extension_info.installed_extensions.len() as u32;

        // Calculate summary metrics
        extension_info.extensions_with_updates = extension_info.installed_extensions.iter().filter(|ext| ext.can_update).count() as u32;

        extension_info.untrusted_extensions = extension_info.installed_extensions.iter().filter(|ext| !ext.is_trusted).count() as u32;

        extension_info.health_score = extension_info.calculate_health_score();

        // Conditionally collect detailed metrics only when issues are detected
        extension_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&extension_info, context).await?;

        Ok(extension_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresExtensionInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresExtensionDetailedMetrics>> {
        let needs_available_extensions = core_info.health_score < Self::HEALTH_THRESHOLD;
        let needs_dependencies = core_info.untrusted_extensions > 0;
        let needs_object_counts = core_info.total_extensions > Self::MANY_EXTENSIONS_THRESHOLD;

        if !needs_available_extensions && !needs_dependencies && !needs_object_counts {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresExtensionDetailedMetrics {
            available_extensions: None,
            extension_dependencies: None,
            extension_objects: None,
            recommendations: core_info.generate_recommendations(),
        };

        // Collect available extensions if health is poor
        if needs_available_extensions {
            let available_input = QueryInput::new(
                "SELECT
                    ae.name, ae.default_version, ae.comment,
                    ae.name IN ('plpgsql', 'pg_stat_statements', 'pgcrypto', 'uuid-ossp', 'hstore', 'btree_gist', 'btree_gin') as trusted,
                    CASE WHEN e.extname IS NULL THEN false ELSE true END as is_installed
                FROM pg_available_extensions ae
                LEFT JOIN pg_extension e ON e.extname = ae.name
                WHERE e.extname IS NULL
                ORDER BY ae.name
                LIMIT 20"
                    .to_string(),
                Vec::new(),
            );

            if let Some(rows) = Self::handle_privileged_query(
                run_query_with_timeout(&available_input, context.clone(), Self::QUERY_TIMEOUT, "available_extensions").await,
                "available_extensions",
            )? {
                detailed_metrics.available_extensions = Some(Self::parse_available_extensions(rows)?);
            }
        }

        // Collect dependencies if there are untrusted extensions
        if needs_dependencies {
            let dependencies_input = QueryInput::new(
                "SELECT
                    e.extname, d.refobjid, de.extname as depends_on_extension,
                    d.deptype
                FROM pg_extension e
                JOIN pg_depend d ON d.objid = e.oid
                LEFT JOIN pg_extension de ON de.oid = d.refobjid
                WHERE d.classid = 'pg_extension'::regclass
                    AND d.deptype IN ('n', 'e')
                    AND de.extname IS NOT NULL
                ORDER BY e.extname
                LIMIT 50"
                    .to_string(),
                Vec::new(),
            );

            if let Some(rows) = Self::handle_privileged_query(
                run_query_with_timeout(&dependencies_input, context.clone(), Self::QUERY_TIMEOUT, "extension_dependencies").await,
                "extension_dependencies",
            )? {
                detailed_metrics.extension_dependencies = Some(Self::parse_dependencies(rows)?);
            }
        }

        // Collect object counts for many extensions
        if needs_object_counts {
            let objects_input = QueryInput::new(
                "SELECT
                    e.extname,
                    COUNT(d.objid) as object_count
                FROM pg_extension e
                LEFT JOIN pg_depend d ON d.refobjid = e.oid
                    AND d.deptype = 'e'
                    AND d.classid != 'pg_extension'::regclass
                GROUP BY e.extname
                ORDER BY object_count DESC"
                    .to_string(),
                Vec::new(),
            );

            if let Some(rows) = Self::handle_privileged_query(
                run_query_with_timeout(&objects_input, context.clone(), Self::QUERY_TIMEOUT, "extension_objects").await,
                "extension_objects",
            )? {
                detailed_metrics.extension_objects = Some(Self::parse_object_counts(rows)?);
            }
        }

        Ok(Some(detailed_metrics))
    }

    // Helper functions for safe type conversion and extraction
    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_bool(row: &PgSimpleRow, column: &str) -> ResultEP<bool> {
        row.get(column)
            .map(|s| s == "t" || s == "true" || s == "1")
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn parse_extensions(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresExtension>> {
        let mut extensions = Vec::with_capacity(rows.len());

        for row in rows {
            let extension_name = Self::safe_get_string(&row, "extname")?;
            let category = PostgresExtensionCategory::from_name(&extension_name);
            let is_trusted = Self::safe_get_bool(&row, "trusted")?;
            let security_level = if is_trusted {
                PostgresExtensionSecurity::Trusted
            } else {
                PostgresExtensionSecurity::assess_from_category(&category)
            };

            extensions.push(PostgresExtension {
                extension_name,
                extension_oid: Self::safe_get_i32(&row, "oid")?,
                installed_version: Self::safe_get_string(&row, "extversion")?,
                default_version: Self::safe_get_string(&row, "default_version")?,
                can_update: Self::safe_get_bool(&row, "can_update")?,
                extension_schema: Self::safe_get_string(&row, "schema_name")?,
                is_relocatable: Self::safe_get_bool(&row, "extrelocatable")?,
                is_trusted,
                description: Self::safe_get_string(&row, "description")?,
                category,
                security_level,
            });
        }

        Ok(extensions)
    }

    fn parse_available_extensions(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresAvailableExtension>> {
        let mut extensions = Vec::with_capacity(rows.len());

        for row in rows {
            extensions.push(PostgresAvailableExtension {
                name: Self::safe_get_string(&row, "name")?,
                default_version: Self::safe_get_string(&row, "default_version")?,
                comment: Self::safe_get_optional_string(&row, "comment")?,
                trusted: Self::safe_get_bool(&row, "trusted")?,
            });
        }

        Ok(extensions)
    }

    fn parse_dependencies(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresExtensionDependency>> {
        let mut dependencies = Vec::with_capacity(rows.len());

        for row in rows {
            let deptype_char = Self::safe_get_string(&row, "deptype")?;
            let dependency_type = if deptype_char == "e" {
                PostgresDependencyType::Extension
            } else {
                PostgresDependencyType::Normal
            };

            dependencies.push(PostgresExtensionDependency {
                extension_name: Self::safe_get_string(&row, "extname")?,
                depends_on: Self::safe_get_string(&row, "depends_on_extension")?,
                dependency_type,
            });
        }

        Ok(dependencies)
    }

    fn parse_object_counts(rows: Vec<PgSimpleRow>) -> ResultEP<HashMap<String, u32>> {
        let mut object_counts = HashMap::new();

        for row in rows {
            let extension_name = Self::safe_get_string(&row, "extname")?;
            let count_text = row
                .get("object_count")
                .ok_or_else(|| EpError::metadata("Failed to get object_count: column not found or NULL".to_string()))?;
            let count = count_text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get object_count: {}", e)))?;

            object_counts.insert(extension_name, count as u32);
        }

        Ok(object_counts)
    }
}

impl PostgresExtensionInfo {
    fn handle_privileged_query(result: ResultEP<Vec<PgSimpleRow>>, query_name: &str) -> ResultEP<Option<Vec<PgSimpleRow>>> {
        match result {
            Ok(rows) => Ok(Some(rows)),
            Err(err) if Self::is_permission_error(&err) => {
                log_warn!(
                    LogContext::default().with_feature("metadata"),
                    format!("postgres.extension_info query `{}` skipped due to insufficient privileges: {}", query_name, err),
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
        let mut info = PostgresExtensionInfo::default();
        info.health_score = info.calculate_health_score();
        info
    }

    /// Calculates overall extension health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        if self.total_extensions > 0 {
            // Deduct for extensions with available updates
            if self.extensions_with_updates > 0 {
                let update_penalty = (self.extensions_with_updates as f64 / self.total_extensions as f64) * 20.0;
                score -= update_penalty;
            }

            // Deduct for untrusted extensions
            if self.untrusted_extensions > 0 {
                let security_penalty = (self.untrusted_extensions as f64 / self.total_extensions as f64) * 15.0;
                score -= security_penalty;
            }
        }

        // Deduct for too many extensions (potential bloat)
        if self.total_extensions > 20 {
            score -= ((self.total_extensions - 20) as f64 * 2.0).min(20.0);
        }

        score.max(0.0)
    }

    /// Generates extension management recommendations
    fn generate_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.extensions_with_updates > 0 {
            recommendations.push(format!(
                "{} extension(s) have available updates - review and update when appropriate",
                self.extensions_with_updates
            ));
        }

        if self.untrusted_extensions > 0 {
            recommendations.push(format!(
                "{} untrusted extension(s) installed - review security implications",
                self.untrusted_extensions
            ));
        }

        if self.total_extensions > 15 {
            recommendations.push("Many extensions installed - consider removing unused extensions".to_string());
        }

        if self.total_extensions == 0 {
            recommendations.push("No extensions installed - consider pg_stat_statements for query monitoring".to_string());
        }

        // Check for common recommended extensions that are missing
        let has_pg_stat_statements = self.installed_extensions.iter().any(|ext| ext.extension_name == "pg_stat_statements");

        if !has_pg_stat_statements {
            recommendations.push("Consider installing pg_stat_statements for query performance monitoring".to_string());
        }

        recommendations
    }

    /// Gets extensions by category
    pub fn get_extensions_by_category(&self, category: PostgresExtensionCategory) -> Vec<&PostgresExtension> {
        self.installed_extensions.iter().filter(|ext| ext.category == category).collect()
    }

    /// Gets extensions that need updates
    pub fn get_extensions_needing_updates(&self) -> Vec<&PostgresExtension> {
        self.installed_extensions.iter().filter(|ext| ext.can_update).collect()
    }

    /// Gets untrusted extensions
    pub fn get_untrusted_extensions(&self) -> Vec<&PostgresExtension> {
        self.installed_extensions.iter().filter(|ext| !ext.is_trusted).collect()
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets extension health summary
    pub fn get_extension_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Extensions are well-maintained".to_string(),
            75..=89 => "Good - Extensions are mostly up-to-date".to_string(),
            60..=74 => "Fair - Some extension maintenance needed".to_string(),
            40..=59 => "Poor - Multiple extension issues detected".to_string(),
            _ => "Critical - Extension management requires attention".to_string(),
        }
    }
}

/// Available extension information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresAvailableExtension {
    /// Extension name
    pub name: String,
    /// Default version
    pub default_version: String,
    /// Extension comment/description
    pub comment: Option<String>,
    /// Whether extension is trusted
    pub trusted: bool,
}

/// Extension dependency information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresExtensionDependency {
    /// Extension name
    pub extension_name: String,
    /// Extension this depends on
    pub depends_on: String,
    /// Type of dependency
    pub dependency_type: PostgresDependencyType,
}

/// Types of extension dependencies
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresDependencyType {
    /// Normal dependency
    Normal,
    /// Extension dependency
    Extension,
}

/// Extension categories
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresExtensionCategory {
    /// Statistics and monitoring extensions
    Statistics,
    /// Data type extensions
    DataTypes,
    /// Index and search extensions
    Indexing,
    /// Connectivity and foreign data wrappers
    Connectivity,
    /// Security and authentication
    Security,
    /// Utility and administrative tools
    Utilities,
    /// Geographic and spatial data
    Geographic,
    /// Machine learning and analytics
    Analytics,
    /// Development and debugging tools
    Development,
    /// Third-party or custom extensions
    ThirdParty,
    /// Unknown category
    Unknown,
}

impl PostgresExtensionCategory {
    /// Infers category from extension name
    pub fn from_name(name: &str) -> Self {
        let name_lower = name.to_lowercase();

        if name_lower.contains("stat") || name_lower.contains("monitor") {
            PostgresExtensionCategory::Statistics
        } else if name_lower.contains("uuid") || name_lower.contains("hstore") || name_lower.contains("json") || name_lower.contains("xml")
        {
            PostgresExtensionCategory::DataTypes
        } else if name_lower.contains("gin") || name_lower.contains("gist") || name_lower.contains("index") || name_lower.contains("btree")
        {
            PostgresExtensionCategory::Indexing
        } else if name_lower.contains("fdw") || name_lower.contains("foreign") {
            PostgresExtensionCategory::Connectivity
        } else if name_lower.contains("crypto") || name_lower.contains("auth") || name_lower.contains("ssl") {
            PostgresExtensionCategory::Security
        } else if name_lower.contains("postgis") || name_lower.contains("geo") {
            PostgresExtensionCategory::Geographic
        } else if name_lower.contains("ml") || name_lower.contains("analytics") || name_lower.contains("vector") {
            PostgresExtensionCategory::Analytics
        } else if name_lower.contains("debug") || name_lower.contains("dev") || name_lower.contains("test") {
            PostgresExtensionCategory::Development
        } else if name_lower.starts_with("pg_") || name_lower == "plpgsql" {
            PostgresExtensionCategory::Utilities
        } else {
            PostgresExtensionCategory::Unknown
        }
    }
}

/// Extension security levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresExtensionSecurity {
    /// Trusted extension, safe for general use
    Trusted,
    /// Generally safe but requires some privileges
    Safe,
    /// Requires careful configuration
    Moderate,
    /// Requires superuser privileges, potential risks
    Elevated,
    /// High security risk, use with caution
    High,
}

impl PostgresExtensionSecurity {
    /// Assesses security level based on category
    pub fn assess_from_category(category: &PostgresExtensionCategory) -> Self {
        match category {
            PostgresExtensionCategory::Security => PostgresExtensionSecurity::Elevated,
            PostgresExtensionCategory::Connectivity => PostgresExtensionSecurity::Moderate,
            PostgresExtensionCategory::Development => PostgresExtensionSecurity::Moderate,
            PostgresExtensionCategory::ThirdParty => PostgresExtensionSecurity::Elevated,
            PostgresExtensionCategory::Geographic => PostgresExtensionSecurity::Safe,
            PostgresExtensionCategory::Analytics => PostgresExtensionSecurity::Safe,
            _ => PostgresExtensionSecurity::Safe,
        }
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_extension_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let extension_info = PostgresExtensionInfo::default();

        let result = extension_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok(), "sync_metadata failed: {:?}", result.as_ref().err());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
    }
}
