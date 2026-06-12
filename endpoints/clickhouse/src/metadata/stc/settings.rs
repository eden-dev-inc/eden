#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO etc.)
use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

mod analytics;
mod classification_types;
mod core_sync;
mod detailed_sync;
mod helpers;
mod model_types;
mod parsers;

pub(crate) use classification_types::*;
pub(crate) use model_types::*;

/// Clickhouse configuration settings and their performance impact.
///
/// Tracks configuration parameters, drift and per-setting risk scores.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSettingsInfo {
    /// Total number of configuration settings
    pub total_settings_count: u64,
    /// Number of settings with non-default values
    pub custom_settings_count: u64,
    /// Number of settings that differ across cluster nodes
    pub inconsistent_settings_count: u64,
    /// Number of deprecated settings in use
    pub deprecated_settings_count: u64,
    /// Number of potentially dangerous settings
    pub dangerous_settings_count: u64,
    /// Number of memory-related settings
    pub memory_settings_count: u64,
    /// Number of performance-tuning settings
    pub performance_settings_count: u64,
    /// Number of security-related settings
    pub security_settings_count: u64,
    /// Total configured memory limit across all settings (bytes)
    pub total_memory_limit: u64,
    /// Maximum single query memory limit (bytes)
    pub max_query_memory_limit: u64,
    /// Number of concurrent processing threads configured
    pub max_threads: u64,
    /// Maximum number of connections allowed
    pub max_connections: u64,
    /// Query timeout settings in seconds
    pub query_timeout_seconds: u64,
    /// Number of settings that have been changed recently
    pub recently_changed_settings: u64,
    /// Number of settings with recommended value mismatches
    pub settings_needing_optimization: u64,
    /// Detailed settings collected when configuration issues are detected
    pub detailed_settings: Option<ClickhouseSettingsDetailedInfo>,
}

/// Detailed settings information collected when configuration issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSettingsDetailedInfo {
    /// Settings that differ across cluster nodes
    pub inconsistent_settings: Vec<ClickhouseInconsistentSetting>,
    /// Deprecated settings currently in use
    pub deprecated_settings: Vec<ClickhouseDeprecatedSetting>,
    /// Potentially dangerous configuration settings
    pub dangerous_settings: Vec<ClickhouseDangerousSetting>,
    /// Memory-related configuration settings
    pub memory_settings: Vec<ClickhouseMemorySetting>,
    /// Performance-tuning settings
    pub performance_settings: Vec<ClickhousePerformanceSetting>,
    /// Security-related settings
    pub security_settings: Vec<ClickhouseSecuritySetting>,
    /// Settings that have been changed recently
    pub recent_setting_changes: Vec<ClickhouseRecentSettingChange>,
    /// Settings that need optimization based on current usage
    pub optimization_recommendations: Vec<ClickhouseSettingOptimization>,
    /// Resource limit settings
    pub resource_limit_settings: Vec<ClickhouseResourceLimitSetting>,
    /// Cluster configuration differences
    pub cluster_configuration_drift: Vec<ClickhouseClusterConfigDrift>,
}

impl MetadataCollection for ClickhouseSettingsInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_SETTINGS_OVERVIEW,
                query(
                    "SELECT
                    count() as total_settings_count,
                    countIf(value != `default`) as custom_settings_count,
                    countIf(name LIKE '%memory%' OR name LIKE '%Memory%') as memory_settings_count,
                    countIf(name LIKE '%thread%' OR name LIKE '%Thread%' OR name LIKE '%parallel%') as performance_settings_count,
                    countIf(name LIKE '%timeout%' OR name LIKE '%Timeout%') as timeout_settings_count
                FROM system.settings"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_MEMORY_LIMITS,
                query(
                    "SELECT
                    toUInt64OrZero(value) as max_memory_usage,
                    toUInt64OrZero((SELECT value FROM system.settings WHERE name = 'max_query_memory_usage')) as max_query_memory_usage,
                    toUInt64OrZero((SELECT value FROM system.settings WHERE name = 'max_threads')) as max_threads,
                    toUInt64OrZero((SELECT value FROM system.settings WHERE name = 'max_connections')) as max_connections
                FROM system.settings
                WHERE name = 'max_memory_usage'"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_TIMEOUT_SETTINGS,
                query(
                    "SELECT
                    toUInt64OrZero(value) as query_timeout
                FROM system.settings
                WHERE name = 'max_execution_time'"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CLUSTER_SETTINGS_CONSISTENCY,
                query("SELECT 0 as inconsistent_settings_count".to_string()),
            ),
            (
                Self::QUERY_DEPRECATED_SETTINGS_COUNT,
                query(
                    "SELECT
                    countIf(name IN (
                        'use_uncompressed_cache',
                        'compile_expressions',
                        'min_count_to_compile_expression',
                        'group_by_overflow_mode',
                        'totals_mode'
                    )) as deprecated_settings_count
                FROM system.settings
                WHERE value != `default`"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse configuration settings and performance impact metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "configuration"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseSettingsInfo {
    const DANGEROUS_MEMORY_THRESHOLD: u64 = 100_000_000_000; // 100GB
    const HIGH_THREAD_COUNT_THRESHOLD: u64 = 128;
    const HIGH_CONNECTION_THRESHOLD: u64 = 10000;
    const LONG_TIMEOUT_THRESHOLD: u64 = 3600; // 1 hour
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 100;
    const QUERY_SETTINGS_OVERVIEW: &'static str = "settings_overview";
    const QUERY_MEMORY_LIMITS: &'static str = "memory_limits";
    const QUERY_TIMEOUT_SETTINGS: &'static str = "timeout_settings";
    const QUERY_CLUSTER_SETTINGS_CONSISTENCY: &'static str = "cluster_settings_consistency";
    const QUERY_DEPRECATED_SETTINGS_COUNT: &'static str = "deprecated_settings";
    const DETAIL_QUERY_INCONSISTENT_SETTINGS: &'static str = "inconsistent_settings";
    const DETAIL_QUERY_DEPRECATED_SETTINGS: &'static str = "deprecated_settings";
    const DETAIL_QUERY_MEMORY_SETTINGS: &'static str = "memory_settings";
    const DETAIL_QUERY_PERFORMANCE_SETTINGS: &'static str = "performance_settings";
    const DETAIL_QUERY_SECURITY_SETTINGS: &'static str = "security_settings";
    const DETAIL_QUERY_RESOURCE_SETTINGS: &'static str = "resource_settings";

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: ClickhouseAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        core_sync::sync_metadata(self, context).await
    }

    fn should_collect_detailed_settings(core_info: &ClickhouseSettingsInfo) -> bool {
        core_info.inconsistent_settings_count > 0
            || core_info.deprecated_settings_count > 0
            || core_info.dangerous_settings_count > 0
            || core_info.custom_settings_count > 50
            || core_info.settings_needing_optimization > 0
            || core_info.total_memory_limit > Self::DANGEROUS_MEMORY_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_types::metadata::MetadataCollection;

    #[test]
    fn calculates_settings_risk_counters() {
        let info = ClickhouseSettingsInfo {
            total_memory_limit: 150_000_000_000,
            max_threads: 256,
            max_connections: 20_000,
            query_timeout_seconds: 7_200,
            custom_settings_count: 60,
            max_query_memory_limit: 10_000_000_000,
            ..Default::default()
        };

        assert_eq!(ClickhouseSettingsInfo::calculate_dangerous_settings_count(&info), 4);
        assert_eq!(ClickhouseSettingsInfo::calculate_security_settings_count(&info), 3);
        assert_eq!(ClickhouseSettingsInfo::calculate_optimization_needs(&info), 5);
    }

    #[test]
    fn settings_request_uses_named_keys() {
        let req = ClickhouseSettingsInfo::default().request();

        assert!(req.contains_key(ClickhouseSettingsInfo::QUERY_SETTINGS_OVERVIEW));
        assert!(req.contains_key(ClickhouseSettingsInfo::QUERY_MEMORY_LIMITS));
        assert!(req.contains_key(ClickhouseSettingsInfo::QUERY_TIMEOUT_SETTINGS));
        assert!(req.contains_key(ClickhouseSettingsInfo::QUERY_CLUSTER_SETTINGS_CONSISTENCY));
        assert!(req.contains_key(ClickhouseSettingsInfo::QUERY_DEPRECATED_SETTINGS_COUNT));
    }
}
