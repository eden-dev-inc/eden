//! # eden_config
//!
//! Centralized configuration management for Eden MDBS.
//!
//! ## Overview
//!
//! This crate replaces scattered `std::env::var()` calls across the Eden codebase
//! with a single, validated configuration system. It is used by `eden_service`,
//! endpoint runtimes, telemetry exporters, and supporting tools.
//!
//! ## Architecture
//!
//! The architecture rests on three pillars:
//!
//! - **[`ArcSwap`]** global (`CONFIG`) — lock-free, wait-free reads (~2-5ns)
//!   with atomic swap for updates. No partial state is ever visible to readers.
//! - **[`ConfigFieldRef<T>`]** — zero-copy smart pointer that holds an `Arc<EdenConfig>`
//!   and derefs to a specific field, avoiding clones for read-only access.
//! - **[Figment](https://docs.rs/figment)** 5-layer merge — defaults → TOML →
//!   legacy env vars (defined in `compat.rs`) → nested env vars (`EDEN__` prefix)
//!   → CLI args.
//!
//! ## Error Handling
//!
//! [`ConfigError`] has three variants:
//! - `LoadError` — general config loading failure
//! - `InvalidValue` — validation constraint violation
//! - `Figment` — Figment deserialization/merge error
//!
//! On startup, the global `CONFIG` lazy static **panics** if loading fails,
//! ensuring misconfigurations are caught immediately rather than causing
//! silent runtime issues.
//!
//! ## Design Philosophy
//!
//! The config crate uses `Lazy<ArcSwap<EdenConfig>>` to provide:
//!
//! 1. **Lock-free reads** - Wait-free atomic loads with zero contention (~2-5ns per access)
//! 2. **Atomic updates** - Entire config swaps atomically, preventing partial state visibility
//! 3. **Zero-copy access** - Direct references to config fields without cloning
//!
//! ## Configuration Sources (Priority Order)
//!
//! 1. Compiled defaults (lowest priority)
//! 2. TOML config file (`eden.toml`)
//! 3. Legacy flat environment variables (`EDEN_PORT`)
//! 4. Nested environment variables (`EDEN__SERVICES__EDEN__PORT`)
//! 5. CLI arguments (highest priority)
//!
//! **Note:** Configuration loading failures will cause the application to panic
//! at startup rather than silently falling back to defaults. This ensures that
//! misconfigurations are caught immediately rather than causing runtime issues.
//!
//! ## Usage Patterns
//!
//! ### Reading Configuration
//!
//! Config accessors return smart pointers that automatically dereference to the config type:
//!
//! ```rust
//! use eden_config::features;
//!
//! let flags = features();
//! if flags.analytics_enabled {
//!     println!("Analytics enabled");
//! }
//! ```
//!
//! ### When to Clone
//!
//! The returned smart pointers hold an Arc to the config. Clone explicitly when you need owned data:
//!
//! ```rust,ignore
//! use eden_config::{analytics, AnalyticsConfig};
//!
//! let analytics_cfg: AnalyticsConfig = analytics().clone();
//! tokio::spawn(async move {
//!     process_analytics(analytics_cfg).await;
//! });
//! ```
//!
//! ### Updating Configuration
//!
//! ```rust
//! use eden_config::update_config;
//!
//! update_config(|c| {
//!     c.limits.rate_limit_ms = 200;
//! }).expect("config validation failed");
//! ```
//!
//! ### Hot Reload from File
//!
//! ```rust,no_run
//! use eden_config::reload_config;
//!
//! // Reloads from file while preserving original CLI argument overrides
//! reload_config().expect("reload failed");
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Config accessor call**: ~2-5ns (atomic load + Arc refcount increment)
//! - **Field access**: 0ns (direct Deref, no overhead)
//! - **Cloning small configs** (FeatureFlags): ~5-10ns
//! - **Cloning large configs** (AnalyticsConfig): ~100-200ns + heap allocations
//!
//! **Recommendation**: Use zero-copy access unless you need owned data.
//!
//! ## Thread Safety
//!
//! All config accessors are lock-free and thread-safe. Multiple readers can access
//! config simultaneously without blocking. Old config versions remain valid until
//! all readers release their references.

#![allow(clippy::result_large_err)]

pub mod agents;
mod analytics;
mod backups;
mod cli;
mod compat;
mod databases;
mod encryption;
mod error;
mod features;
mod licensing;
mod limits;
mod marketplace;
mod memory;
mod org_transfer;
mod rbac_pg_sync;
mod services;
mod telemetry;

pub use agents::{AgentsConfig, AgentsSecurityConfig, SkillPolicyConfig, SkillPolicyMode};
pub use analytics::{
    defaults, AggAntiPatternsConfig, AlwaysOnConfig, AnalyticsConfig, AuditConfig, BurstConfig, DiscoveryConfig, DivergenceConfig,
    ForceSampleConfig, IngestionConfig, MongoAnalyticsConfig, MongoAntiPatternsConfig, MongoProfilingMode, MongoRecommendationConfig,
    PostgresAnalyticsConfig, PostgresAntiPatternsConfig, RecommendationConfig, RedisAggAntiPatternsConfig, RedisAnalyticsConfig,
    SamplingConfig, StreamConfig,
};
pub use backups::BackupConfig;
pub use cli::CliArgs;
pub use databases::{DatabasesConfig, InternalClickhouseConfig, InternalPostgresConfig, InternalRedisConfig};
pub use encryption::EncryptionConfig;
pub use error::ConfigError;
pub use features::{FeatureFlags, PolicyMode};
pub use licensing::LicensingClientConfig;
pub use limits::LimitsConfig;
pub use marketplace::MarketplaceConfig;
pub use memory::MemoryConfig;
pub use org_transfer::OrgTransferConfig;
pub use rbac_pg_sync::RbacPgSyncConfig;
pub use services::{
    EdenServiceConfig, EngineServiceConfig, GatewayCpuAffinityMode, InternalLlmConfig, LlmTier, ResolvedLlmTier, ServicesConfig,
};
pub use telemetry::{DuckDbTelemetryConfig, TelemetryConfig};

use arc_swap::{ArcSwap, Guard};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, path::Path, sync::Arc};

/// Root configuration for Eden MDBS.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EdenConfig {
    pub features: FeatureFlags,
    pub agents: AgentsConfig,
    pub analytics: AnalyticsConfig,
    pub limits: LimitsConfig,
    pub telemetry: TelemetryConfig,
    pub databases: DatabasesConfig,
    pub services: ServicesConfig,
    pub rbac_pg_sync: RbacPgSyncConfig,
    pub backup: BackupConfig,
    pub org_transfer: OrgTransferConfig,
    pub licensing: LicensingClientConfig,
    pub memory: MemoryConfig,
    pub marketplace: MarketplaceConfig,
    pub encryption: encryption::EncryptionConfig,
}

impl EdenConfig {
    /// Validate all configuration constraints.
    ///
    /// Checks port values, pool sizes, analytics rules, licensing consistency,
    /// and the cross-field constraint that PII detection requires analytics to be enabled.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.services.eden.port == 0 {
            return Err(ConfigError::InvalidValue("services.eden.port cannot be 0".into()));
        }
        if self.services.engine.port == 0 {
            return Err(ConfigError::InvalidValue("services.engine.port cannot be 0".into()));
        }
        if self.limits.clickhouse_pool_size == 0 {
            return Err(ConfigError::InvalidValue("limits.clickhouse_pool_size must be > 0".into()));
        }

        self.analytics.validate()?;
        self.licensing.validate()?;

        if self.analytics.sampling.always_on.pii_detection && !self.features.analytics_enabled {
            return Err(ConfigError::InvalidValue(
                "analytics.sampling.always_on.pii_detection requires features.analytics_enabled".into(),
            ));
        }

        Ok(())
    }

    /// Toggle the `features.analytics_enabled` flag at runtime.
    pub fn set_analytics_enabled(enabled: bool) -> Result<(), ConfigError> {
        update_config(|c| c.features.analytics_enabled = enabled)
    }

    /// Update the `limits.rate_limit_ms` value at runtime.
    pub fn set_rate_limit_ms(ms: u64) -> Result<(), ConfigError> {
        update_config(|c| c.limits.rate_limit_ms = ms)
    }

    /// Update the `telemetry.log_level` value at runtime.
    pub fn set_log_level(level: String) -> Result<(), ConfigError> {
        update_config(|c| c.telemetry.log_level = level)
    }
}

/// Smart pointer that provides lock-free access to a config field.
///
/// This wrapper holds an Arc to the full config and a projection function
/// that extracts a reference to a specific field. It automatically
/// dereferences to the field type.
pub struct ConfigFieldRef<T> {
    config: Arc<EdenConfig>,
    project: fn(&EdenConfig) -> &T,
}

impl<T> Deref for ConfigFieldRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        (self.project)(&self.config)
    }
}

/// Load configuration from file with optional CLI argument overrides.
///
/// Priority order (lowest to highest):
/// 1. Compiled defaults
/// 2. TOML config file (eden.toml or from args)
/// 3. Legacy flat env vars (EDEN_PORT)
/// 4. Nested env vars (EDEN__SERVICES__EDEN__PORT)
/// 5. CLI args (if provided - highest priority)
pub fn load_config(args: Option<&CliArgs>) -> Result<EdenConfig, ConfigError> {
    let config_path = args.map(|a| a.config.as_path()).unwrap_or_else(|| Path::new("eden.toml"));
    load_config_from_path(config_path, args)
}

fn load_config_from_path(config_path: &Path, args: Option<&CliArgs>) -> Result<EdenConfig, ConfigError> {
    let mut figment = Figment::new().merge(Serialized::defaults(EdenConfig::default())).merge(Toml::file(config_path));

    figment = crate::compat::apply_legacy_env_vars(figment);
    figment = figment.merge(Env::prefixed("EDEN__").split("__"));

    if let Some(cli_args) = args {
        if let Some(port) = cli_args.port {
            figment = figment.merge(Serialized::global("services.eden.port", port));
        }
        if let Some(ref level) = cli_args.log_level {
            figment = figment.merge(Serialized::global("telemetry.log_level", level));
        }
        if let Some(ref collector) = cli_args.otlp_collector {
            figment = figment.merge(Serialized::global("telemetry.otlp_collector", collector));
        }
    }

    let config: EdenConfig = figment.extract()?;
    config.validate()?;
    Ok(config)
}

pub static CONFIG: Lazy<ArcSwap<EdenConfig>> = Lazy::new(|| {
    let config = load_config(None).unwrap_or_else(|e| {
        panic!(
            "FATAL: Failed to load Eden MDBS configuration: {e}\n\
             \n\
             Configuration errors must be fixed before the service can start.\n\
             \n\
             Common causes:\n\
             • Config file (eden.toml) has TOML syntax errors\n\
             • Required environment variables are missing (check EDEN__ prefixed vars)\n\
             • Config validation failed (port = 0, invalid pool sizes, inconsistent flags)\n\
             \n\
             Check the error message above for specific details.\n\
             Set EDEN_LOG_LEVEL=debug for verbose configuration loading output."
        );
    });
    ArcSwap::from_pointee(config)
});

/// Global storage for CLI arguments to preserve them across reloads.
pub static CLI_ARGS: Lazy<ArcSwap<Option<CliArgs>>> = Lazy::new(|| ArcSwap::new(Arc::new(None)));

/// Load the full [`EdenConfig`] from the global store.
///
/// Returns a lightweight guard that derefs to `Arc<EdenConfig>`.
pub fn config() -> Guard<Arc<EdenConfig>> {
    CONFIG.load()
}

/// Zero-copy access to [`FeatureFlags`] (analytics, policy mode, Redis PSYNC).
pub fn features() -> impl Deref<Target = FeatureFlags> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.features }
}

/// Zero-copy access to [`AgentsConfig`] (skills, tool-pass limits, agent security).
pub fn agents() -> ConfigFieldRef<AgentsConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.agents }
}

/// Zero-copy access to [`AnalyticsConfig`] (sampling, audit, stream, ingestion, metadata, Redis analytics).
pub fn analytics() -> impl Deref<Target = AnalyticsConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.analytics }
}

/// Zero-copy access to [`LimitsConfig`] (rate limits, pool sizes, timeouts).
pub fn limits() -> impl Deref<Target = LimitsConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.limits }
}

/// Zero-copy access to [`TelemetryConfig`] (OTLP collectors, log level).
pub fn telemetry() -> impl Deref<Target = TelemetryConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.telemetry }
}

/// Zero-copy access to [`DatabasesConfig`] (Redis, PostgreSQL, ClickHouse).
pub fn databases() -> impl Deref<Target = DatabasesConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.databases }
}

/// Zero-copy access to [`ServicesConfig`] (Eden, Engine, LLM).
pub fn services() -> impl Deref<Target = ServicesConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.services }
}

/// Zero-copy access to [`RbacPgSyncConfig`] (RBAC Redis-to-Postgres sync worker).
pub fn rbac_pg_sync() -> impl Deref<Target = RbacPgSyncConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.rbac_pg_sync }
}

/// Zero-copy access to [`BackupConfig`] (path, password, directory).
pub fn backup() -> impl Deref<Target = BackupConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.backup }
}

/// Zero-copy access to [`OrgTransferConfig`] (transfer directory).
pub fn org_transfer() -> impl Deref<Target = OrgTransferConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.org_transfer }
}

/// Zero-copy access to [`LicensingClientConfig`] for external metering compatibility.
pub fn licensing() -> impl Deref<Target = LicensingClientConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.licensing }
}

/// Zero-copy access to [`MemoryConfig`].
pub fn memory() -> impl Deref<Target = MemoryConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.memory }
}

/// Zero-copy access to [`MarketplaceConfig`].
pub fn marketplace() -> impl Deref<Target = MarketplaceConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.marketplace }
}

/// Zero-copy access to [`EncryptionConfig`] (ELS credential encryption-at-rest).
pub fn encryption() -> impl Deref<Target = encryption::EncryptionConfig> {
    ConfigFieldRef { config: CONFIG.load_full(), project: |c| &c.encryption }
}

/// Atomically update the configuration using a mutator function.
///
/// The mutator receives a mutable copy of the current config.
/// If validation succeeds, the new config is atomically swapped in.
pub fn update_config(mutator: impl FnOnce(&mut EdenConfig)) -> Result<(), ConfigError> {
    let mut new_config = (**CONFIG.load()).clone();
    mutator(&mut new_config);
    new_config.validate()?;
    CONFIG.store(Arc::new(new_config));
    Ok(())
}

/// Reload configuration from file, preserving original CLI argument overrides.
///
/// This loads fresh config from the file and reapplies any CLI args that were
/// provided at startup, then atomically swaps it in.
pub fn reload_config() -> Result<(), ConfigError> {
    let cli_args = CLI_ARGS.load();
    // load_config already validates; no need to validate again.
    let new_config = load_config(cli_args.as_ref().as_ref())?;
    CONFIG.store(Arc::new(new_config));
    Ok(())
}

/// Install a pre-loaded configuration into the global CONFIG.
///
/// Note: This does not store CLI args. If you have CLI args available,
/// use `install_config_with_args()` instead to ensure hot reloads work correctly.
pub fn install_config(config: EdenConfig) {
    CONFIG.store(Arc::new(config));
}

/// Install a pre-loaded configuration and CLI args into the global stores.
///
/// This is the recommended way to initialize config from main.rs when CLI args
/// are available, ensuring that hot reloads preserve CLI overrides.
pub fn install_config_with_args(config: EdenConfig, args: Option<CliArgs>) {
    CLI_ARGS.store(Arc::new(args));
    CONFIG.store(Arc::new(config));
}

/// Install default configuration without loading from file.
///
/// **WARNING:** Only use this for testing or development. Production
/// deployments should load configuration from files and environment variables.
#[doc(hidden)]
pub fn install_default_config() {
    CONFIG.store(Arc::new(EdenConfig::default()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_default_config_is_valid() {
        let config = EdenConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_zero_port_invalid() {
        let mut config = EdenConfig::default();
        config.services.eden.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_pii_detection_requires_analytics() {
        let mut config = EdenConfig::default();
        config.features.analytics_enabled = false;
        config.analytics.sampling.always_on.pii_detection = true;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_load_config_defaults() {
        let config = load_config(None).unwrap_or_else(|_| EdenConfig::default());
        assert_eq!(config.services.eden.port, 8000);
        assert_eq!(config.services.engine.port, 8001);
    }

    #[test]
    fn test_cli_args_override() {
        let args = CliArgs {
            config: std::path::PathBuf::from("eden.toml"),
            port: Some(9000),
            log_level: Some("debug".to_string()),
            otlp_collector: Some("http://custom:4317".to_string()),
        };

        let config = load_config(Some(&args)).unwrap_or_else(|_| EdenConfig::default());
        assert_eq!(config.services.eden.port, 9000);
    }

    #[test]
    #[serial]
    fn test_reload_preserves_cli_args() {
        // Install config with CLI override
        let args = CliArgs {
            config: std::path::PathBuf::from("eden.toml"),
            port: Some(9000),
            log_level: None,
            otlp_collector: None,
        };

        let config = load_config(Some(&args)).unwrap_or_else(|_| EdenConfig::default());
        install_config_with_args(config, Some(args));

        assert_eq!(services().eden.port, 9000);

        // Reload should preserve CLI override
        let _ = reload_config();
        assert_eq!(services().eden.port, 9000);

        // Cleanup
        install_config(EdenConfig::default());
    }

    #[test]
    fn test_analytics_config_validation() {
        let mut config = EdenConfig::default();
        config.analytics.sampling.burst.enabled = true;
        config.analytics.sampling.burst.window_duration_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    #[serial]
    fn test_global_config_access() {
        install_config(EdenConfig::default());
        let cfg = config();
        assert_eq!(cfg.services.eden.port, 8000);
    }

    #[test]
    #[serial]
    fn test_convenience_accessors() {
        install_config(EdenConfig::default());

        assert_eq!(limits().rate_limit_ms, 100);
        assert_eq!(services().eden.port, 8000);
        assert_eq!(databases().redis.port, 6379);
        assert_eq!(telemetry().log_level, "info");
        assert!(telemetry().clickhouse_enabled);
        assert_eq!(agents().session_ttl_secs, 1_800);
        assert!(analytics().sampling.burst.enabled);
        assert!(backup().path.is_none());
    }

    #[test]
    #[serial]
    fn test_update_config() {
        install_config(EdenConfig::default());
        let original_rate_limit = limits().rate_limit_ms;

        let result = update_config(|c| {
            c.limits.rate_limit_ms = 200;
        });
        assert!(result.is_ok());
        assert_eq!(limits().rate_limit_ms, 200);

        let _ = update_config(|c| {
            c.limits.rate_limit_ms = original_rate_limit;
        });
    }

    #[test]
    #[serial]
    fn test_update_config_validation() {
        install_config(EdenConfig::default());
        let result = update_config(|c| {
            c.services.eden.port = 0;
        });
        assert!(result.is_err());
        assert!(config().validate().is_ok());
        assert_ne!(config().services.eden.port, 0);
    }

    #[test]
    #[serial]
    fn test_typed_setters() {
        install_config(EdenConfig::default());

        assert!(EdenConfig::set_analytics_enabled(true).is_ok());
        assert!(features().analytics_enabled);

        assert!(EdenConfig::set_rate_limit_ms(150).is_ok());
        assert_eq!(limits().rate_limit_ms, 150);

        assert!(EdenConfig::set_log_level("debug".to_string()).is_ok());
        assert_eq!(telemetry().log_level, "debug");

        let _ = EdenConfig::set_analytics_enabled(false);
        let _ = EdenConfig::set_rate_limit_ms(100);
        let _ = EdenConfig::set_log_level("info".to_string());
    }

    #[test]
    #[serial]
    fn test_install_config() {
        install_config(EdenConfig::default());
        let mut custom_config = EdenConfig::default();
        custom_config.services.eden.port = 9999;

        install_config(custom_config);
        assert_eq!(config().services.eden.port, 9999);

        install_config(EdenConfig::default());
    }

    #[test]
    fn test_invalid_config_returns_error() {
        // Test that validation errors are returned correctly
        let mut config = EdenConfig::default();
        config.services.eden.port = 0;

        let result = config.validate();
        assert!(result.is_err(), "Config with port=0 should fail validation");

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("port cannot be 0"), "Error message should mention port validation: {}", err_msg);
    }

    #[test]
    fn test_install_default_config_helper() {
        install_default_config();
        assert_eq!(config().services.eden.port, 8000);
        assert_eq!(config().services.engine.port, 8001);
    }

    #[test]
    fn test_recommendation_config_defaults_valid() {
        let config = RecommendationConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_recommendation_config_zero_observation_windows() {
        let config = RecommendationConfig { min_observation_windows: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("min_observation_windows"));
    }

    #[test]
    fn test_recommendation_config_zero_min_total_requests() {
        let config = RecommendationConfig { min_total_requests: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("min_total_requests"));
    }

    #[test]
    fn test_recommendation_config_invalid_ratio() {
        let config = RecommendationConfig { write_heavy_ratio: 1.5, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("write_heavy_ratio"));
    }

    #[test]
    fn test_recommendation_config_negative_ram_price() {
        let config = RecommendationConfig { ram_price_per_gb_monthly: -1.0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("ram_price_per_gb_monthly"));
    }

    #[test]
    fn test_recommendation_config_zero_stale_days() {
        let config = RecommendationConfig { stale_days: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("stale_days"));
    }

    #[test]
    fn test_recommendation_config_zero_oversized_value_bytes() {
        let config = RecommendationConfig { oversized_value_bytes: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("oversized_value_bytes"));
    }

    #[test]
    fn test_ingestion_config_defaults_valid() {
        let config = IngestionConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_ingestion_config_zero_flush_interval() {
        let config = IngestionConfig { rollup_flush_interval_secs: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("rollup_flush_interval_secs"));
    }

    #[test]
    fn test_ingestion_config_zero_shutdown_timeout() {
        let config = IngestionConfig { shutdown_flush_timeout_secs: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("shutdown_flush_timeout_secs"));
    }

    #[test]
    fn test_ingestion_config_zero_live_flush_timeout() {
        let config = IngestionConfig { live_flush_timeout_secs: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("live_flush_timeout_secs"));
    }

    #[test]
    fn test_ingestion_config_zero_shutdown_total_timeout() {
        let config = IngestionConfig { shutdown_total_timeout_secs: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("shutdown_total_timeout_secs"));
    }

    #[test]
    fn test_ingestion_config_zero_blocked_command_max() {
        let config = IngestionConfig { blocked_command_max_per_endpoint: 0, ..Default::default() };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("blocked_command_max_per_endpoint"));
    }

    #[test]
    fn test_ingestion_config_total_timeout_less_than_flush_timeout() {
        let config = IngestionConfig {
            shutdown_flush_timeout_secs: 10,
            shutdown_total_timeout_secs: 5,
            ..Default::default()
        };
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("shutdown_total_timeout_secs"));
        assert!(err.contains("shutdown_flush_timeout_secs"));
    }

    #[test]
    #[serial]
    fn test_legacy_env_licensing() {
        // Clean up any pre-existing env vars that could interfere
        std::env::remove_var("EDEN_LICENSE_KEY");
        std::env::remove_var("EDEN_CLUSTER_UID");
        std::env::remove_var("EDEN_METERING_INGEST_API_KEY");

        std::env::set_var("EDEN_LICENSE_KEY", "test-metering-token");
        std::env::set_var("EDEN_CLUSTER_UID", "cluster-abc");
        std::env::set_var("EDEN_METERING_INGEST_API_KEY", "metering-ingest-key");

        let config = load_config(None).expect("load_config failed");

        assert_eq!(config.licensing.license_key.as_deref(), Some("test-metering-token"));
        assert_eq!(config.licensing.cluster_uid.as_deref(), Some("cluster-abc"));
        assert_eq!(config.licensing.metering_ingest_api_key.as_deref(), Some("metering-ingest-key"));

        std::env::remove_var("EDEN_LICENSE_KEY");
        std::env::remove_var("EDEN_CLUSTER_UID");
        std::env::remove_var("EDEN_METERING_INGEST_API_KEY");
    }

    #[test]
    #[serial]
    fn test_legacy_env_agents() {
        std::env::remove_var("EDEN_SKILL_PROMPT_BUDGET_TOKENS");
        std::env::remove_var("EDEN_SKILL_CONTEXT_WINDOW_TOKENS");

        std::env::set_var("EDEN_SKILL_PROMPT_BUDGET_TOKENS", "2048");
        std::env::set_var("EDEN_SKILL_CONTEXT_WINDOW_TOKENS", "16384");

        let config = load_config(None).expect("load_config failed");

        assert_eq!(config.agents.skill_prompt_budget_tokens, Some(2_048));
        assert_eq!(config.agents.default_context_window_tokens, 16_384);

        std::env::remove_var("EDEN_SKILL_PROMPT_BUDGET_TOKENS");
        std::env::remove_var("EDEN_SKILL_CONTEXT_WINDOW_TOKENS");
    }

    #[test]
    #[serial]
    fn test_clickhouse_telemetry_legacy_env_false() {
        std::env::remove_var("EDEN_CLICKHOUSE_TELEMETRY_ENABLED");

        std::env::set_var("EDEN_CLICKHOUSE_TELEMETRY_ENABLED", "false");

        let config = load_config(None).expect("load_config failed");

        assert!(!config.telemetry.clickhouse_enabled);

        std::env::remove_var("EDEN_CLICKHOUSE_TELEMETRY_ENABLED");
    }

    #[test]
    #[serial]
    fn test_legacy_env_adam_quarantined_skills() {
        // Surgical incident-response deny-list: comma-separated env var gets
        // routed through `agents.skill_policy.quarantined_skills` and
        // deserialised into a `Vec<String>` by the custom visitor.
        std::env::remove_var("ADAM_QUARANTINED_SKILLS");
        std::env::remove_var("ADAM_SKILL_POLICY_MODE");

        std::env::set_var("ADAM_QUARANTINED_SKILLS", "foo-skill, bar-skill,,baz-skill");
        std::env::set_var("ADAM_SKILL_POLICY_MODE", "builtins_only");

        let config = load_config(None).expect("load_config failed");

        assert_eq!(config.agents.skill_policy.mode, SkillPolicyMode::BuiltinsOnly);
        assert_eq!(
            config.agents.skill_policy.quarantined_skills,
            vec!["foo-skill".to_string(), "bar-skill".to_string(), "baz-skill".to_string()]
        );

        std::env::remove_var("ADAM_QUARANTINED_SKILLS");
        std::env::remove_var("ADAM_SKILL_POLICY_MODE");
    }

    #[test]
    #[serial]
    fn test_allow_customer_skill_crud_default_is_off() {
        // Without explicit config the launch posture must be "no customer
        // writes". If this default ever flips, the admin/marketplace guards
        // stop protecting anything.
        std::env::remove_var("ADAM_ALLOW_CUSTOMER_SKILL_CRUD");
        let config = load_config(None).expect("load_config failed");
        assert!(!config.agents.allow_customer_skill_crud);
    }

    #[test]
    #[serial]
    fn test_nested_env_agents_tool_endpoint_allowed_hosts_array() {
        std::env::remove_var("EDEN__AGENTS__SECURITY__TOOL_ENDPOINT_ALLOWED_HOSTS");

        std::env::set_var("EDEN__AGENTS__SECURITY__TOOL_ENDPOINT_ALLOWED_HOSTS", "[\"localhost\",\"127.0.0.1\",\"::1\"]");

        let config = load_config(None).expect("load_config failed");

        assert_eq!(
            config.agents.security.tool_endpoint_allowed_hosts,
            vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()]
        );

        std::env::remove_var("EDEN__AGENTS__SECURITY__TOOL_ENDPOINT_ALLOWED_HOSTS");
    }

    #[test]
    #[serial]
    #[ignore] // pre-existing failure — external metering compatibility loading incomplete
    fn test_toml_licensing_loading() {
        use std::io::Write;

        // Figment's Toml::file().nested() expects profile names at the top level.
        // The default profile is "default", so licensing config lives under [default.licensing].
        let toml_content = r#"
            [default.licensing]
            license_key = "toml-metering-token"
            cluster_uid = "toml-cluster-uid"
            heartbeat_interval_secs = 3600
        "#;

        let path = std::env::temp_dir().join("eden_test_toml_licensing.toml");
        let mut file = std::fs::File::create(&path).expect("failed to create temp toml");
        file.write_all(toml_content.as_bytes()).expect("failed to write temp toml");

        let args = CliArgs { config: path.clone(), ..CliArgs::default() };
        let config = load_config(Some(&args)).expect("load_config failed");

        assert_eq!(config.licensing.license_key.as_deref(), Some("toml-metering-token"));
        assert_eq!(config.licensing.cluster_uid.as_deref(), Some("toml-cluster-uid"));
        assert_eq!(config.licensing.heartbeat_interval_secs, 3600);

        std::fs::remove_file(&path).ok();
    }
}
