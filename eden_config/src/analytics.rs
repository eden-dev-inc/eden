//! Analytics pipeline configuration: sampling, audit, stream, ingestion, metadata, and Redis analytics.
//!
//! Maps to the `[analytics]` section in `eden.toml`. Contains the full hierarchy
//! of sub-configs (burst, always-on, force-sample, discovery, divergence, etc.).

use crate::ConfigError;
use serde::{Deserialize, Serialize};

/// Default values for analytics sampling and ingestion.
pub mod defaults {
    pub const BATCH_SIZE: usize = 5_000;
    pub const MAX_BATCH_SIZE: usize = 50_000;
    pub const CHANNEL_CAPACITY: usize = 32_768;
    pub const FLUSH_INTERVAL_MS: u64 = 1_000;
    pub const RATE_ADJUST_INTERVAL_SECS: f64 = 5.0;
    pub const TABLE_INIT_MAX_FAILURES: u32 = 5;
    pub const TABLE_INIT_MAX_BACKOFF_SECS: u64 = 60;
    pub const CIRCUIT_BREAKER_MAX_FAILURES: u32 = 5;
    pub const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 30;

    pub const BATCH_INSERT_MAX_RETRIES: u32 = 3;
    pub const BATCH_INSERT_INITIAL_BACKOFF_MS: u64 = 100;
    pub const DEAD_LETTER_MAX_EVENTS: usize = 250_000;
    pub const HOT_KEY_LIMIT: usize = 10;
    pub const KEY_PATTERN_LIMIT: usize = 10;
    pub const SLOW_COMMAND_LIMIT: usize = 5;
    pub const WIRE_METRICS_FLUSH_INTERVAL_SECS: u64 = 60;
    pub const MAX_HOT_KEYS_TRACKED: usize = 10_000;
    pub const MAX_KEY_PATTERNS_TRACKED: usize = 1_000;

    // Burst capture settings (anomaly-triggered)
    pub const BURST_ENABLED: bool = true;
    pub const BURST_WINDOW_DURATION_SECS: u64 = 30;
    pub const BURST_COOLDOWN_SECS: u64 = 300;
    pub const BURST_MAX_REQUESTS_PER_WINDOW: usize = 1_000;
    pub const BURST_MAX_CONCURRENT_WINDOWS: usize = 10;
    pub const BURST_TRACK_SEQUENCES: bool = true;

    // Always-on analysis settings
    pub const ALWAYS_ON_PII_DETECTION: bool = false;
    pub const ALWAYS_ON_DANGEROUS_COMMANDS: bool = true;
    pub const ALWAYS_ON_SLOW_QUERY_THRESHOLD_US: u64 = 10_000;
    pub const ALWAYS_ON_ERROR_TRACKING: bool = true;

    // Force sampling settings
    pub const FORCE_SAMPLE_ON_ERROR: bool = true;
    pub const FORCE_SAMPLE_ON_SLOW_QUERY: bool = true;
    pub const FORCE_SAMPLE_ON_DANGEROUS_COMMAND: bool = true;
    pub const FORCE_SAMPLE_ON_WRITE_COMMAND: bool = false;

    pub const MAX_FORCE_SAMPLES_PER_WINDOW: u64 = 10_000;

    // Discovery window settings (timer-triggered, for template reverse-engineering)
    pub const DISCOVERY_ENABLED: bool = true;
    pub const DISCOVERY_WINDOW_DURATION_SECS: u64 = 10;
    pub const DISCOVERY_INTERVAL_SECS: u64 = 300;
    pub const DISCOVERY_COOLDOWN_SECS: u64 = 300;
    pub const DISCOVERY_MAX_REQUESTS_PER_WINDOW: usize = 500;
    pub const DISCOVERY_MAX_CONCURRENT_WINDOWS: usize = 5;

    // Aggregate anti-pattern detection thresholds
    pub const AGG_ANTI_PATTERN_LARGE_RESPONSE_BYTES: u64 = 65_536;
    pub const AGG_ANTI_PATTERN_HIGH_FANOUT_THRESHOLD: u64 = 100;
    pub const AGG_ANTI_PATTERN_HIGH_ERROR_RATE: f64 = 0.10;
    pub const AGG_ANTI_PATTERN_HIGH_LATENCY_VARIANCE_RATIO: f64 = 1.5;
    pub const AGG_ANTI_PATTERN_HIGH_LATENCY_VARIANCE_MIN_REQUESTS: u64 = 100;
    pub const AGG_ANTI_PATTERN_NO_TTL_RATIO: f64 = 0.80;
    // Postgres anti-pattern detection thresholds
    pub const POSTGRES_ANTI_PATTERN_NPLUS_THRESHOLD: usize = 5;
    pub const POSTGRES_ANTI_PATTERN_EXCESSIVE_SUBQUERY_THRESHOLD: usize = 3;
    pub const POSTGRES_ANTI_PATTERN_DETECT_SELECT_STAR: bool = true;
    pub const POSTGRES_ANTI_PATTERN_EXCESSIVE_JOINS_THRESHOLD: usize = 4;

    // Divergence anomaly detection settings
    pub const DIVERGENCE_ENABLED: bool = true;
    pub const DIVERGENCE_WINDOW_SECS: u64 = 10;
    pub const DIVERGENCE_EWMA_ALPHA: f64 = 0.01;
    pub const DIVERGENCE_DIRICHLET_BETA: f64 = 50.0;
    pub const DIVERGENCE_WARN_THRESHOLD: f64 = 0.02;
    pub const DIVERGENCE_CRITICAL_THRESHOLD: f64 = 0.05;
    pub const DIVERGENCE_CONFIRM_WINDOWS: u32 = 3;
    pub const DIVERGENCE_WATCH_MULTIPLIER: f64 = 2.0;
    pub const DIVERGENCE_CONFIRMED_MULTIPLIER: f64 = 5.0;

    pub const AUDIT_FLUSH_INTERVAL_SECS: u64 = 10;
    pub const STREAM_ENABLED: bool = false;
    pub const STREAM_INTERVAL_SECS: u64 = 15;

    // Recommendation engine thresholds
    pub const REC_MIN_OBSERVATION_WINDOWS: u64 = 30;
    pub const REC_MIN_TOTAL_REQUESTS: u64 = 500;
    pub const REC_WRITE_HEAVY_RATIO: f64 = 0.95;
    pub const REC_WRITE_HEAVY_MIN_READS_PER_DAY: u64 = 10;
    pub const REC_NO_TTL_COVERAGE_THRESHOLD: f64 = 0.1;
    pub const REC_NO_TTL_MIN_WRITES: u64 = 1000;
    pub const REC_STALE_DAYS: u64 = 7;
    pub const REC_OVERSIZED_VALUE_BYTES: u64 = 10_240;
    pub const REC_OVERSIZED_MIN_REQUESTS: u64 = 500;
    pub const REC_HOT_KEY_PCT_THRESHOLD: f64 = 0.15;
    pub const REC_HIGH_ERROR_RATE: f64 = 0.05;
    pub const REC_EXPENSIVE_READ_MAX_WRITE_RATIO: f64 = 0.2;
    pub const REC_EXPENSIVE_READ_LATENCY_US: u64 = 10_000;
    pub const REC_MISSING_PIPELINE_MIN_READS_PER_WINDOW: u64 = 50;
    pub const REC_LARGE_HASH_FETCH_MIN_VALUE_BYTES: u64 = 5120;
    pub const REC_DANGEROUS_COMMAND_MIN_OCCURRENCES: u64 = 5;
    pub const REC_PII_MIN_DETECTIONS: u64 = 10;
    pub const REC_READ_REPLICA_MIN_REQUESTS: u64 = 100_000;
    pub const REC_READ_REPLICA_READ_RATIO_THRESHOLD: f64 = 0.70;
    pub const REC_HIGH_FANOUT_MIN_OCCURRENCES: u64 = 10;
    pub const REC_RAM_PRICE_PER_GB_MONTHLY: f64 = 5.0;
    pub const REC_ERROR_NOTE_MIN_RATE: f64 = 0.01;
    pub const REC_MAX_RECOMMENDATIONS_PER_RULE: usize = 10;
    pub const REC_CACHE_MISS_HOTSPOT_MAX_HIT_RATE: f64 = 0.80;
    pub const REC_CACHE_MISS_HOTSPOT_MIN_REQUESTS: u64 = 500;
    pub const REC_REDIRECT_STORM_RATIO: f64 = 0.05;
    pub const REC_REDIRECT_STORM_MIN_REQUESTS: u64 = 500;
    pub const REC_COMMAND_COST_OUTLIER_RATIO: f64 = 2.0;
    pub const REC_COMMAND_COST_OUTLIER_MIN_REQUESTS: u64 = 500;
    pub const REC_ERROR_CATEGORY_CONCENTRATION_RATIO: f64 = 0.90;
    pub const REC_ERROR_CATEGORY_CONCENTRATION_MIN_ERRORS: u64 = 25;
    pub const MONGO_REC_HIGH_VARIANCE_LATENCY_RATIO: f64 = 1.5;
    pub const MONGO_REC_HIGH_VARIANCE_LATENCY_MIN_REQUESTS: u64 = 200;

    // Ingestion loop settings
    pub const ROLLUP_FLUSH_INTERVAL_SECS: u64 = 60;
    pub const LIVE_FLUSH_TIMEOUT_SECS: u64 = 5;
    pub const SHUTDOWN_FLUSH_TIMEOUT_SECS: u64 = 5;
    pub const SHUTDOWN_TOTAL_TIMEOUT_SECS: u64 = 30;
    pub const BLOCKED_COMMAND_MAX_PER_ENDPOINT: usize = 10_000;
}

/// Top-level analytics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AnalyticsConfig {
    /// Sampling configuration for analytics pipeline.
    pub sampling: SamplingConfig,
    /// Audit trail configuration.
    pub audit: AuditConfig,
    /// Real-time stream configuration.
    pub stream: StreamConfig,
    pub ingestion: IngestionConfig,
    pub metadata: MetadataCollectionConfig,
    /// Redis-specific analytics config (recommendations, anti-pattern thresholds).
    pub redis: RedisAnalyticsConfig,
    /// Postgres-specific analytics config (recommendations, anti-pattern thresholds).
    pub postgres: PostgresAnalyticsConfig,
    /// MongoDB-specific analytics config (profiling and recommendations).
    #[serde(default)]
    pub mongo: MongoAnalyticsConfig,
}

impl AnalyticsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.sampling.validate()?;
        if self.audit.flush_interval_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.audit.flush_interval_secs must be > 0".into()));
        }
        if self.stream.enabled && self.stream.interval_secs == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.stream.interval_secs must be > 0 when stream is enabled".into(),
            ));
        }
        self.redis.validate()?;
        self.postgres.validate()?;
        self.mongo.validate()?;
        self.ingestion.validate()?;
        self.metadata.validate()?;
        Ok(())
    }
}

/// Redis-specific analytics configuration.
///
/// Lives under `[analytics.redis]` in eden.toml. Other database analyzers
/// (MongoDB, DynamoDB, etc.) get their own sibling section and struct.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RedisAnalyticsConfig {
    pub recommendations: RecommendationConfig,
    pub anti_patterns: RedisAggAntiPatternsConfig,
}

impl RedisAnalyticsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.recommendations.validate_for("analytics.redis.recommendations")?;
        self.anti_patterns.validate_for("analytics.redis.anti_patterns")?;
        Ok(())
    }
}

/// Postgres-specific analytics configuration.
///
/// Lives under `[analytics.postgres]` in eden.toml.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PostgresAnalyticsConfig {
    pub recommendations: RecommendationConfig,
    pub anti_patterns: PostgresAntiPatternsConfig,
    /// Aggregate (60-second window) anti-pattern thresholds for PostgreSQL endpoints.
    /// Controls large_response, high_error_rate and dangerous_command detection.
    pub agg_anti_patterns: AggAntiPatternsConfig,
}

impl PostgresAnalyticsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.recommendations.validate_for("analytics.postgres.recommendations")?;
        self.anti_patterns.validate()?; // PostgresAntiPatternsConfig has its own validate()
        self.agg_anti_patterns.validate_for("analytics.postgres.agg_anti_patterns")?;
        Ok(())
    }
}

/// Postgres anti-pattern detection thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresAntiPatternsConfig {
    /// N+1 detection threshold (min repeated queries to flag).
    pub nplus_threshold: usize,
    /// Excessive subquery count threshold.
    pub excessive_subquery_threshold: usize,
    /// Whether to detect SELECT * patterns.
    pub detect_select_star: bool,
    /// Maximum join count before flagging as excessive.
    pub excessive_joins_threshold: usize,
}

impl Default for PostgresAntiPatternsConfig {
    fn default() -> Self {
        Self {
            nplus_threshold: defaults::POSTGRES_ANTI_PATTERN_NPLUS_THRESHOLD,
            excessive_subquery_threshold: defaults::POSTGRES_ANTI_PATTERN_EXCESSIVE_SUBQUERY_THRESHOLD,
            detect_select_star: defaults::POSTGRES_ANTI_PATTERN_DETECT_SELECT_STAR,
            excessive_joins_threshold: defaults::POSTGRES_ANTI_PATTERN_EXCESSIVE_JOINS_THRESHOLD,
        }
    }
}

impl PostgresAntiPatternsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.nplus_threshold == 0 {
            return Err(ConfigError::InvalidValue("analytics.postgres.anti_patterns.nplus_threshold must be > 0".into()));
        }
        if self.excessive_subquery_threshold == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.postgres.anti_patterns.excessive_subquery_threshold must be > 0".into(),
            ));
        }
        if self.excessive_joins_threshold == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.postgres.anti_patterns.excessive_joins_threshold must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// MongoDB profiling management mode.
///
/// Controls whether Eden manages the MongoDB profiling level:
/// - `disabled`: Eden does not touch profiling; profiling-dependent collectors
///   are skipped at registration time (current default behavior).
/// - `level1`: Eden sets `{ profile: 1, slowms: <profiling_slow_ms> }` each
///   metadata cycle; profiling-dependent collectors are registered.
/// - `level2`: Eden sets `{ profile: 2, slowms: <profiling_slow_ms> }` each
///   metadata cycle; all collectors are registered.
/// - `dynamic`: Profiling is toggled on/off based on anomaly escalation level.
///   Collectors are registered (same as level1) since profiling may be active
///   during escalation. The runtime check skips collectors when profiling
///   is off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MongoProfilingMode {
    #[default]
    Disabled,
    Level1,
    Level2,
    Dynamic,
}

/// MongoDB-specific analytics configuration.
///
/// Lives under `[analytics.mongo]` in eden.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MongoAnalyticsConfig {
    /// Profiling management mode.
    pub profiling: MongoProfilingMode,
    /// Slow operation threshold in milliseconds passed to `{ profile: N, slowms: M }`.
    pub profiling_slow_ms: u64,
    /// Recommendation engine thresholds.
    #[serde(default)]
    pub recommendations: MongoRecommendationConfig,
    /// Aggregate (60-second window) anti-pattern thresholds for MongoDB endpoints.
    /// Controls large_response, high_error_rate and dangerous_command detection.
    #[serde(default)]
    pub agg_anti_patterns: AggAntiPatternsConfig,
    /// Per-request anti-pattern detection thresholds.
    #[serde(default)]
    pub anti_patterns: MongoAntiPatternsConfig,
}

impl Default for MongoAnalyticsConfig {
    fn default() -> Self {
        Self {
            profiling: MongoProfilingMode::default(),
            profiling_slow_ms: 100,
            recommendations: MongoRecommendationConfig::default(),
            agg_anti_patterns: AggAntiPatternsConfig::default(),
            anti_patterns: MongoAntiPatternsConfig::default(),
        }
    }
}

impl MongoAnalyticsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.recommendations.validate()?;
        self.agg_anti_patterns.validate_for("analytics.mongo.agg_anti_patterns")?;
        self.anti_patterns.validate()?;
        Ok(())
    }
}

/// MongoDB per-request anti-pattern detection thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MongoAntiPatternsConfig {
    /// Skip value threshold for large-offset pagination detection.
    pub large_skip_threshold: u64,
    /// $in/$nin array length threshold for large-array detection.
    pub large_in_array_threshold: u32,
    /// Repeated single-key read count to flag N+1 pattern per connection window.
    pub n_plus_one_threshold: u32,
    /// Minimum latency in microseconds before flagging missing maxTimeMS.
    pub no_max_time_ms_latency_threshold_us: u64,
}

impl Default for MongoAntiPatternsConfig {
    fn default() -> Self {
        Self {
            large_skip_threshold: 1_000,
            large_in_array_threshold: 100,
            n_plus_one_threshold: 3,
            no_max_time_ms_latency_threshold_us: 100_000,
        }
    }
}

impl MongoAntiPatternsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.n_plus_one_threshold <= 1 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.anti_patterns.n_plus_one_threshold must be > 1 \
                 (1 would flag every single read as N+1)"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Mongo recommendation thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoRecommendationConfig {
    #[serde(default)]
    pub min_observation_windows: u64,
    #[serde(default)]
    pub min_shape_requests: u64,
    #[serde(default)]
    pub min_confidence: f64,
    #[serde(default)]
    pub max_recommendations_per_rule: usize,
    #[serde(default)]
    pub cpu_price_per_core_hour_usd: f64,
    #[serde(default)]
    pub max_ms_saved_per_request: f64,
    #[serde(default)]
    pub heuristic_confidence_multiplier: f64,
    #[serde(default)]
    pub hot_shape_share_threshold: f64,
    #[serde(default)]
    pub index_candidate_min_avg_latency_us: u64,
    #[serde(default)]
    pub covered_projection_max_fields: usize,
    #[serde(default)]
    pub large_skip_threshold: u64,
    #[serde(default)]
    pub anti_pattern_min_occurrences: u64,
    #[serde(default)]
    pub no_max_time_ms_min_occurrences: u64,
    #[serde(default)]
    pub no_write_concern_min_occurrences: u64,
    #[serde(default)]
    pub majority_without_wtimeout_min_occurrences: u64,
    #[serde(default)]
    pub connection_pressure_ratio: f64,
    #[serde(default)]
    pub connection_pressure_min_snapshots: u64,
    #[serde(default)]
    pub lock_contention_ratio_threshold: f64,
    #[serde(default)]
    pub lock_contention_min_snapshots: u64,
    #[serde(default)]
    pub index_reduction_ratio: f64,
    #[serde(default)]
    pub sort_index_reduction_ratio: f64,
    #[serde(default)]
    pub covered_reduction_ratio: f64,
    #[serde(default)]
    pub pagination_reduction_ratio: f64,
    #[serde(default)]
    pub regex_reduction_ratio: f64,
    #[serde(default)]
    pub n_plus_one_ms_saved_per_req: f64,
    #[serde(default)]
    pub lookup_without_match_ms_saved: f64,
    #[serde(default)]
    pub late_match_ms_saved: f64,
    #[serde(default)]
    pub connection_pool_tuning_reduction_ratio: f64,
    #[serde(default)]
    pub dangerous_command_min_occurrences: u64,
    #[serde(default)]
    pub high_error_rate: f64,
    #[serde(default)]
    pub lock_contention_reduction_ratio: f64,
    #[serde(default)]
    pub high_variance_latency_ratio: f64,
    #[serde(default)]
    pub high_variance_latency_min_requests: u64,
}

impl Default for MongoRecommendationConfig {
    fn default() -> Self {
        Self {
            min_observation_windows: 12,
            min_shape_requests: 200,
            min_confidence: 0.5,
            max_recommendations_per_rule: 10,
            cpu_price_per_core_hour_usd: 0.08,
            max_ms_saved_per_request: 25.0,
            heuristic_confidence_multiplier: 0.75,
            hot_shape_share_threshold: 0.05,
            index_candidate_min_avg_latency_us: 2_000,
            covered_projection_max_fields: 6,
            large_skip_threshold: 1_000,
            anti_pattern_min_occurrences: 10,
            no_max_time_ms_min_occurrences: 20,
            no_write_concern_min_occurrences: 20,
            majority_without_wtimeout_min_occurrences: 5,
            connection_pressure_ratio: 0.85,
            connection_pressure_min_snapshots: 3,
            lock_contention_ratio_threshold: 0.20,
            lock_contention_min_snapshots: 3,
            index_reduction_ratio: 0.35,
            sort_index_reduction_ratio: 0.25,
            covered_reduction_ratio: 0.20,
            pagination_reduction_ratio: 0.30,
            regex_reduction_ratio: 0.30,
            n_plus_one_ms_saved_per_req: 2.0,
            lookup_without_match_ms_saved: 8.0,
            late_match_ms_saved: 10.0,
            connection_pool_tuning_reduction_ratio: 0.15,
            lock_contention_reduction_ratio: 0.12,
            dangerous_command_min_occurrences: 5,
            high_error_rate: 0.05,
            high_variance_latency_ratio: defaults::MONGO_REC_HIGH_VARIANCE_LATENCY_RATIO,
            high_variance_latency_min_requests: defaults::MONGO_REC_HIGH_VARIANCE_LATENCY_MIN_REQUESTS,
        }
    }
}

impl MongoRecommendationConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.min_observation_windows == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.recommendations.min_observation_windows must be > 0".into(),
            ));
        }
        if self.max_recommendations_per_rule == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.recommendations.max_recommendations_per_rule must be > 0".into(),
            ));
        }
        let ratios: &[(&str, f64)] = &[
            ("min_confidence", self.min_confidence),
            ("hot_shape_share_threshold", self.hot_shape_share_threshold),
            ("connection_pressure_ratio", self.connection_pressure_ratio),
            ("lock_contention_ratio_threshold", self.lock_contention_ratio_threshold),
            ("high_error_rate", self.high_error_rate),
        ];
        for &(name, value) in ratios {
            if !(0.0..=1.0).contains(&value) {
                return Err(ConfigError::InvalidValue(format!(
                    "analytics.mongo.recommendations.{name} must be in 0.0..=1.0, got {value}"
                )));
            }
        }
        if self.cpu_price_per_core_hour_usd < 0.0 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.recommendations.cpu_price_per_core_hour_usd must be >= 0".into(),
            ));
        }
        if self.high_variance_latency_ratio <= 0.0 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.recommendations.high_variance_latency_ratio must be > 0".into(),
            ));
        }
        if self.high_variance_latency_min_requests == 0 {
            return Err(ConfigError::InvalidValue(
                "analytics.mongo.recommendations.high_variance_latency_min_requests must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// Top-level sampling configuration.
///
/// The analytics pipeline uses anomaly-driven sampling:
/// - Aggregates are collected for 100% of requests (in-memory)
/// - Force-sampling captures errors, slow queries, dangerous commands and write commands
/// - Burst capture is triggered by anomaly detection (JS divergence, latency spikes, error rate)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SamplingConfig {
    /// Burst capture settings (anomaly-triggered windows).
    pub burst: BurstConfig,
    /// Always-on analysis settings.
    pub always_on: AlwaysOnConfig,
    /// Force sampling settings.
    pub force_sample: ForceSampleConfig,
    /// JS divergence anomaly detection settings.
    pub divergence: DivergenceConfig,
    /// Discovery window settings (timer-triggered, for template reverse-engineering).
    pub discovery: DiscoveryConfig,
}

impl SamplingConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.burst.validate()?;
        self.always_on.validate()?;
        self.force_sample.validate()?;
        self.discovery.validate()?;
        self.divergence.validate()?;
        Ok(())
    }
}

/// Burst capture configuration (anomaly-triggered).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BurstConfig {
    /// Enable burst capture windows.
    pub enabled: bool,
    /// Duration of each burst window in seconds.
    pub window_duration_secs: u64,
    /// Cooldown between burst windows per endpoint in seconds.
    pub cooldown_secs: u64,
    /// Maximum requests captured per window.
    pub max_requests_per_window: usize,
    /// Maximum concurrent burst windows across all endpoints.
    pub max_concurrent_windows: usize,
    /// Track per-connection sequences during a burst window.
    pub track_sequences: bool,
}

impl Default for BurstConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::BURST_ENABLED,
            window_duration_secs: defaults::BURST_WINDOW_DURATION_SECS,
            cooldown_secs: defaults::BURST_COOLDOWN_SECS,
            max_requests_per_window: defaults::BURST_MAX_REQUESTS_PER_WINDOW,
            max_concurrent_windows: defaults::BURST_MAX_CONCURRENT_WINDOWS,
            track_sequences: defaults::BURST_TRACK_SEQUENCES,
        }
    }
}

impl BurstConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.window_duration_secs == 0 {
            return Err(ConfigError::InvalidValue("burst.window_duration_secs must be > 0".into()));
        }
        if self.enabled && self.cooldown_secs == 0 {
            return Err(ConfigError::InvalidValue("burst.cooldown_secs must be > 0".into()));
        }
        if self.enabled && self.max_requests_per_window == 0 {
            return Err(ConfigError::InvalidValue("burst.max_requests_per_window must be > 0".into()));
        }
        Ok(())
    }
}

/// Always-on analysis configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AlwaysOnConfig {
    /// Enable PII detection for keys.
    pub pii_detection: bool,
    /// Enable dangerous command detection.
    pub dangerous_commands: bool,
    /// Slow query threshold in microseconds.
    pub slow_query_threshold_us: u64,
    /// Enable error tracking.
    pub error_tracking: bool,
}

impl Default for AlwaysOnConfig {
    fn default() -> Self {
        Self {
            pii_detection: defaults::ALWAYS_ON_PII_DETECTION,
            dangerous_commands: defaults::ALWAYS_ON_DANGEROUS_COMMANDS,
            slow_query_threshold_us: defaults::ALWAYS_ON_SLOW_QUERY_THRESHOLD_US,
            error_tracking: defaults::ALWAYS_ON_ERROR_TRACKING,
        }
    }
}

impl AlwaysOnConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        // slow_query_threshold_us = 0 is valid (disabled).
        // Boolean fields have no invalid states.
        Ok(())
    }
}

/// Force sampling configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ForceSampleConfig {
    /// Force sampling on errors.
    pub on_error: bool,
    /// Force sampling on slow queries.
    pub on_slow_query: bool,
    /// Force sampling on dangerous commands.
    pub on_dangerous_command: bool,
    /// Force sampling on write commands.
    pub on_write_command: bool,
}

impl Default for ForceSampleConfig {
    fn default() -> Self {
        Self {
            on_error: defaults::FORCE_SAMPLE_ON_ERROR,
            on_slow_query: defaults::FORCE_SAMPLE_ON_SLOW_QUERY,
            on_dangerous_command: defaults::FORCE_SAMPLE_ON_DANGEROUS_COMMAND,
            on_write_command: defaults::FORCE_SAMPLE_ON_WRITE_COMMAND,
        }
    }
}

impl ForceSampleConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Boolean fields have no invalid states.
        Ok(())
    }
}

/// Discovery window configuration (timer-triggered, for template reverse-engineering).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// Enable periodic discovery windows.
    pub enabled: bool,
    /// Duration of each discovery window in seconds.
    pub window_duration_secs: u64,
    /// Interval between discovery windows per endpoint in seconds.
    pub interval_secs: u64,
    /// Cooldown after a discovery window completes, per endpoint, in seconds.
    pub cooldown_secs: u64,
    /// Maximum requests captured per discovery window.
    pub max_requests_per_window: usize,
    /// Maximum concurrent discovery windows across all endpoints.
    pub max_concurrent_windows: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::DISCOVERY_ENABLED,
            window_duration_secs: defaults::DISCOVERY_WINDOW_DURATION_SECS,
            interval_secs: defaults::DISCOVERY_INTERVAL_SECS,
            cooldown_secs: defaults::DISCOVERY_COOLDOWN_SECS,
            max_requests_per_window: defaults::DISCOVERY_MAX_REQUESTS_PER_WINDOW,
            max_concurrent_windows: defaults::DISCOVERY_MAX_CONCURRENT_WINDOWS,
        }
    }
}

impl DiscoveryConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.window_duration_secs == 0 {
            return Err(ConfigError::InvalidValue("discovery.window_duration_secs must be > 0".into()));
        }
        if self.enabled && self.interval_secs == 0 {
            return Err(ConfigError::InvalidValue("discovery.interval_secs must be > 0".into()));
        }
        if self.enabled && self.cooldown_secs == 0 {
            return Err(ConfigError::InvalidValue("discovery.cooldown_secs must be > 0".into()));
        }
        if self.enabled && self.max_requests_per_window == 0 {
            return Err(ConfigError::InvalidValue("discovery.max_requests_per_window must be > 0".into()));
        }
        Ok(())
    }
}

/// JS divergence anomaly detection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DivergenceConfig {
    pub enabled: bool,
    pub window_secs: u64,
    pub ewma_alpha: f64,
    pub dirichlet_beta: f64,
    pub warn_threshold: f64,
    pub critical_threshold: f64,
    pub confirm_windows: u32,
    pub watch_multiplier: f64,
    pub confirmed_multiplier: f64,
}

impl Default for DivergenceConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::DIVERGENCE_ENABLED,
            window_secs: defaults::DIVERGENCE_WINDOW_SECS,
            ewma_alpha: defaults::DIVERGENCE_EWMA_ALPHA,
            dirichlet_beta: defaults::DIVERGENCE_DIRICHLET_BETA,
            warn_threshold: defaults::DIVERGENCE_WARN_THRESHOLD,
            critical_threshold: defaults::DIVERGENCE_CRITICAL_THRESHOLD,
            confirm_windows: defaults::DIVERGENCE_CONFIRM_WINDOWS,
            watch_multiplier: defaults::DIVERGENCE_WATCH_MULTIPLIER,
            confirmed_multiplier: defaults::DIVERGENCE_CONFIRMED_MULTIPLIER,
        }
    }
}

impl DivergenceConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.window_secs == 0 {
            return Err(ConfigError::InvalidValue("divergence.window_secs must be > 0 when enabled".into()));
        }
        if !(0.0..1.0).contains(&self.ewma_alpha) {
            return Err(ConfigError::InvalidValue(format!(
                "divergence.ewma_alpha must be in (0.0, 1.0), got {}",
                self.ewma_alpha
            )));
        }
        if self.dirichlet_beta <= 0.0 {
            return Err(ConfigError::InvalidValue(format!(
                "divergence.dirichlet_beta must be > 0, got {}",
                self.dirichlet_beta
            )));
        }
        if self.warn_threshold > self.critical_threshold {
            return Err(ConfigError::InvalidValue(format!(
                "divergence.warn_threshold ({}) must be <= critical_threshold ({})",
                self.warn_threshold, self.critical_threshold
            )));
        }
        if self.watch_multiplier < 1.0 {
            return Err(ConfigError::InvalidValue(format!(
                "divergence.watch_multiplier must be >= 1.0, got {}",
                self.watch_multiplier
            )));
        }
        if self.confirmed_multiplier < 1.0 {
            return Err(ConfigError::InvalidValue(format!(
                "divergence.confirmed_multiplier must be >= 1.0, got {}",
                self.confirmed_multiplier
            )));
        }
        Ok(())
    }
}

/// Aggregate anti-pattern detection thresholds shared across all protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AggAntiPatternsConfig {
    /// Response size threshold in bytes.
    pub large_response_bytes: u64,
    /// Error rate threshold (0.0–1.0).
    pub high_error_rate: f64,
    /// Coefficient of variation threshold (`stddev / mean`) for per-command latency.
    pub high_latency_variance_ratio: f64,
    /// Minimum requests for a command before `high_latency_variance` can fire.
    pub high_latency_variance_min_requests: u64,
}

impl Default for AggAntiPatternsConfig {
    fn default() -> Self {
        Self {
            large_response_bytes: defaults::AGG_ANTI_PATTERN_LARGE_RESPONSE_BYTES,
            high_error_rate: defaults::AGG_ANTI_PATTERN_HIGH_ERROR_RATE,
            high_latency_variance_ratio: defaults::AGG_ANTI_PATTERN_HIGH_LATENCY_VARIANCE_RATIO,
            high_latency_variance_min_requests: defaults::AGG_ANTI_PATTERN_HIGH_LATENCY_VARIANCE_MIN_REQUESTS,
        }
    }
}

impl AggAntiPatternsConfig {
    /// Validate with a caller-supplied config path prefix for error messages.
    pub fn validate_for(&self, path: &str) -> Result<(), ConfigError> {
        if self.large_response_bytes == 0 {
            return Err(ConfigError::InvalidValue(format!("{path}.large_response_bytes must be > 0")));
        }
        if !(0.0..=1.0).contains(&self.high_error_rate) {
            return Err(ConfigError::InvalidValue(format!(
                "{path}.high_error_rate must be in 0.0..=1.0, got {}",
                self.high_error_rate
            )));
        }
        if self.high_latency_variance_ratio <= 0.0 {
            return Err(ConfigError::InvalidValue(format!(
                "{path}.high_latency_variance_ratio must be > 0, got {}",
                self.high_latency_variance_ratio
            )));
        }
        if self.high_latency_variance_min_requests == 0 {
            return Err(ConfigError::InvalidValue(format!("{path}.high_latency_variance_min_requests must be > 0")));
        }
        Ok(())
    }
}

/// Redis-specific aggregate anti-pattern thresholds.
///
/// Extends the shared config with Redis-only fields (high fanout, no TTL).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisAggAntiPatternsConfig {
    /// Shared thresholds (large_response_bytes, high_error_rate).
    #[serde(flatten)]
    pub base: AggAntiPatternsConfig,
    /// Average target count threshold for MGET/MSET (high fanout).
    pub high_fanout_threshold: u64,
    /// Ratio of sampled writes without TTL to flag (0.0-1.0).
    pub no_ttl_ratio: f64,
}

impl Default for RedisAggAntiPatternsConfig {
    fn default() -> Self {
        Self {
            base: AggAntiPatternsConfig::default(),
            high_fanout_threshold: defaults::AGG_ANTI_PATTERN_HIGH_FANOUT_THRESHOLD,
            no_ttl_ratio: defaults::AGG_ANTI_PATTERN_NO_TTL_RATIO,
        }
    }
}

impl RedisAggAntiPatternsConfig {
    pub fn validate_for(&self, path: &str) -> Result<(), ConfigError> {
        self.base.validate_for(path)?;
        if self.high_fanout_threshold == 0 {
            return Err(ConfigError::InvalidValue(format!("{path}.high_fanout_threshold must be > 0")));
        }
        if !(0.0..=1.0).contains(&self.no_ttl_ratio) {
            return Err(ConfigError::InvalidValue(format!(
                "{path}.no_ttl_ratio must be in 0.0..=1.0, got {}",
                self.no_ttl_ratio
            )));
        }
        Ok(())
    }
}

/// Audit trail configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuditConfig {
    /// Redis commands to audit (validated against RedisApi at consumption site).
    pub commands: Vec<String>,
    /// Services to audit.
    pub services: Vec<String>,
    /// Flush interval in seconds.
    pub flush_interval_secs: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
            services: Vec::new(),
            flush_interval_secs: defaults::AUDIT_FLUSH_INTERVAL_SECS,
        }
    }
}

/// Real-time analytics stream configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamConfig {
    /// Enable real-time stream.
    pub enabled: bool,
    /// Stream snapshot interval in seconds.
    pub interval_secs: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::STREAM_ENABLED,
            interval_secs: defaults::STREAM_INTERVAL_SECS,
        }
    }
}

/// Thresholds that control when recommendation rules fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RecommendationConfig {
    // Confidence thresholds
    pub min_observation_windows: u64,
    pub min_total_requests: u64,

    // WriteHeavyNoRead
    pub write_heavy_ratio: f64,
    pub write_heavy_min_reads_per_day: u64,

    // NoTtlHighCardinality
    pub no_ttl_coverage_threshold: f64,
    pub no_ttl_min_writes: u64,

    // StalePattern
    pub stale_days: u64,

    // OversizedValues
    pub oversized_value_bytes: u64,
    pub oversized_min_requests: u64,

    // HotKeyConcentration
    pub hot_key_pct_threshold: f64,

    // HighErrorRate
    pub high_error_rate: f64,

    // ExpensiveReads
    pub expensive_read_max_write_ratio: f64,
    pub expensive_read_latency_us: u64,

    // CacheMissHotspot
    pub cache_miss_hotspot_max_hit_rate: f64,
    pub cache_miss_hotspot_min_requests: u64,

    // RedirectStorm
    pub redirect_storm_ratio: f64,
    pub redirect_storm_min_requests: u64,

    // CommandCostOutlier
    pub command_cost_outlier_ratio: f64,
    pub command_cost_outlier_min_requests: u64,

    // HighErrorCategoryConcentration
    pub error_category_concentration_ratio: f64,
    pub error_category_concentration_min_errors: u64,

    // MissingPipeline
    pub missing_pipeline_min_reads_per_window: u64,

    // LargeHashFetch
    pub large_hash_fetch_min_value_bytes: u64,

    // Context rules (pipeline feature)
    pub dangerous_command_min_occurrences: u64,
    pub pii_min_detections: u64,
    pub read_replica_min_requests: u64,
    pub read_replica_read_ratio_threshold: f64,
    pub high_fanout_min_occurrences: u64,

    // ElastiCache pricing ($ per GB per month)
    pub ram_price_per_gb_monthly: f64,

    // Error note: cross-reference threshold for pattern-level error rate
    pub error_note_min_rate: f64,

    // Per-rule output cap (0 = unlimited)
    pub max_recommendations_per_rule: usize,
}

impl Default for RecommendationConfig {
    fn default() -> Self {
        Self {
            min_observation_windows: defaults::REC_MIN_OBSERVATION_WINDOWS,
            min_total_requests: defaults::REC_MIN_TOTAL_REQUESTS,
            write_heavy_ratio: defaults::REC_WRITE_HEAVY_RATIO,
            write_heavy_min_reads_per_day: defaults::REC_WRITE_HEAVY_MIN_READS_PER_DAY,
            no_ttl_coverage_threshold: defaults::REC_NO_TTL_COVERAGE_THRESHOLD,
            no_ttl_min_writes: defaults::REC_NO_TTL_MIN_WRITES,
            stale_days: defaults::REC_STALE_DAYS,
            oversized_value_bytes: defaults::REC_OVERSIZED_VALUE_BYTES,
            oversized_min_requests: defaults::REC_OVERSIZED_MIN_REQUESTS,
            hot_key_pct_threshold: defaults::REC_HOT_KEY_PCT_THRESHOLD,
            high_error_rate: defaults::REC_HIGH_ERROR_RATE,
            expensive_read_max_write_ratio: defaults::REC_EXPENSIVE_READ_MAX_WRITE_RATIO,
            expensive_read_latency_us: defaults::REC_EXPENSIVE_READ_LATENCY_US,
            cache_miss_hotspot_max_hit_rate: defaults::REC_CACHE_MISS_HOTSPOT_MAX_HIT_RATE,
            cache_miss_hotspot_min_requests: defaults::REC_CACHE_MISS_HOTSPOT_MIN_REQUESTS,
            redirect_storm_ratio: defaults::REC_REDIRECT_STORM_RATIO,
            redirect_storm_min_requests: defaults::REC_REDIRECT_STORM_MIN_REQUESTS,
            command_cost_outlier_ratio: defaults::REC_COMMAND_COST_OUTLIER_RATIO,
            command_cost_outlier_min_requests: defaults::REC_COMMAND_COST_OUTLIER_MIN_REQUESTS,
            error_category_concentration_ratio: defaults::REC_ERROR_CATEGORY_CONCENTRATION_RATIO,
            error_category_concentration_min_errors: defaults::REC_ERROR_CATEGORY_CONCENTRATION_MIN_ERRORS,
            missing_pipeline_min_reads_per_window: defaults::REC_MISSING_PIPELINE_MIN_READS_PER_WINDOW,
            large_hash_fetch_min_value_bytes: defaults::REC_LARGE_HASH_FETCH_MIN_VALUE_BYTES,
            dangerous_command_min_occurrences: defaults::REC_DANGEROUS_COMMAND_MIN_OCCURRENCES,
            pii_min_detections: defaults::REC_PII_MIN_DETECTIONS,
            read_replica_min_requests: defaults::REC_READ_REPLICA_MIN_REQUESTS,
            read_replica_read_ratio_threshold: defaults::REC_READ_REPLICA_READ_RATIO_THRESHOLD,
            high_fanout_min_occurrences: defaults::REC_HIGH_FANOUT_MIN_OCCURRENCES,
            ram_price_per_gb_monthly: defaults::REC_RAM_PRICE_PER_GB_MONTHLY,
            error_note_min_rate: defaults::REC_ERROR_NOTE_MIN_RATE,
            max_recommendations_per_rule: defaults::REC_MAX_RECOMMENDATIONS_PER_RULE,
        }
    }
}

impl RecommendationConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_for("recommendations")
    }

    /// Validate with a context prefix for error messages (e.g., "analytics.redis.recommendations").
    pub fn validate_for(&self, context: &str) -> Result<(), ConfigError> {
        if self.min_observation_windows == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.min_observation_windows must be > 0")));
        }
        if self.min_total_requests == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.min_total_requests must be > 0")));
        }

        let ratios: &[(&str, f64)] = &[
            ("write_heavy_ratio", self.write_heavy_ratio),
            ("no_ttl_coverage_threshold", self.no_ttl_coverage_threshold),
            ("hot_key_pct_threshold", self.hot_key_pct_threshold),
            ("high_error_rate", self.high_error_rate),
            ("expensive_read_max_write_ratio", self.expensive_read_max_write_ratio),
            ("cache_miss_hotspot_max_hit_rate", self.cache_miss_hotspot_max_hit_rate),
            ("redirect_storm_ratio", self.redirect_storm_ratio),
            ("error_category_concentration_ratio", self.error_category_concentration_ratio),
            ("read_replica_read_ratio_threshold", self.read_replica_read_ratio_threshold),
            ("error_note_min_rate", self.error_note_min_rate),
        ];
        for &(name, value) in ratios {
            if !(0.0..=1.0).contains(&value) {
                return Err(ConfigError::InvalidValue(format!("{context}.{name} must be in 0.0..=1.0, got {value}")));
            }
        }

        if self.stale_days == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.stale_days must be > 0")));
        }
        if self.oversized_value_bytes == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.oversized_value_bytes must be > 0")));
        }
        if self.cache_miss_hotspot_min_requests == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.cache_miss_hotspot_min_requests must be > 0")));
        }
        if self.redirect_storm_min_requests == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.redirect_storm_min_requests must be > 0")));
        }
        if self.command_cost_outlier_ratio <= 0.0 {
            return Err(ConfigError::InvalidValue(format!(
                "{context}.command_cost_outlier_ratio must be > 0, got {}",
                self.command_cost_outlier_ratio
            )));
        }
        if self.command_cost_outlier_min_requests == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.command_cost_outlier_min_requests must be > 0")));
        }
        if self.error_category_concentration_min_errors == 0 {
            return Err(ConfigError::InvalidValue(format!("{context}.error_category_concentration_min_errors must be > 0")));
        }
        if self.ram_price_per_gb_monthly < 0.0 {
            return Err(ConfigError::InvalidValue(format!("{context}.ram_price_per_gb_monthly must be >= 0")));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestionConfig {
    /// Snapshotted at loop start; changes need a restart.
    pub rollup_flush_interval_secs: u64,
    /// Per-batch timeout during live flushes. Snapshotted at loop start.
    pub live_flush_timeout_secs: u64,
    /// Per-batch timeout during shutdown flush. Snapshotted at loop start.
    pub shutdown_flush_timeout_secs: u64,
    /// Caps total time spent flushing during shutdown. Snapshotted at loop start.
    pub shutdown_total_timeout_secs: u64,
    /// Read per-call; hot-reloads take effect immediately.
    pub blocked_command_max_per_endpoint: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            rollup_flush_interval_secs: defaults::ROLLUP_FLUSH_INTERVAL_SECS,
            live_flush_timeout_secs: defaults::LIVE_FLUSH_TIMEOUT_SECS,
            shutdown_flush_timeout_secs: defaults::SHUTDOWN_FLUSH_TIMEOUT_SECS,
            shutdown_total_timeout_secs: defaults::SHUTDOWN_TOTAL_TIMEOUT_SECS,
            blocked_command_max_per_endpoint: defaults::BLOCKED_COMMAND_MAX_PER_ENDPOINT,
        }
    }
}

impl IngestionConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.rollup_flush_interval_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.ingestion.rollup_flush_interval_secs must be > 0".into()));
        }
        if self.rollup_flush_interval_secs > u16::MAX as u64 {
            return Err(ConfigError::InvalidValue(format!(
                "analytics.ingestion.rollup_flush_interval_secs must be <= {} (u16 max, used as window_secs downstream)",
                u16::MAX
            )));
        }
        if self.live_flush_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.ingestion.live_flush_timeout_secs must be > 0".into()));
        }
        if self.shutdown_flush_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.ingestion.shutdown_flush_timeout_secs must be > 0".into()));
        }
        if self.shutdown_total_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.ingestion.shutdown_total_timeout_secs must be > 0".into()));
        }
        if self.shutdown_total_timeout_secs < self.shutdown_flush_timeout_secs {
            return Err(ConfigError::InvalidValue(format!(
                "analytics.ingestion.shutdown_total_timeout_secs ({}) must be >= shutdown_flush_timeout_secs ({})",
                self.shutdown_total_timeout_secs, self.shutdown_flush_timeout_secs
            )));
        }
        if self.blocked_command_max_per_endpoint == 0 {
            return Err(ConfigError::InvalidValue("analytics.ingestion.blocked_command_max_per_endpoint must be > 0".into()));
        }
        Ok(())
    }
}

/// Metadata collection scheduler: intervals, timeouts, backoff, Redis prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetadataCollectionConfig {
    /// High-priority collection interval (seconds).
    pub high_interval_secs: u64,
    /// Medium-priority collection interval (seconds).
    pub medium_interval_secs: u64,
    /// Low-priority collection interval (seconds).
    pub low_interval_secs: u64,
    /// Per-job timeout (seconds).
    pub job_timeout_secs: u64,
    /// Per-endpoint wall-clock timeout (seconds). Caps the total time a
    /// single `process_endpoint` call can take across all its jobs.
    pub endpoint_timeout_secs: u64,
    /// Maximum number of endpoints processed concurrently per tick.
    pub max_concurrent_endpoints: usize,
    /// Backoff base delay on failure (seconds).
    pub backoff_base_secs: u64,
    /// Backoff multiplier per consecutive failure.
    pub backoff_factor: u32,
    /// Maximum backoff delay (seconds).
    pub backoff_max_secs: u64,
    /// Redis key prefix for metadata storage.
    pub redis_prefix: String,
    /// Default query timeout for metadata collectors (seconds).
    pub collector_query_timeout_secs: u64,
}

impl Default for MetadataCollectionConfig {
    fn default() -> Self {
        Self {
            high_interval_secs: 60,
            medium_interval_secs: 1800,
            low_interval_secs: 86400,
            job_timeout_secs: 60,
            endpoint_timeout_secs: 120,
            max_concurrent_endpoints: 4,
            backoff_base_secs: 30,
            backoff_factor: 2,
            backoff_max_secs: 900,
            redis_prefix: "metadata:".to_string(),
            collector_query_timeout_secs: 5,
        }
    }
}

impl MetadataCollectionConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.high_interval_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.high_interval_secs must be > 0".into()));
        }
        if self.medium_interval_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.medium_interval_secs must be > 0".into()));
        }
        if self.low_interval_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.low_interval_secs must be > 0".into()));
        }
        if self.job_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.job_timeout_secs must be > 0".into()));
        }
        if self.endpoint_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.endpoint_timeout_secs must be > 0".into()));
        }
        if self.max_concurrent_endpoints == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.max_concurrent_endpoints must be > 0".into()));
        }
        if self.backoff_factor == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.backoff_factor must be > 0".into()));
        }
        if self.backoff_max_secs == 0 {
            return Err(ConfigError::InvalidValue("analytics.metadata.backoff_max_secs must be > 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `#[serde(flatten)]` correctly round-trips the shared base
    /// fields through `RedisAggAntiPatternsConfig`.
    #[test]
    fn redis_agg_anti_pattern_config_flatten_round_trip() {
        let original = RedisAggAntiPatternsConfig {
            base: AggAntiPatternsConfig {
                large_response_bytes: 65536,
                high_error_rate: 0.25,
                high_latency_variance_ratio: 1.75,
                high_latency_variance_min_requests: 250,
            },
            high_fanout_threshold: 100,
            no_ttl_ratio: 0.8,
        };

        let json = serde_json::to_string(&original).expect("serialize");
        let parsed: RedisAggAntiPatternsConfig = serde_json::from_str(&json).expect("deserialize");

        // Shared (flattened) fields survive the round-trip.
        assert_eq!(parsed.base.large_response_bytes, 65536);
        assert!((parsed.base.high_error_rate - 0.25).abs() < f64::EPSILON);
        assert!((parsed.base.high_latency_variance_ratio - 1.75).abs() < f64::EPSILON);
        assert_eq!(parsed.base.high_latency_variance_min_requests, 250);

        // Redis-specific fields survive the round-trip.
        assert_eq!(parsed.high_fanout_threshold, 100);
        assert!((parsed.no_ttl_ratio - 0.8).abs() < f64::EPSILON);
    }
}
