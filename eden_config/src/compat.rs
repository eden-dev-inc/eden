//! Legacy environment variable compatibility layer.
//!
//! Maps legacy flat env vars (e.g. `EDEN_PORT`) to their nested config paths,
//! preserving backwards compatibility with existing deployment scripts.

use figment::{providers::Serialized, Figment};

/// Legacy flat environment variable mappings to nested config paths.
///
/// This ensures backwards compatibility with existing deployment scripts
/// that use flat env vars like EDEN_PORT instead of nested EDEN__SERVICES__EDEN__PORT.
const LEGACY_MAPPINGS: &[(&str, &str)] = &[
    // Features
    ("EDEN_ANALYTICS_ENABLED", "features.analytics_enabled"),
    ("EDEN_POLICY_ENFORCEMENT_MODE", "features.policy_enforcement_mode"),
    ("REDIS_PSYNC", "features.redis_psync"),
    // Services - Eden
    ("EDEN_HOST", "services.eden.host"),
    ("EDEN_PORT", "services.eden.port"),
    ("EDEN_JWT_SECRET", "services.eden.jwt_secret"),
    ("EDEN_JWT_EXPIRY_S", "limits.jwt_expiry_secs"),
    ("EDEN_NODE_UUID", "services.eden.node_uuid"),
    ("EDEN_NEW_ORG_TOKEN", "services.eden.new_org_token"),
    ("EDEN_GATEWAY_CPU_AFFINITY", "services.eden.gateway_cpu_affinity"),
    ("EDEN_RATE_LIMIT", "limits.rate_limit_ms"),
    // Services - Engine
    ("ENGINE_HOST", "services.engine.host"),
    ("ENGINE_PORT", "services.engine.port"),
    // Telemetry
    ("EDEN_OTLP_COLLECTOR", "telemetry.otlp_collector"),
    ("EDEN_OTLP_EXPORT_ENABLED", "telemetry.otlp_export_enabled"),
    ("EDEN_OTLP_DB_COLLECTOR", "telemetry.otlp_db_collector"),
    ("ENGINE_OTLP_COLLECTOR", "telemetry.engine_otlp_collector"),
    ("EDEN_LOG_LEVEL", "telemetry.log_level"),
    ("EDEN_DOGSTATSD_ENABLED", "telemetry.dogstatsd_enabled"),
    ("EDEN_DOGSTATSD_ENDPOINT", "telemetry.dogstatsd_endpoint"),
    ("EDEN_CLICKHOUSE_TELEMETRY_ENABLED", "telemetry.clickhouse_enabled"),
    ("EDEN_DUCKDB_PATH", "telemetry.duckdb.path"),
    ("EDEN_DUCKDB_MEMORY_LIMIT", "telemetry.duckdb.memory_limit"),
    ("EDEN_DUCKDB_TEMP_DIRECTORY", "telemetry.duckdb.temp_directory"),
    ("EDEN_DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "telemetry.duckdb.max_temp_directory_size"),
    ("EDEN_DUCKDB_CHECKPOINT_THRESHOLD", "telemetry.duckdb.checkpoint_threshold"),
    ("EDEN_DUCKDB_CHECKPOINT_INTERVAL_SECS", "telemetry.duckdb.checkpoint_interval_secs"),
    ("EDEN_DUCKDB_ANALYTICS_RETENTION_DAYS", "telemetry.duckdb.analytics_retention_days"),
    ("EDEN_DUCKDB_LOGS_RETENTION_DAYS", "telemetry.duckdb.logs_retention_days"),
    ("EDEN_DUCKDB_TRACES_RETENTION_DAYS", "telemetry.duckdb.traces_retention_days"),
    // Databases - Redis
    ("REDIS_HOST", "databases.redis.host"),
    ("REDIS_PORT", "databases.redis.port"),
    ("REDIS_USER", "databases.redis.username"),
    ("REDIS_PASSWORD", "databases.redis.password"),
    ("REDIS_DB_NUMBER", "databases.redis.db_number"),
    ("REDIS_CACHE_TTL", "limits.redis_cache_ttl_secs"),
    // Databases - Postgres
    ("POSTGRES_HOST", "databases.postgres.host"),
    ("POSTGRES_PORT", "databases.postgres.port"),
    ("POSTGRES_USER", "databases.postgres.username"),
    ("POSTGRES_PASSWORD", "databases.postgres.password"),
    ("POSTGRES_DB_NAME", "databases.postgres.database"),
    // Databases - ClickHouse
    ("CLICKHOUSE_URL", "databases.clickhouse.url"),
    ("CLICKHOUSE_USER", "databases.clickhouse.username"),
    ("CLICKHOUSE_PASSWORD", "databases.clickhouse.password"),
    ("CLICKHOUSE_DATABASE", "databases.clickhouse.database"),
    ("CLICKHOUSE_DB", "databases.clickhouse.database"),
    ("CLICKHOUSE_POOL_SIZE", "limits.clickhouse_pool_size"),
    // LLM
    ("EDEN_INTERNAL_LLM_PROVIDER", "services.llm.provider"),
    ("EDEN_INTERNAL_LLM_MODEL", "services.llm.model"),
    ("EDEN_INTERNAL_LLM_API_KEY", "services.llm.api_key"),
    ("EDEN_INTERNAL_LLM_BASE_URL", "services.llm.base_url"),
    ("EDEN_INTERNAL_LLM_SYSTEM_PROMPT", "services.llm.system_prompt"),
    ("EDEN_INTERNAL_LLM_TEMPERATURE", "services.llm.temperature"),
    ("EDEN_INTERNAL_LLM_MAX_TOKENS", "services.llm.max_tokens"),
    ("EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PUBLISH_PATH", "services.llm.gateway_snapshot_publish_path"),
    (
        "EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PUBLISH_SECS",
        "services.llm.gateway_snapshot_publish_interval_secs",
    ),
    // Agents
    ("EDEN_SKILL_PROMPT_BUDGET_TOKENS", "agents.skill_prompt_budget_tokens"),
    ("EDEN_SKILL_CONTEXT_WINDOW_TOKENS", "agents.default_context_window_tokens"),
    ("ADAM_SKILL_POLICY_MODE", "agents.skill_policy.mode"),
    ("ADAM_QUARANTINED_SKILLS", "agents.skill_policy.quarantined_skills"),
    ("ADAM_ALLOW_CUSTOMER_SKILL_CRUD", "agents.allow_customer_skill_crud"),
    // Snapshot
    ("EDEN_SNAPSHOT_PATH", "snapshot.path"),
    ("EDEN_SNAPSHOT_PASSWORD", "snapshot.password"),
    ("EDEN_SNAPSHOT_DIR", "snapshot.dir"),
    // Organization Transfer
    ("EDEN_ORG_TRANSFER_DIR", "org_transfer.dir"),
    // Tools
    ("TOOLS_SERVICE_TIMEOUT_SECS", "limits.tools_service_timeout_secs"),
    // Redis pool cap
    ("EDEN_REDIS_POOL_MAX_CONNECTIONS_CAP", "limits.redis_pool_max_connections_cap"),
    // Redis migration batching
    ("REDIS_BATCH_COUNT", "limits.redis_batch_count"),
    ("REDIS_BATCH_SIZE", "limits.redis_batch_size_bytes"),
    // Licensing client
    ("EDEN_LICENSE_KEY", "licensing.license_key"),
    ("EDEN_CLUSTER_UID", "licensing.cluster_uid"),
    ("EDEN_PHONE_HOME_URL", "licensing.phone_home_url"),
    ("EDEN_HEARTBEAT_INTERVAL_SECS", "licensing.heartbeat_interval_secs"),
    ("EDEN_PHONE_HOME_DISABLED", "licensing.disabled"),
    ("EDEN_METERING_ENABLED", "licensing.metering_enabled"),
    ("EDEN_METERING_ENDPOINT_URL", "licensing.metering_endpoint_url"),
    ("EDEN_METERING_INGEST_API_KEY", "licensing.metering_ingest_api_key"),
    ("EDEN_METERING_FLUSH_INTERVAL_SECS", "licensing.metering_flush_interval_secs"),
    ("EDEN_METERING_MAX_BATCH_SIZE", "licensing.metering_max_batch_size"),
    ("EDEN_METERING_RETRY_MAX_ATTEMPTS", "licensing.metering_retry_max_attempts"),
    ("EDEN_METERING_RETRY_BASE_DELAY_MS", "licensing.metering_retry_base_delay_ms"),
    ("EDEN_METERING_RETRY_MAX_DELAY_MS", "licensing.metering_retry_max_delay_ms"),
];

/// Apply legacy environment variable mappings to a Figment.
///
/// This function reads flat environment variables and maps them to their
/// nested config path equivalents, enabling backwards compatibility.
pub fn apply_legacy_env_vars(figment: Figment) -> Figment {
    let mut result = figment;

    for (env_var, config_path) in LEGACY_MAPPINGS {
        if let Ok(value) = std::env::var(env_var) {
            // Figment's Serialized treats all values by their Rust type.
            // Env vars are always strings, but many config fields are numeric
            // or boolean.  Try parsing as bool → i64 → f64 before falling
            // back to string so the downstream deserializer sees the right type.
            if let Ok(b) = value.parse::<bool>() {
                result = result.merge(Serialized::global(config_path, b));
            } else if let Ok(n) = value.parse::<i64>() {
                result = result.merge(Serialized::global(config_path, n));
            } else if let Ok(f) = value.parse::<f64>() {
                result = result.merge(Serialized::global(config_path, f));
            } else {
                result = result.merge(Serialized::global(config_path, value));
            }
        }
    }

    result
}

#[allow(dead_code)]
/// Get a list of all supported legacy environment variables.
pub fn legacy_env_vars() -> &'static [(&'static str, &'static str)] {
    LEGACY_MAPPINGS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_mappings_have_unique_env_vars() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for (env_var, _) in LEGACY_MAPPINGS {
            assert!(seen.insert(env_var), "Duplicate env var: {}", env_var);
        }
    }
}
