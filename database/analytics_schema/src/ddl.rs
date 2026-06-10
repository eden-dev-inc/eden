//! DDL helpers for creating ClickHouse analytics tables.

use anyhow::Context;

macro_rules! include_sql {
    ($path:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/", $path, ".sql"))
    };
}

fn telemetry_metric_table_ddl(table: &str) -> String {
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {table}
(
    timestamp DateTime64(3),
    organization_uuid String,
    service_name LowCardinality(String),
    node_uuid String,
    metric_name LowCardinality(String),
    metric_kind LowCardinality(String),
    value Nullable(Float64),
    count Nullable(UInt64),
    sum Nullable(Float64),
    bucket_bounds Array(Float64),
    bucket_counts Array(UInt64),
    labels Map(String, String),
    scope LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (organization_uuid, metric_name, timestamp, node_uuid)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
"#
    )
}

const TELEMETRY_TRACES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS analytics.traces
(
    timestamp DateTime64(3),
    organization_uuid String,
    service_name LowCardinality(String),
    node_uuid String,
    trace_id String,
    span_id String,
    parent_span_id String,
    span_name String,
    span_kind LowCardinality(String),
    start_time DateTime64(3),
    end_time DateTime64(3),
    duration_ns UInt64,
    status LowCardinality(String),
    status_message String,
    attributes Map(String, String),
    events_json String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (organization_uuid, trace_id, start_time, span_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
"#;

fn telemetry_metric_org_column_ddl(table: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS organization_uuid String AFTER timestamp")
}

const TELEMETRY_TRACES_ORG_COLUMN_DDL: &str = r#"
ALTER TABLE analytics.traces ADD COLUMN IF NOT EXISTS organization_uuid String AFTER timestamp
"#;

const TELEMETRY_LOGS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS analytics.logs
(
    timestamp DateTime64(3),
    service_name LowCardinality(String),
    node_uuid String,
    level LowCardinality(String),
    audience LowCardinality(String),
    message String,
    trace_id String,
    span_id String,
    feature String,
    function String,
    file String,
    line Nullable(UInt32),
    eden_node_uuid String,
    organization_uuid String,
    organization_id String,
    user_uuid String,
    user_id String,
    endpoint_uuid String,
    endpoint_id String,
    endpoint_kind String,
    error_code String,
    error_category String,
    labels Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (organization_uuid, timestamp, level, node_uuid)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
"#;

/// Create Eden telemetry metric, trace, and log tables.
pub async fn ensure_telemetry_tables(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    client
        .query(include_sql!("analytics/001_analytics_db"))
        .execute()
        .await
        .with_context(|| "failed to create analytics database")
        .map_err(|err| clickhouse::error::Error::Other(err.into()))?;

    for table in [
        crate::telemetry::tables::PROXY,
        crate::telemetry::tables::ENDPOINT,
        crate::telemetry::tables::EDEN,
        crate::telemetry::tables::IAM,
        crate::telemetry::tables::METADATA,
        crate::telemetry::tables::SNAPSHOT,
        crate::telemetry::tables::WORKLOAD,
        crate::telemetry::tables::VALIDATOR,
        crate::telemetry::tables::ANALYTICS,
    ] {
        client
            .query(&telemetry_metric_table_ddl(table))
            .execute()
            .await
            .with_context(|| format!("failed to create telemetry metric table {table}"))
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
        client
            .query(&telemetry_metric_org_column_ddl(table))
            .execute()
            .await
            .with_context(|| format!("failed to add organization_uuid to telemetry metric table {table}"))
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
    }

    for (label, ddl) in [("analytics/traces", TELEMETRY_TRACES_DDL), ("analytics/logs", TELEMETRY_LOGS_DDL)] {
        client
            .query(ddl)
            .execute()
            .await
            .with_context(|| format!("failed to execute telemetry DDL {label}"))
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
    }

    client
        .query(TELEMETRY_TRACES_ORG_COLUMN_DDL)
        .execute()
        .await
        .with_context(|| "failed to add organization_uuid to telemetry trace table")
        .map_err(|err| clickhouse::error::Error::Other(err.into()))?;

    Ok(())
}

/// Create analytics database and wire-protocol tables.
pub async fn ensure_wire_tables(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    client
        .query(include_sql!("analytics/001_analytics_db"))
        .execute()
        .await
        .with_context(|| "failed to create analytics database")
        .map_err(|err| clickhouse::error::Error::Other(err.into()))?;

    for (label, ddl) in [
        // Wire protocol tables (protocol-agnostic aggregates)
        ("analytics/041a_command_rollups", include_sql!("analytics/041a_command_rollups")),
        ("analytics/042_endpoint_metrics", include_sql!("analytics/042_endpoint_metrics")),
        ("analytics/043_target_pattern_rollups", include_sql!("analytics/043_target_pattern_rollups")),
        (
            "analytics/044_redis_key_pattern_profiles_view",
            include_sql!("analytics/044_redis_key_pattern_profiles_view"),
        ),
        ("analytics/045a_llm_operation_rollups", include_sql!("analytics/045a_llm_operation_rollups")),
        ("analytics/045b_llm_operation_events", include_sql!("analytics/045b_llm_operation_events")),
        (
            "analytics/045c_llm_operation_economics_columns",
            include_sql!("analytics/045c_llm_operation_economics_columns"),
        ),
        ("analytics/045d_llm_price_snapshots", include_sql!("analytics/045d_llm_price_snapshots")),
        (
            "analytics/045e_llm_operation_event_economics_columns",
            include_sql!("analytics/045e_llm_operation_event_economics_columns"),
        ),
        (
            "analytics/045f_llm_rollup_route_cache_columns",
            include_sql!("analytics/045f_llm_rollup_route_cache_columns"),
        ),
        (
            "analytics/045g_llm_operation_event_route_cache_columns",
            include_sql!("analytics/045g_llm_operation_event_route_cache_columns"),
        ),
        (
            "analytics/045h_llm_operation_rollup_organization_uuid",
            include_sql!("analytics/045h_llm_operation_rollup_organization_uuid"),
        ),
        (
            "analytics/045i_llm_operation_event_organization_uuid",
            include_sql!("analytics/045i_llm_operation_event_organization_uuid"),
        ),
        // Sparse event tables
        ("analytics/060_anti_patterns", include_sql!("analytics/060_anti_patterns")),
        ("analytics/062a_antipattern_daily_table", include_sql!("analytics/062a_antipattern_daily_table")),
        ("analytics/062b_antipattern_daily_mv", include_sql!("analytics/062b_antipattern_daily_mv")),
        ("analytics/080_blocked_commands", include_sql!("analytics/080_blocked_commands")),
        ("analytics/081_audit_trail", include_sql!("analytics/081_audit_trail")),
        // PII aggregate table
        ("analytics/085_pii_aggregate", include_sql!("analytics/085_pii_aggregate")),
        ("analytics/090_exemplars", include_sql!("analytics/090_exemplars")),
        // MongoDB shape rollups
        ("analytics/048_mongo_shape_rollups", include_sql!("analytics/048_mongo_shape_rollups")),
        // Hourly rollup table + materialized view
        ("analytics/041b_command_rollups_hourly", include_sql!("analytics/041b_command_rollups_hourly")),
        ("analytics/041c_command_rollups_hourly_mv", include_sql!("analytics/041c_command_rollups_hourly_mv")),
        ("analytics/051_anomaly_transitions", include_sql!("analytics/051_anomaly_transitions")),
        // Per-user rollups
        ("analytics/052_user_rollups", include_sql!("analytics/052_user_rollups")),
        // Session and API usage history tables
        ("analytics/100_session_history", include_sql!("analytics/100_session_history")),
        ("analytics/101_api_usage_history", include_sql!("analytics/101_api_usage_history")),
        // Infrastructure snapshot metrics
        ("analytics/102_infrastructure_snapshots", include_sql!("analytics/102_infrastructure_snapshots")),
        // Connection metrics snapshots
        ("analytics/110_connection_metrics", include_sql!("analytics/110_connection_metrics")),
        // Endpoint metrics schema additions
        (
            "analytics/111_endpoint_metrics_p95_latency",
            include_sql!("analytics/111_endpoint_metrics_p95_latency"),
        ),
    ] {
        client
            .query(ddl)
            .execute()
            .await
            .with_context(|| format!("failed to execute analytics DDL {label}"))
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
    }

    #[cfg(feature = "pipeline")]
    {
        client
            .query(include_sql!("analytics/095_discovery_templates"))
            .execute()
            .await
            .with_context(|| "failed to execute discovery templates DDL")
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
    }

    Ok(())
}

/// Create analytics database and poll-metrics tables only.
pub async fn ensure_poll_tables(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    client
        .query(include_sql!("analytics/001_analytics_db"))
        .execute()
        .await
        .with_context(|| "failed to create analytics database")
        .map_err(|err| clickhouse::error::Error::Other(err.into()))?;

    for (label, ddl) in [
        ("analytics/045_redis_poll_metrics", include_sql!("analytics/045_redis_poll_metrics")),
        ("analytics/046_postgres_poll_metrics", include_sql!("analytics/046_postgres_poll_metrics")),
        ("analytics/047_mongo_poll_metrics", include_sql!("analytics/047_mongo_poll_metrics")),
        ("analytics/048_oracle_poll_metrics", include_sql!("analytics/048_oracle_poll_metrics")),
        ("analytics/049_cassandra_poll_metrics", include_sql!("analytics/049_cassandra_poll_metrics")),
        ("analytics/050_clickhouse_poll_metrics", include_sql!("analytics/050_clickhouse_poll_metrics")),
    ] {
        client
            .query(ddl)
            .execute()
            .await
            .with_context(|| format!("failed to execute analytics DDL {label}"))
            .map_err(|err| clickhouse::error::Error::Other(err.into()))?;
    }

    Ok(())
}

/// Create all analytics tables: wire-protocol aggregates and per-protocol poll metrics.
pub async fn ensure_tables_exist(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    ensure_wire_tables(client).await?;
    ensure_poll_tables(client).await?;
    Ok(())
}
